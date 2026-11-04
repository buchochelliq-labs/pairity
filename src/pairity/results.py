"""Versioned results shared by comparison backends, sinks and frontends."""

from dataclasses import dataclass, field
from typing import Any, Literal

Status = Literal["passed", "failed", "blocked", "error", "skipped"]


class BlockedComparison(ValueError):
    """A prerequisite is missing; comparison cannot certify the pair."""


@dataclass
class TableInput:
    frame: Any
    table: str
    version: int | None = None
    snapshot_pinned: bool = False

    def metadata(self) -> dict:
        return {
            "table": self.table,
            "version": self.version,
            "snapshot_pinned": self.snapshot_pinned,
        }


@dataclass
class PairResult:
    pair_id: str
    status: Status
    counts: dict[str, int] | None = None
    column_changes: dict[str, int] = field(default_factory=dict)
    schema_differences: list[str] = field(default_factory=list)
    reason: str | None = None
    keys: list[dict[str, str]] = field(default_factory=list)
    inputs: dict[str, dict] = field(default_factory=dict)
    effective_settings: dict = field(default_factory=dict)
    differences: Any = field(default=None, repr=False, compare=False)

    def to_dict(self) -> dict:
        # A distributed difference DataFrame is deliberately not materialized or deep-copied.
        return {
            name: getattr(self, name)
            for name in (
                "pair_id",
                "status",
                "counts",
                "column_changes",
                "schema_differences",
                "reason",
                "keys",
                "inputs",
                "effective_settings",
            )
        }


@dataclass
class RunResult:
    run_id: str
    suite: str
    config_hash: str
    configuration: dict
    pairs: list[PairResult]
    started_at: str
    finished_at: str

    @property
    def status(self) -> Status:
        statuses = {p.status for p in self.pairs}
        for status in ("error", "blocked", "failed"):
            if status in statuses:
                return status
        return "passed" if "passed" in statuses else "skipped"

    def to_dict(self) -> dict:
        return {
            "result_schema_version": 1,
            "run_id": self.run_id,
            "suite": self.suite,
            "status": self.status,
            "config_hash": self.config_hash,
            "configuration": self.configuration,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "pairs": [p.to_dict() for p in self.pairs],
        }


def validate_pair_result(result, pair, settings) -> None:
    """Check inexpensive invariants before accepting a plugin's result as evidence."""
    if (
        not isinstance(result, PairResult)
        or result.pair_id != pair.id
        or result.status not in {"passed", "failed", "blocked", "error", "skipped"}
    ):
        raise ValueError("Comparison plugin returned an invalid result contract")
    if result.status == "skipped" and pair.enabled:
        raise ValueError("An enabled pair cannot be silently skipped by a comparison plugin")
    counts = result.counts
    if counts is None:
        if result.status == "passed" or (
            result.status == "failed" and not result.schema_differences
        ):
            raise ValueError("Executed comparisons must include complete row counts")
        return
    required = {
        "baseline_rows",
        "candidate_rows",
        "matched_rows",
        "changed_rows",
        "missing_rows",
        "extra_rows",
    }
    if (
        not isinstance(counts, dict)
        or not required <= counts.keys()
        or any(type(value) is not int or value < 0 for value in counts.values())
    ):
        raise ValueError("Row counts must be complete, nonnegative integers")
    if (
        counts["matched_rows"] + counts["missing_rows"] != counts["baseline_rows"]
        or counts["matched_rows"] + counts["extra_rows"] != counts["candidate_rows"]
        or counts["changed_rows"] > counts["matched_rows"]
    ):
        raise ValueError("Row counts are internally inconsistent")
    if not isinstance(result.column_changes, dict) or any(
        type(value) is not int or not 0 <= value <= counts["changed_rows"]
        for value in result.column_changes.values()
    ):
        raise ValueError("Column change counts are inconsistent with changed row counts")
    regression = bool(result.schema_differences) or any(
        counts[f"{kind}_rows"] > getattr(settings, f"max_{kind}_rows")
        for kind in ("changed", "missing", "extra")
    )
    if result.status == "passed" and regression:
        raise ValueError("A passing result cannot contain schema differences or exceed thresholds")
    if result.status == "failed" and not regression:
        raise ValueError(
            "A failed row comparison must identify an exceeded threshold or schema change"
        )
