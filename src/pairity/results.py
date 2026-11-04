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
