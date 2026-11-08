"""Atomic run history and opt-in distributed differences in a managed Delta table."""

import hashlib
import json
import platform
from datetime import datetime, timedelta, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F

from pairity import __version__
from pairity.config import SuiteConfig, resolve_settings
from pairity.metadata import quote_table
from pairity.results import validate_pair_result

COLUMNS = (
    "record_type string, run_id string, suite string, pair_id string, "
    "finished_at string, report_hash string, payload string"
)


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


class DeltaResultStore:
    """One Delta transaction publishes the summary and all retained differences.

    A report row is the completion marker. Other consumers never need Postgres.
    Retries reuse the run ID; concurrent incompatible retries fail the post-write
    hash check instead of acknowledging another writer's report as their own.
    """

    api_version = 1

    def __init__(self, spark, table, *, retain_differences=False):
        if type(retain_differences) is not bool:
            raise ValueError("retain_differences must be a boolean")
        self.spark = spark
        self.name = table
        self.table = quote_table(table)
        self.retain_differences = retain_differences

    def initialize(self):
        """Explicit provisioning; catalog/schema must already exist."""
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.table} ({COLUMNS}) USING DELTA")
        expected = {
            field.name: field.dataType for field in self.spark.createDataFrame([], COLUMNS).schema
        }
        actual = {field.name: field.dataType for field in self.spark.table(self.table).schema}
        if actual != expected:
            raise ValueError("Existing result table has an incompatible schema")

    def _rows(self, run_id):
        return self.spark.table(self.table).where(F.col("run_id") == F.lit(run_id))

    def load(self, run_id):
        rows = (
            self._rows(run_id)
            .where(F.col("record_type") == "report")
            .select("payload", "report_hash")
            .limit(2)
            .collect()
        )
        if not rows:
            raise FileNotFoundError("No completed result for this run")
        if len(rows) != 1:
            raise ValueError("Duplicate run completion records")
        payload = rows[0]["payload"]
        if hashlib.sha256(payload.encode()).hexdigest() != rows[0]["report_hash"]:
            raise ValueError("Stored result checksum mismatch")
        result = json.loads(payload)
        if result.get("storage_schema_version") != 1:
            raise ValueError("Unsupported result storage schema version")
        return result

    def write(self, result):
        if not isinstance(result.run_id, str) or not result.run_id:
            raise ValueError("A stable run ID is required")
        config = SuiteConfig.model_validate(result.configuration)
        if config.fingerprint != result.config_hash or config.suite != result.suite:
            raise ValueError("Run configuration hash or suite mismatch")
        configured = {p.id: p for p in config.pairs}
        ids = [p.pair_id for p in result.pairs]
        if len(ids) != len(set(ids)) or set(ids) - configured.keys():
            raise ValueError("Invalid run pair coverage")
        for pair in result.pairs:
            validate_pair_result(
                pair, configured[pair.pair_id], resolve_settings(config, configured[pair.pair_id])
            )
            if (
                self.retain_differences
                and pair.counts
                and any(pair.counts[f"{kind}_rows"] for kind in ("changed", "missing", "extra"))
                and pair.differences is None
            ):
                raise ValueError(
                    "Requested difference retention but a pair has no difference DataFrame"
                )
        document = {
            "storage_schema_version": 1,
            "report": result.to_dict(),
            "differences_retained": self.retain_differences,
            "runtime": {
                "pairity": __version__,
                "python": platform.python_version(),
                "spark": self.spark.version,
            },
        }
        payload = _json(document)
        digest = hashlib.sha256(payload.encode()).hexdigest()
        frame = self.spark.createDataFrame(
            [("report", result.run_id, result.suite, None, result.finished_at, digest, payload)],
            COLUMNS,
        )
        if self.retain_differences:
            for pair in result.pairs:
                if pair.differences is None:
                    continue
                difference = pair.differences
                required = {"kind", "baseline", "candidate", "changed_columns"}
                if not required <= set(difference.columns):
                    raise ValueError("Difference DataFrame lacks the standard evidence columns")
                rows = difference.select(
                    F.lit("difference").alias("record_type"),
                    F.lit(result.run_id).alias("run_id"),
                    F.lit(result.suite).alias("suite"),
                    F.lit(pair.pair_id).alias("pair_id"),
                    F.lit(result.finished_at).alias("finished_at"),
                    F.lit(digest).alias("report_hash"),
                    F.to_json(
                        F.struct(*[F.col(c) for c in sorted(required)]),
                        {"ignoreNullFields": "false"},
                    ).alias("payload"),
                )
                frame = frame.unionByName(rows)
        if self.retain_differences:
            frame = frame.persist()
        try:
            # Materialize a stable candidate and validate bounded kind aggregates.
            for pair in result.pairs:
                if not self.retain_differences:
                    break
                records = frame.where(
                    (F.col("record_type") == "difference") & (F.col("pair_id") == pair.pair_id)
                )
                kind = F.get_json_object("payload", "$.kind")
                if (
                    records.where(kind.isNull() | ~kind.isin("changed", "missing", "extra"))
                    .limit(1)
                    .count()
                ):
                    raise ValueError("Invalid difference kind in retained evidence")
                counts = {
                    row["kind"]: row["count"]
                    for row in records.select(kind.alias("kind")).groupBy("kind").count().collect()
                }
                expected = pair.counts or {}
                if any(
                    counts.get(k, 0) != expected.get(f"{k}_rows", 0)
                    for k in ("changed", "missing", "extra")
                ):
                    raise ValueError("Retained evidence counts differ from the comparison counts")
            frame.count()
            try:
                existing = self.load(result.run_id)
            except FileNotFoundError:
                existing = None
            if existing is not None:
                if _json(existing) != payload:
                    raise ValueError(
                        "Run ID already contains a different result or retention policy"
                    )
                self._verify_evidence(frame, result.run_id)
                return
            # The report and evidence are published atomically in a single Delta commit.
            (
                frame.write.format("delta")
                .mode("append")
                .option("txnAppId", "pairity:" + result.run_id)
                .option("txnVersion", 0)
                .saveAsTable(self.table)
            )
            if _json(self.load(result.run_id)) != payload:
                raise ValueError("Concurrent writer committed a different result for this run ID")
            self._verify_evidence(frame, result.run_id)
        finally:
            if self.retain_differences:
                frame.unpersist()

    def _verify_evidence(self, candidate, run_id):
        # Exact distributed multiset comparison; no lossy hash can certify a retry.
        wanted = candidate.select("record_type", "pair_id", "payload")
        stored = self._rows(run_id).select("record_type", "pair_id", "payload")
        if wanted.exceptAll(stored).limit(1).count() or stored.exceptAll(wanted).limit(1).count():
            raise ValueError("Run ID already contains different row evidence")

    def differences(self, run_id, pair_id):
        document = self.load(run_id)
        if not document["differences_retained"]:
            raise ValueError("Row differences were not retained for this run")
        pairs = {pair["pair_id"]: pair for pair in document["report"]["pairs"]}
        if pair_id not in pairs:
            raise ValueError("Unknown pair in this run")
        if pairs[pair_id]["counts"] is None:
            raise ValueError("Row comparison was not executed for this pair")
        return (
            self._rows(run_id)
            .where((F.col("record_type") == "difference") & (F.col("pair_id") == F.lit(pair_id)))
            .select("payload")
        )

    def history(self, *, suite=None, limit=100):
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("History limit must be between 1 and 1000")
        rows = self.spark.table(self.table).where(F.col("record_type") == "report")
        if suite is not None:
            rows = rows.where(F.col("suite") == F.lit(suite))
        return rows.orderBy(F.col("finished_at").desc(), F.col("run_id")).limit(limit)

    # --- Retention -------------------------------------------------------------------

    def _eligible(self, criteria):
        """Reports eligible for removal under the criteria, oldest first."""
        cutoff = datetime.fromisoformat(criteria["cutoff"])
        reports = (
            self.spark.table(self.table)
            .where(F.col("record_type") == "report")
            .select("run_id", "suite", "finished_at")
            .withColumn("finished", F.col("finished_at").cast("timestamp"))
        )
        if criteria["suite"] is not None:
            reports = reports.where(F.col("suite") == F.lit(criteria["suite"]))
        newest = Window.partitionBy("suite").orderBy(F.col("finished").desc(), F.col("run_id"))
        return (
            reports.withColumn("rank", F.row_number().over(newest))
            # An unparseable finish time is never treated as old.
            .where(F.col("finished").isNotNull())
            .where(F.col("finished") < F.lit(cutoff))
            .where(F.col("rank") > F.lit(criteria["keep_latest_per_suite"]))
            .orderBy("finished", "run_id")
        )

    def retention_plan(
        self, *, older_than_days, keep_latest_per_suite=1, suite=None, max_runs=1000, now=None
    ):
        """Preview whole runs to remove; nothing is deleted.

        A run is eligible when it finished before the cutoff and is not among the
        ``keep_latest_per_suite`` newest runs of its suite. Plans are capped at
        ``max_runs``; ``truncated`` reports that more runs remain eligible.
        """
        if type(older_than_days) is not int or older_than_days < 1:
            raise ValueError("older_than_days must be a positive integer")
        if type(keep_latest_per_suite) is not int or keep_latest_per_suite < 0:
            raise ValueError("keep_latest_per_suite must be a nonnegative integer")
        if type(max_runs) is not int or not 1 <= max_runs <= 1000:
            raise ValueError("max_runs must be between 1 and 1000")
        now = now or datetime.now(timezone.utc)
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        criteria = {
            "cutoff": (now - timedelta(days=older_than_days)).isoformat(),
            "keep_latest_per_suite": keep_latest_per_suite,
            "suite": suite,
        }
        rows = self._eligible(criteria).limit(max_runs + 1).collect()
        return {
            "table": self.name,
            "criteria": criteria,
            "runs": [
                {"run_id": r["run_id"], "suite": r["suite"], "finished_at": r["finished_at"]}
                for r in rows[:max_runs]
            ],
            "truncated": len(rows) > max_runs,
        }

    def apply_retention(self, plan):
        """Remove the planned runs that are still eligible, in one Delta commit.

        Reports and their retained differences are deleted together, so no report can
        claim evidence that is gone. Eligibility is re-checked against the table's
        current state, so runs that became newest-in-suite since the plan are kept.
        """
        if plan.get("table") != self.name:
            raise ValueError("Retention plan was made for a different result table")
        planned = {run["run_id"] for run in plan["runs"]}
        still = {r["run_id"] for r in self._eligible(plan["criteria"]).collect()}
        targets = sorted(planned & still)
        if not targets:
            return {"deleted": [], "kept": sorted(planned)}
        markers = ", ".join(f":run{i}" for i in range(len(targets)))
        self.spark.sql(
            f"DELETE FROM {self.table} WHERE run_id IN ({markers})",
            args={f"run{i}": run_id for i, run_id in enumerate(targets)},
        )
        remaining = self.spark.table(self.table).where(F.col("run_id").isin(targets)).limit(1)
        if remaining.count():
            raise ValueError("Retention delete did not remove every planned run")
        return {"deleted": targets, "kept": sorted(planned - set(targets))}

    # --- Analytical views ----------------------------------------------------------

    def create_views(self, *, runs_view=None, pairs_view=None):
        """Create or replace read-only views decoding stored reports for SQL analysis.

        ``<table>_runs`` has one row per completed run; ``<table>_pairs`` has one row
        per pair outcome with counts and per-column change counts, suitable for
        comparing a pair across runs.
        """
        runs_view = quote_table(runs_view or self.name + "_runs")
        pairs_view = quote_table(pairs_view or self.name + "_pairs")
        report = "get_json_object(payload, '$.report"
        self.spark.sql(
            f"""CREATE OR REPLACE VIEW {runs_view} AS
SELECT
  run_id,
  suite,
  {report}.status') AS status,
  {report}.config_hash') AS config_hash,
  CAST({report}.started_at') AS TIMESTAMP) AS started_at,
  CAST(finished_at AS TIMESTAMP) AS finished_at,
  size(from_json({report}.pairs'), 'array<string>')) AS pair_count,
  CAST(get_json_object(payload, '$.differences_retained') AS BOOLEAN) AS differences_retained,
  get_json_object(payload, '$.runtime.pairity') AS pairity_version,
  get_json_object(payload, '$.runtime.spark') AS spark_version,
  report_hash
FROM {self.table}
WHERE record_type = 'report'"""
        )
        pair_type = (
            "array<struct<pair_id:string,status:string,reason:string,"
            "counts:struct<baseline_rows:bigint,candidate_rows:bigint,matched_rows:bigint,"
            "changed_rows:bigint,missing_rows:bigint,extra_rows:bigint>,"
            "column_changes:map<string,bigint>,schema_differences:array<string>>>"
        )
        self.spark.sql(
            f"""CREATE OR REPLACE VIEW {pairs_view} AS
SELECT
  r.run_id,
  r.suite,
  CAST(r.finished_at AS TIMESTAMP) AS finished_at,
  p.pair_id,
  p.status,
  p.reason,
  p.counts.baseline_rows,
  p.counts.candidate_rows,
  p.counts.matched_rows,
  p.counts.changed_rows,
  p.counts.missing_rows,
  p.counts.extra_rows,
  p.column_changes,
  p.schema_differences
FROM {self.table} r
LATERAL VIEW explode(from_json({report}.pairs'), '{pair_type}')) pairs AS p
WHERE r.record_type = 'report'"""
        )
        return {"runs_view": runs_view, "pairs_view": pairs_view}
