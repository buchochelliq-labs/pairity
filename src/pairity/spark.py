"""Distributed scalar comparison and Databricks Delta input adapters.

Only this optional adapter module imports PySpark. No source data is written.
"""

from contextlib import contextmanager
from functools import reduce
from operator import and_, or_

from py4j.clientserver import ClientServer
from pyspark import inheritable_thread_target
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pairity.config import ColumnMapping
from pairity.engine import ComparisonRunner, PairityEngine
from pairity.execution import ThreadExecutionBackend
from pairity.metadata import UnityCatalogMetadata, quote_table
from pairity.results import BlockedComparison, PairResult, TableInput
from pairity.spark_casts import normalize_frames
from pairity.spark_sources import SparkSourceResolver

SCALAR_TYPES = (
    T.BooleanType,
    T.ByteType,
    T.ShortType,
    T.IntegerType,
    T.LongType,
    T.FloatType,
    T.DoubleType,
    T.DecimalType,
    T.StringType,
    T.BinaryType,
    T.DateType,
    T.TimestampType,
    T.TimestampNTZType,
    T.NullType,
)


def _quote(column):
    return "`" + column.replace("`", "``") + "`"


def _field(side, name):
    return F.col(f"{side}._row.{_quote(name)}")


def _validate_keys(frame, names, side):
    selected = frame.select(*[F.col(_quote(name)).alias(f"k{i}") for i, name in enumerate(names)])
    nulls = reduce(or_, [F.col(name).isNull() for name in selected.columns])
    if selected.where(nulls).limit(1).count():
        raise BlockedComparison(f"{side} primary key contains null values")
    grouped = selected.groupBy(*selected.columns).agg(F.count(F.lit(1)).alias("_count"))
    if grouped.where(F.col("_count") > 1).limit(1).count():
        raise BlockedComparison(f"{side} primary key contains duplicate values")


def _layout(left, right, keys, columns, settings, *, check_types=True):
    for frame in (left, right):
        if frame.isStreaming:
            raise BlockedComparison("Regression comparisons require batch snapshots")
        if len({c.casefold() for c in frame.columns}) != len(frame.columns):
            raise BlockedComparison("Ambiguous duplicate or case-colliding columns")
    left_types = {f.name: f.dataType for f in left.schema}
    right_types = {f.name: f.dataType for f in right.schema}
    for key in keys:
        if key.baseline not in left_types or key.candidate not in right_types:
            raise BlockedComparison("Configured primary key column does not exist on both sides")
        if check_types and left_types[key.baseline] != right_types[key.candidate]:
            raise BlockedComparison("Primary key column types must match exactly")
        if not isinstance(left_types[key.baseline], SCALAR_TYPES):
            raise BlockedComparison("Unsupported primary key type")
    if len({k.baseline for k in keys}) != len(keys) or len({k.candidate for k in keys}) != len(
        keys
    ):
        raise BlockedComparison("Duplicate primary key mappings")
    left_keys = {k.baseline for k in keys}
    right_keys = {k.candidate for k in keys}
    exclusions = set(settings.exclude_columns)
    if exclusions - (left_types.keys() | right_types.keys()):
        raise BlockedComparison("Configured exclusion column does not exist")
    if exclusions & (left_keys | right_keys):
        raise BlockedComparison("Primary key columns cannot be excluded")
    left_values = left_types.keys() - left_keys - exclusions
    right_values = right_types.keys() - right_keys - exclusions
    mappings = list(columns)
    if len({c.baseline for c in mappings}) != len(mappings) or len(
        {c.candidate for c in mappings}
    ) != len(mappings):
        raise BlockedComparison("Duplicate column mappings")
    for mapping in mappings:
        if mapping.baseline not in left_values or mapping.candidate not in right_values:
            raise BlockedComparison("Mapped value column is missing, excluded or a primary key")
    remaining_left = left_values - {m.baseline for m in mappings}
    remaining_right = right_values - {m.candidate for m in mappings}
    mappings += [
        ColumnMapping(baseline=name, candidate=name)
        for name in left.columns
        if name in remaining_left & remaining_right
    ]
    differences = [
        f"Missing candidate column: {name}" for name in sorted(remaining_left - remaining_right)
    ] + [f"Extra candidate column: {name}" for name in sorted(remaining_right - remaining_left)]
    if settings.column_policy == "common":
        differences = []
    for mapping in mappings:
        a, b = left_types[mapping.baseline], right_types[mapping.candidate]
        if check_types and a != b:
            differences.append(
                f"Type changed: {mapping.baseline}: {a.simpleString()} -> {b.simpleString()}"
            )
    if differences:
        return mappings, differences
    if not mappings and not settings.allow_key_only:
        raise BlockedComparison("No value columns to compare; set allow_key_only explicitly")
    if set(settings.column_rules) - {m.baseline for m in [*keys, *mappings]}:
        raise BlockedComparison("A column rule refers to a column that is not compared")
    for mapping in mappings:
        dtype = left_types[mapping.baseline]
        if not isinstance(dtype, SCALAR_TYPES):
            raise BlockedComparison(f"Unsupported comparison type for {mapping.baseline}: {dtype}")
        rule = settings.column_rules.get(mapping.baseline)
        if (
            check_types
            and rule
            and (rule.absolute_tolerance or rule.relative_tolerance)
            and not isinstance(dtype, (T.FloatType, T.DoubleType))
        ):
            raise BlockedComparison(
                "Numeric tolerance currently requires float/double columns; "
                "decimal and integer columns support exact comparison"
            )
    return mappings, differences


def _equal(left, right, rule, null_safe=True):
    if rule is not None and rule.null_safe is not None:
        null_safe = rule.null_safe
    exact = left.eqNullSafe(right) if null_safe else F.coalesce(left == right, F.lit(False))
    if rule is None or not (rule.absolute_tolerance or rule.relative_tolerance):
        return exact
    # FloatType subtraction may overflow even when the result fits in a double.
    left, right = left.cast("double"), right.cast("double")
    finite = (
        left.isNotNull()
        & right.isNotNull()
        & ~F.isnan(left)
        & ~F.isnan(right)
        & (F.abs(left) != float("inf"))
        & (F.abs(right) != float("inf"))
    )
    distance = F.abs(left - right)
    within = distance <= F.lit(rule.absolute_tolerance or 0)
    if rule.relative_tolerance:
        scale = F.greatest(F.abs(left), F.abs(right))
        # Opposite extreme doubles can overflow subtraction. Normalizing first
        # bounds that distance by two and avoids an infinity <= infinity false pass.
        scaled_distance = F.when(
            distance == float("inf"), F.abs(left / scale - right / scale)
        ).otherwise(distance / scale)
        within = within | (scaled_distance <= F.lit(rule.relative_tolerance))
    return exact | F.coalesce(finite & within, F.lit(False))


class SparkComparisonBackend:
    api_version = 1

    def compare(self, pair, settings, keys, baseline, candidate):
        if not keys:
            raise BlockedComparison("Primary keys are required")
        left, right = baseline.frame, candidate.frame
        mappings, schema_diffs = _layout(
            left, right, keys, pair.columns, settings, check_types=False
        )
        ignored = {
            "baseline": [
                c for c in left.columns if c not in {m.baseline for m in [*keys, *mappings]}
            ],
            "candidate": [
                c for c in right.columns if c not in {m.candidate for m in [*keys, *mappings]}
            ],
        }
        casts = []
        if not schema_diffs:
            left, right, casts = normalize_frames(left, right, [*keys, *mappings], settings)
            if any(c["invalid_baseline_rows"] or c["invalid_candidate_rows"] for c in casts):
                return PairResult(
                    pair.id,
                    "blocked",
                    reason="Invalid or lossy cast; row comparison was not executed",
                    casts=casts,
                    ignored_columns=ignored,
                )
            try:
                mappings, schema_diffs = _layout(left, right, keys, pair.columns, settings)
            except BlockedComparison as exc:
                exc.casts = casts
                exc.ignored_columns = ignored
                raise
        if schema_diffs:
            return PairResult(
                pair.id,
                "failed",
                schema_differences=schema_diffs,
                casts=casts,
                ignored_columns=ignored,
                reason="Schema differs; row comparison was not executed",
            )
        try:
            _validate_keys(left, [k.baseline for k in keys], "Baseline")
            _validate_keys(right, [k.candidate for k in keys], "Candidate")
        except BlockedComparison as exc:
            exc.casts = casts
            exc.ignored_columns = ignored
            raise

        def wrapped(frame, alias):
            return frame.select(
                F.lit(True).alias("_present"),
                F.struct(*[F.col(_quote(c)).alias(c) for c in frame.columns]).alias("_row"),
            ).alias(alias)

        condition = reduce(
            and_, [_field("b", key.baseline) == _field("c", key.candidate) for key in keys]
        )
        joined = wrapped(left, "b").join(wrapped(right, "c"), condition, "full_outer")
        present_left, present_right = (
            F.col("b._present").isNotNull(),
            F.col("c._present").isNotNull(),
        )
        matched = present_left & present_right
        changed_flags = [
            (
                m.baseline,
                matched
                & ~_equal(
                    _field("b", m.baseline),
                    _field("c", m.candidate),
                    settings.column_rules.get(m.baseline),
                    settings.null_safe,
                ),
            )
            for m in mappings
        ]
        any_changed = reduce(or_, [expr for _, expr in changed_flags], F.lit(False))
        changed_columns = F.filter(
            F.array(*[F.when(expr, F.lit(name)) for name, expr in changed_flags]).cast(
                "array<string>"
            ),
            lambda item: item.isNotNull(),
        )
        classified = joined.select(
            F.when(~present_left, F.lit("extra"))
            .when(~present_right, F.lit("missing"))
            .when(any_changed, F.lit("changed"))
            .otherwise(F.lit("equal"))
            .alias("kind"),
            F.col("b._row").alias("baseline"),
            F.col("c._row").alias("candidate"),
            changed_columns.alias("changed_columns"),
        )
        aggregates = [
            F.count(F.when(F.col("kind") == kind, 1)).alias(kind)
            for kind in ("equal", "changed", "missing", "extra")
        ]
        aggregates += [
            F.count(F.when(F.array_contains("changed_columns", m.baseline), 1)).alias(f"col_{i}")
            for i, m in enumerate(mappings)
        ]
        # Exactly one aggregate row reaches the driver, independent of input row count.
        summary = classified.agg(*aggregates).first().asDict()
        matched_count = summary["equal"] + summary["changed"]
        counts = {
            "baseline_rows": matched_count + summary["missing"],
            "candidate_rows": matched_count + summary["extra"],
            "matched_rows": matched_count,
            "changed_rows": summary["changed"],
            "missing_rows": summary["missing"],
            "extra_rows": summary["extra"],
        }
        failed = any(
            counts[f"{name}_rows"] > getattr(settings, f"max_{name}_rows")
            for name in ("changed", "missing", "extra")
        )
        return PairResult(
            pair.id,
            "failed" if failed else "passed",
            counts=counts,
            casts=casts,
            ignored_columns=ignored,
            column_changes={m.baseline: summary[f"col_{i}"] for i, m in enumerate(mappings)},
            differences=classified.where(F.col("kind") != "equal"),
        )


class SparkTableReader:
    """Delta-only reader: resolves each version once; a pair owns its SparkSession."""

    api_version = 1

    def __init__(self, spark):
        self.spark = spark

    @contextmanager
    def scope(self, run_id, pair_id):
        context = self.spark.sparkContext
        properties = (
            "spark.jobGroup.id",
            "spark.job.description",
            "spark.job.interruptOnCancel",
            "spark.scheduler.pool",
        )
        previous = {name: context.getLocalProperty(name) for name in properties}
        context.setJobGroup(f"pairity:{run_id}:{pair_id}", f"Pairity pair {pair_id}", True)
        context.setLocalProperty("spark.scheduler.pool", f"pairity:{pair_id}")
        try:
            yield
        finally:
            for name, value in previous.items():
                context.setLocalProperty(name, value)

    def read(self, table, version=None):
        quoted = quote_table(table)
        history = self.spark.sql(f"DESCRIBE HISTORY {quoted} LIMIT 1").first()
        if history is None:
            raise BlockedComparison(f"No Delta history available for {table}")
        resolved = int(history["version"]) if version is None else version
        frame = self.spark.read.option("versionAsOf", resolved).table(quoted)
        return TableInput(frame, table, version=resolved, snapshot_pinned=True)


class SparkExecutionBackend(ThreadExecutionBackend):
    """Capture an independent JVM property copy for each scheduled pair."""

    def __init__(self, spark):
        # Pinning is required for per-thread job groups, pools and caller context.
        if not isinstance(spark.sparkContext._gateway, ClientServer):
            raise ValueError("Classic Spark pinned-thread mode is required for parallel execution")

    def map(self, function, items, *, max_parallel_pairs):
        # Wrapping once would share a mutable Java Properties object across workers.
        targets = [inheritable_thread_target(lambda item=item: function(item)) for item in items]
        return super().map(lambda target: target(), targets, max_parallel_pairs=max_parallel_pairs)


def create_spark_engine(spark, *, sink=None):
    """Library and CLI composition root. No global session mutation or DQX dependency."""

    def factory():
        session = spark.newSession()
        metadata = UnityCatalogMetadata(
            lambda sql, params: (row.asDict() for row in session.sql(sql, args=params).collect())
        )
        return ComparisonRunner(
            SparkTableReader(session),
            metadata,
            SparkComparisonBackend(),
            source_resolver=SparkSourceResolver(),
        )

    return PairityEngine(factory, execution=SparkExecutionBackend(spark), sink=sink)
