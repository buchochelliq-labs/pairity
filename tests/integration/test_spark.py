from contextlib import nullcontext
from decimal import Decimal
from threading import Barrier

import pytest

from pairity.config import ColumnMapping, ComparisonSettings, PairConfig, SuiteConfig
from pairity.engine import ComparisonRunner, PairityEngine
from pairity.results import BlockedComparison, TableInput
from pairity.spark import SparkComparisonBackend, SparkTableReader

pytestmark = pytest.mark.spark


def compare(left, right, *, settings=None, keys=None, columns=None):
    pair = PairConfig(
        id="orders", baseline="prod.s.orders", candidate="dev.s.orders", columns=columns or []
    )
    return SparkComparisonBackend().compare(
        pair,
        settings or ComparisonSettings(),
        keys or [ColumnMapping(baseline="id", candidate="id")],
        TableInput(left, pair.baseline),
        TableInput(right, pair.candidate),
    )


def test_exact_missing_extra_changed_counts_and_column_details(spark):
    left = spark.createDataFrame(
        [(1, "same", 10.0), (2, "before", 20.0), (3, None, 30.0)],
        "id long, value string, amount double",
    )
    right = spark.createDataFrame(
        [(1, "same", 10.005), (2, "after", 20.0), (4, None, 40.0)],
        "id long, value string, amount double",
    )
    result = compare(
        left,
        right,
        settings=ComparisonSettings(column_rules={"amount": {"absolute_tolerance": 0.01}}),
    )
    assert result.status == "failed"
    assert result.counts == {
        "baseline_rows": 3,
        "candidate_rows": 3,
        "matched_rows": 2,
        "changed_rows": 1,
        "missing_rows": 1,
        "extra_rows": 1,
    }
    assert result.column_changes == {"value": 1, "amount": 0}
    assert sorted(row.kind for row in result.differences.collect()) == [
        "changed",
        "extra",
        "missing",
    ]


@pytest.mark.parametrize("rows", [[(None, "a")], [(1, "a"), (1, "b")]])
def test_invalid_keys_block_before_full_outer_join(spark, rows):
    bad = spark.createDataFrame(rows, "id long, value string")
    good = spark.createDataFrame([(1, "a")], "id long, value string")
    for left, right in ((bad, good), (good, bad)):
        with pytest.raises(BlockedComparison, match="key"):
            compare(left, right)


def test_composite_renamed_keys_and_value_columns(spark):
    left = spark.createDataFrame([(1, 2, "ok")], "tenant long, id long, value string")
    right = spark.createDataFrame([(2, 1, "ok")], "order_key long, account long, renamed string")
    result = compare(
        left,
        right,
        keys=[
            ColumnMapping(baseline="tenant", candidate="account"),
            ColumnMapping(baseline="id", candidate="order_key"),
        ],
        columns=[ColumnMapping(baseline="value", candidate="renamed")],
    )
    assert result.status == "passed"


def test_schema_drift_is_not_silently_reduced_to_common_columns(spark):
    left = spark.createDataFrame([(1, "ok")], "id long, value string")
    right = spark.createDataFrame([(1, "ok", 9)], "id long, value string, new_col long")
    result = compare(left, right)
    assert result.status == "failed"
    assert result.counts is None
    assert any("new_col" in difference for difference in result.schema_differences)


def test_nested_types_and_typo_exclusions_block_instead_of_silent_pass(spark):
    nested = spark.createDataFrame([(1, {"a": 1})], "id long, value map<string,long>")
    with pytest.raises(BlockedComparison, match="Unsupported"):
        compare(nested, nested)
    flat = spark.createDataFrame([(1, "ok")], "id long, value string")
    with pytest.raises(BlockedComparison, match="exclusion"):
        compare(flat, flat, settings=ComparisonSettings(exclude_columns=["typo"]))


def test_empty_tables_and_key_only_explicit_opt_in(spark):
    empty = spark.createDataFrame([], "id long, value string")
    result = compare(empty, empty)
    assert result.status == "passed"
    assert result.counts["baseline_rows"] == 0
    only_keys = spark.createDataFrame([(1,)], "id long")
    with pytest.raises(BlockedComparison, match="columns"):
        compare(only_keys, only_keys)
    assert (
        compare(only_keys, only_keys, settings=ComparisonSettings(allow_key_only=True)).status
        == "passed"
    )


def test_null_nan_infinity_and_float_tolerance_boundaries(spark):
    schema = "id long, value double"
    left = spark.createDataFrame(
        [(1, None), (2, float("nan")), (3, float("inf")), (4, float("inf")), (5, 1.0), (6, 1.0)],
        schema,
    )
    right = spark.createDataFrame(
        [(1, None), (2, float("nan")), (3, float("inf")), (4, 3.0), (5, 1.125), (6, 1.25)], schema
    )
    result = compare(
        left,
        right,
        settings=ComparisonSettings(column_rules={"value": {"absolute_tolerance": 0.125}}),
    )
    assert result.counts["changed_rows"] == 2


def test_decimal_exact_comparison_keeps_precision_and_rejects_unimplemented_tolerance(spark):
    left = spark.createDataFrame(
        [(1, Decimal("9007199254740992.00"))], "id long, value decimal(30,2)"
    )
    right = spark.createDataFrame(
        [(1, Decimal("9007199254740992.01"))], "id long, value decimal(30,2)"
    )
    assert compare(left, right).counts["changed_rows"] == 1
    with pytest.raises(BlockedComparison, match="tolerance"):
        compare(
            left,
            right,
            settings=ComparisonSettings(column_rules={"value": {"absolute_tolerance": 0.1}}),
        )


def test_real_spark_pairs_execute_through_parallel_runner(spark):
    gate = Barrier(2)
    frame = spark.createDataFrame([(1, "ok")], "id long, value string")

    class Reader:
        api_version = 1

        def scope(self, run_id, pair_id):
            return nullcontext()

        def read(self, table, version=None):
            return TableInput(frame, table, snapshot_pinned=True)

    class Metadata:
        api_version = 1

        def primary_key(self, table):
            return ["id"]

    class Backend(SparkComparisonBackend):
        def compare(self, *args):
            gate.wait(timeout=15)
            return super().compare(*args)

    suite = SuiteConfig.model_validate(
        {
            "schema_version": 1,
            "suite": "parallel",
            "execution": {"max_parallel_pairs": 2},
            "pairs": [
                {"id": str(i), "baseline": f"prod.s.t{i}", "candidate": f"dev.s.t{i}"}
                for i in range(2)
            ],
        }
    )
    engine = PairityEngine(lambda: ComparisonRunner(Reader(), Metadata(), Backend()))
    result = engine.run(suite)
    assert result.status == "passed", result.to_dict()
    assert [r.counts["matched_rows"] for r in result.pairs] == [1, 1]


def test_spark_workers_inherit_context_and_isolate_job_groups_and_pools(spark):
    from pairity.spark import SparkExecutionBackend

    context = spark.sparkContext
    gate = Barrier(2)
    context.setLocalProperty("pairity.test.context", "caller")
    context.setLocalProperty("spark.scheduler.pool", "parent")

    def inspect_pair(pair_id):
        reader = SparkTableReader(spark.newSession())
        with reader.scope("run", pair_id):
            gate.wait(timeout=5)
            inside = (
                context.getLocalProperty("spark.jobGroup.id"),
                context.getLocalProperty("spark.scheduler.pool"),
                context.getLocalProperty("pairity.test.context"),
            )
        return inside, context.getLocalProperty("spark.scheduler.pool")

    try:
        results = SparkExecutionBackend(spark).map(inspect_pair, ["a", "b"], max_parallel_pairs=2)
        assert results == [
            (("pairity:run:a", "pairity:a", "caller"), "parent"),
            (("pairity:run:b", "pairity:b", "caller"), "parent"),
        ]
        assert context.getLocalProperty("spark.scheduler.pool") == "parent"
    finally:
        context.setLocalProperty("pairity.test.context", None)
        context.setLocalProperty("spark.scheduler.pool", None)


@pytest.mark.parametrize(
    "dtype,left_value,right_value,tolerance,changed",
    [
        ("double", 1e308, -1e308, 1.8, 1),
        ("double", 1e308, -1e308, 2.0, 0),
        ("float", 3e38, -3e38, 2.0, 0),
    ],
)
def test_relative_tolerance_does_not_overflow(
    dtype, left_value, right_value, tolerance, changed, spark
):
    left = spark.createDataFrame([(1, left_value)], f"id long, value {dtype}")
    right = spark.createDataFrame([(1, right_value)], f"id long, value {dtype}")
    result = compare(
        left,
        right,
        settings=ComparisonSettings(column_rules={"value": {"relative_tolerance": tolerance}}),
    )
    assert result.counts["changed_rows"] == changed
