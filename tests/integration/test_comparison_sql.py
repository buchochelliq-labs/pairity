"""Renderer equivalence: generated comparison SQL reproduces the engine's exact counts."""

from decimal import Decimal
from uuid import uuid4

import pytest

from pairity.comparison_sql import UnsupportedSql, comparison_sql
from pairity.config import ColumnMapping, ComparisonSettings, PairConfig
from pairity.plugins import PluginRegistry
from pairity.results import TableInput
from pairity.spark import SparkComparisonBackend

pytestmark = pytest.mark.spark


def engine_and_sql(spark, left, right, *, settings=None, keys=None, columns=None, backend=None):
    pair = PairConfig(id="p", baseline="prod.s.t", candidate="dev.s.t", columns=columns or [])
    settings = settings or ComparisonSettings()
    keys = keys or [ColumnMapping(baseline="id", candidate="id")]
    result = (backend or SparkComparisonBackend()).compare(
        pair, settings, keys, TableInput(left, pair.baseline), TableInput(right, pair.candidate)
    )
    assert result.status in {"passed", "failed"} and result.counts, result.reason
    views = []
    for frame in (left, right):
        name = f"pairity_eq_{uuid4().hex}"
        frame.createOrReplaceTempView(name)
        views.append(f"SELECT * FROM `{name}`")
    compared = [
        next((c for c in pair.columns if c.baseline == name), None)
        or ColumnMapping(baseline=name, candidate=name)
        for name in result.column_changes
    ]
    counts_sql, differences_sql = comparison_sql(
        *views,
        keys=keys,
        columns=compared,
        settings=settings,
        casts=result.casts,
        comparators=result.comparators,
    )
    return result, spark.sql(counts_sql).first().asDict(), spark.sql(differences_sql)


def assert_equivalent(result, row, differences):
    counts = result.counts
    assert row["changed_rows"] == counts["changed_rows"]
    assert row["missing_rows"] == counts["missing_rows"]
    assert row["extra_rows"] == counts["extra_rows"]
    assert row["equal_rows"] + row["changed_rows"] == counts["matched_rows"]
    for name, value in result.column_changes.items():
        assert row[f"column:{name}"] == value, name
    kinds = sorted(r.kind for r in differences.collect())
    expected = sorted(
        ["changed"] * counts["changed_rows"]
        + ["missing"] * counts["missing_rows"]
        + ["extra"] * counts["extra_rows"]
    )
    assert kinds == expected


SCENARIOS = {
    "scalars_missing_extra_and_nulls": (
        "id long, v string, n long",
        [(1, "a", 1), (2, "b", None), (3, None, 3), (4, "d", 4)],
        [(1, "a", 1), (2, "B", None), (3, None, 30), (5, "e", 5)],
        {},
    ),
    "strict_null_policy_per_column": (
        "id long, v string, n long",
        [(1, None, None), (2, "x", 1)],
        [(1, None, None), (2, "x", 1)],
        {"null_safe": True, "column_rules": {"v": {"null_safe": False}}},
    ),
    "float_absolute_relative_nan_infinity": (
        "id long, f double, g float",
        [(1, 1.0, 1.0), (2, float("nan"), 2.0), (3, float("inf"), 3.0), (4, 1e308, 1e38),
         (5, 100.0, 5.0), (6, None, None)],
        [(1, 1.06, 1.2), (2, float("nan"), 2.4), (3, 3.0, 3.0), (4, -1e308, -1e38),
         (5, 104.0, 5.0), (6, 0.0, None)],
        {"column_rules": {"f": {"absolute_tolerance": 0.05, "relative_tolerance": 0.05},
                          "g": {"relative_tolerance": 2.0}}},
    ),
    "exact_decimal_and_integral_tolerances": (
        "id long, d decimal(12,2), w decimal(37,10), i long",
        [(1, Decimal("10.00"), Decimal("1.0000000000"), 9007199254740993),
         (2, Decimal("10.00"), Decimal("1.0000000000"), 1), (3, None, None, None)],
        [(1, Decimal("10.01"), Decimal("1.0000000001"), 9007199254740992),
         (2, Decimal("10.02"), Decimal("1.0000000002"), 3), (3, Decimal("0.00"), None, None)],
        {"column_rules": {"d": {"absolute_tolerance": 0.015},
                          "w": {"absolute_tolerance": 1e-10},
                          "i": {"absolute_tolerance": 1.0}}},
    ),
    "nested_structs_maps_and_unordered_arrays": (
        "id long, s struct<a:long,b:array<long>>, m map<string,array<long>>, a array<long>",
        [(1, (1, [1, 2]), {"x": [1], "y": [2]}, [1, 2, 2]),
         (2, (2, None), {"x": [1]}, [3, 1]), (3, None, None, None)],
        [(1, (1, [1, 2]), {"y": [2], "x": [1]}, [2, 1, 2]),
         (2, (2, [9]), {"x": [2]}, [1, 1]), (3, None, None, [])],
        {"column_rules": {"a": {"array_order": "unordered"}}},
    ),
}  # fmt: skip


@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_rendered_sql_matches_engine_counts(spark, name):
    schema, left_rows, right_rows, settings = SCENARIOS[name]
    result, row, differences = engine_and_sql(
        spark,
        spark.createDataFrame(left_rows, schema),
        spark.createDataFrame(right_rows, schema),
        settings=ComparisonSettings.model_validate(settings),
    )
    assert result.counts["changed_rows"] > 0 or result.counts["extra_rows"] > 0
    assert_equivalent(result, row, differences)


def test_struct_field_order_casts_and_renamed_composite_keys(spark):
    left = spark.createDataFrame(
        [(1, "01", 5, (1, "x")), (1, "02", 6, (2, "y")), (2, "01", 7, (3, "z"))],
        "tenant long, code string, amount int, s struct<a:long,b:string>",
    )
    right = spark.createDataFrame(
        [(1, 1, 5, ("x", 1)), (1, 2, 60, ("y", 2)), (2, 1, 7, ("Z", 3))],
        "account long, code_num long, total long, s struct<b:string,a:long>",
    )
    result, row, differences = engine_and_sql(
        spark,
        left,
        right,
        keys=[
            ColumnMapping(baseline="tenant", candidate="account"),
            ColumnMapping(baseline="code", candidate="code_num"),
        ],
        columns=[ColumnMapping(baseline="amount", candidate="total")],
        settings=ComparisonSettings.model_validate(
            {"column_rules": {"code": {"compare_as": "decimal(38,0)"}}}
        ),
    )
    assert {c["compare_as"] for c in result.casts} == {"decimal(38,0)", "long"}
    assert result.counts["changed_rows"] == 2
    assert_equivalent(result, row, differences)


def test_custom_comparators_are_reported_unsupported_not_approximated(spark):
    class Lower:
        api_version = 1

        def problems(self, data_type, rule):
            return []

        def equal(self, left, right, data_type, rule, null_safe):
            from pyspark.sql import functions as F

            return F.lower(left).eqNullSafe(F.lower(right))

        def policy(self, data_type, rule):
            return {}

    registry = PluginRegistry()
    registry.register("lower", Lower)
    frame = spark.createDataFrame([(1, "A")], "id long, v string")
    with pytest.raises(UnsupportedSql, match="custom comparator lower"):
        engine_and_sql(
            spark,
            frame,
            frame,
            settings=ComparisonSettings.model_validate(
                {"column_rules": {"v": {"comparator": "lower"}}}
            ),
            backend=SparkComparisonBackend(registry),
        )


@pytest.mark.skipif(
    not __import__("os").environ.get("PAIRITY_TEST_DELTA"), reason="Set PAIRITY_TEST_DELTA=1"
)
def test_exported_reproduce_sql_reproduces_counts_after_tables_change(spark):
    from pairity.config import SuiteConfig
    from pairity.engine import ComparisonRunner, PairityEngine
    from pairity.incidents import IncidentExporter
    from pairity.spark import SparkTableReader
    from pairity.spark_sources import SparkSourceResolver

    suffix = uuid4().hex[:8]
    names = {side: f"spark_catalog.default.pairity_repro_{side}_{suffix}" for side in "bc"}
    schema = "id long, region string, amount decimal(12,2), tags map<string,long>"
    spark.createDataFrame(
        [(1, "EU", Decimal("1.00"), {"a": 1, "b": 2}), (2, "EU", Decimal("2.00"), {}),
         (3, "US", Decimal("3.00"), {}), (4, "EU", Decimal("4.00"), {"x": 1})],
        schema,
    ).write.format("delta").saveAsTable(names["b"])  # fmt: skip
    spark.createDataFrame(
        [(1, "EU", Decimal("1.01"), {"b": 2, "a": 1}), (2, "EU", Decimal("2.50"), {}),
         (3, "US", Decimal("9.00"), {}), (5, "EU", Decimal("5.00"), {})],
        schema,
    ).write.format("delta").saveAsTable(names["c"])  # fmt: skip

    class Metadata:
        api_version = 1

        def primary_key(self, table):
            return ["id"]

    scope = [{"column": "region", "op": "=", "value": "EU"}]
    config = SuiteConfig.model_validate(
        {
            "schema_version": 1,
            "suite": "repro",
            "defaults": {"column_rules": {"amount": {"absolute_tolerance": 0.01}}},
            "pairs": [
                {
                    "id": "p",
                    "baseline": {"kind": "table", "table": names["b"], "where": scope},
                    "candidate": {"kind": "table", "table": names["c"], "where": scope},
                    "keys": [{"baseline": "id", "candidate": "id"}],
                }
            ],
        }
    )
    try:
        run = PairityEngine(
            lambda: ComparisonRunner(
                SparkTableReader(spark),
                Metadata(),
                SparkComparisonBackend(),
                source_resolver=SparkSourceResolver(),
            )
        ).run(config)
        outcome = run.pairs[0]
        assert outcome.counts["changed_rows"] == 1 and outcome.counts["extra_rows"] == 1
        files = IncidentExporter().export(run.to_dict())
        manifest = __import__("json").loads(files["manifest.json"])
        assert manifest["reproduction"]["exact_comparison_sql"] == {"p": True}
        blocks = files["reproduce.sql"].decode().split("\n\n")
        counts_sql = blocks[blocks.index(
            "-- Pair 1 comparison counts (expected: p in result.json)"
        ) + 1].rstrip(";")  # fmt: skip
        # New commits must not change the reproduction: it reads the recorded versions.
        for name in names.values():
            spark.createDataFrame([(9, "EU", Decimal("9.00"), {})], schema).write.format(
                "delta"
            ).mode("append").saveAsTable(name)
        row = spark.sql(counts_sql).first().asDict()
        assert row["changed_rows"] == outcome.counts["changed_rows"]
        assert row["missing_rows"] == outcome.counts["missing_rows"]
        assert row["extra_rows"] == outcome.counts["extra_rows"]
        assert row["column:amount"] == outcome.column_changes["amount"]
        assert row["column:tags"] == outcome.column_changes["tags"] == 0
    finally:
        for name in names.values():
            spark.sql(f"DROP TABLE IF EXISTS {name}")
