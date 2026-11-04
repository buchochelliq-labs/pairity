import pytest
from test_spark import compare

from pairity.config import ComparisonSettings
from pairity.results import BlockedComparison

pytestmark = pytest.mark.spark


def test_implicit_numeric_string_casts_by_default_and_reports(spark):
    a = spark.createDataFrame([(1, "00042"), (2, None)], "id long, value string")
    b = spark.createDataFrame([(1, 42), (2, None)], "id long, value long")
    result = compare(a, b)
    assert result.status == "passed"
    assert result.casts[0]["mode"] == "implicit"
    assert result.casts[0]["compare_as"] == "decimal(38,0)"
    assert result.to_dict()["casts"] == result.casts


def test_implicit_cast_switches_and_explicit_override(spark):
    a = spark.createDataFrame([(1, "42")], "id long, value string")
    b = spark.createDataFrame([(1, 42)], "id long, value long")
    for settings in (
        {"implicit_casts": False},
        {"column_rules": {"value": {"implicit_casts": False}}},
    ):
        assert compare(a, b, settings=ComparisonSettings(**settings)).status == "failed"
    for rule in ({"compare_as": "long"}, {"implicit_casts": True}):
        result = compare(
            a, b, settings=ComparisonSettings(implicit_casts=False, column_rules={"value": rule})
        )
        assert result.status == "passed"


@pytest.mark.parametrize("value", ["nope", "1.2", "999999999999999999999999999999999999999"])
def test_implicit_cast_invalid_or_lossy_values_cannot_pass(spark, value):
    a = spark.createDataFrame([(1, value)], "id long, value string")
    b = spark.createDataFrame([(1, None)], "id long, value long")
    result = compare(a, b)
    assert result.status == "blocked"
    assert result.casts[0]["invalid_baseline_rows"] == 1


def test_common_columns_and_exclusions_are_audited(spark):
    a = spark.createDataFrame([(1, "ok", 3, 8)], "id long, value string, drop long, old long")
    b = spark.createDataFrame([(1, "ok", 4, 9)], "id long, value string, drop long, new long")
    result = compare(
        a, b, settings=ComparisonSettings(column_policy="common", exclude_columns=["drop"])
    )
    assert result.status == "passed"
    assert result.ignored_columns == {"baseline": ["drop", "old"], "candidate": ["drop", "new"]}


def test_null_safe_off_counts_nulls_as_different_and_column_override(spark):
    a = spark.createDataFrame([(1, None), (2, None), (3, "ok")], "id long, value string")
    b = spark.createDataFrame([(1, None), (2, "x"), (3, "ok")], "id long, value string")
    assert compare(a, b).counts["changed_rows"] == 1
    assert compare(a, b, settings=ComparisonSettings(null_safe=False)).counts["changed_rows"] == 2
    assert (
        compare(
            a,
            b,
            settings=ComparisonSettings(
                null_safe=False, column_rules={"value": {"null_safe": True}}
            ),
        ).counts["changed_rows"]
        == 1
    )


def test_cast_primary_keys_revalidated_for_collisions(spark):
    a = spark.createDataFrame([("01", "a"), ("1", "b")], "id string, value string")
    b = spark.createDataFrame([(1, "a")], "id long, value string")
    with pytest.raises(BlockedComparison, match="duplicate"):
        compare(a, b)


def test_template_union_dedup_preserves_leaf_versions_and_requires_keys(spark):
    from contextlib import nullcontext

    from pairity.config import SuiteConfig
    from pairity.engine import ComparisonRunner, PairityEngine
    from pairity.results import TableInput
    from pairity.spark import SparkComparisonBackend

    data = {
        "schema_version": 1,
        "suite": "sources",
        "templates": {
            "unique": {
                "parameters": ["rows"],
                "source": {"kind": "deduplicate", "source": {"kind": "parameter", "name": "rows"}},
            }
        },
        "pairs": [
            {
                "id": "p",
                "baseline": {
                    "kind": "template",
                    "name": "unique",
                    "inputs": {
                        "rows": {"kind": "union", "sources": ["prod.s.current", "prod.s.archive"]}
                    },
                },
                "candidate": "dev.s.orders",
                "keys": [{"baseline": "id", "candidate": "id"}],
            }
        ],
    }
    frames = {
        "prod.s.current": spark.createDataFrame([(1, "a"), (2, "b")], "id long, value string"),
        "prod.s.archive": spark.createDataFrame([(1, "a")], "id long, value string"),
        "dev.s.orders": spark.createDataFrame([(2, "b"), (1, "a")], "id long, value string"),
    }
    calls = []

    class Reader:
        api_version = 1

        def scope(self, *args):
            return nullcontext()

        def read(self, table, version=None):
            calls.append(table)
            return TableInput(frames[table], table, version=7, snapshot_pinned=True)

    class Metadata:
        api_version = 1

        def primary_key(self, table):
            raise AssertionError("Derived sources must not guess PKs")

    from pairity.spark_sources import SparkSourceResolver

    engine = PairityEngine(
        lambda: ComparisonRunner(
            Reader(), Metadata(), SparkComparisonBackend(), source_resolver=SparkSourceResolver()
        )
    )
    run = engine.run(SuiteConfig.model_validate(data))
    assert run.status == "passed", run.to_dict()
    assert run.pairs[0].counts["baseline_rows"] == 2
    assert [leaf["version"] for leaf in run.pairs[0].inputs["baseline"]["leaves"]] == [7, 7]
    del data["pairs"][0]["keys"]
    run = engine.run(SuiteConfig.model_validate(data))
    assert run.status == "blocked"
    assert "explicit" in run.pairs[0].reason


@pytest.mark.parametrize(
    "value,expected",
    [("42", "passed"), ("9007199254740993", "blocked"), ("0.100000000000000001", "blocked")],
)
def test_numeric_strings_to_double_do_not_hide_precision_loss(spark, value, expected):
    a = spark.createDataFrame([(1, value)], "id long, value string")
    b = spark.createDataFrame([(1, float(value))], "id long, value double")
    assert compare(a, b).status == expected


def test_integral_double_alignment_rejects_lossy_values(spark):
    for value, expected in [(42, "passed"), (9007199254740993, "blocked")]:
        a = spark.createDataFrame([(1, value)], "id long, value long")
        b = spark.createDataFrame([(1, float(value))], "id long, value double")
        assert compare(a, b).status == expected


def test_engine_retains_cast_and_snapshot_audit_on_key_collision(spark):
    from contextlib import nullcontext

    from pairity.config import SuiteConfig
    from pairity.engine import ComparisonRunner, PairityEngine
    from pairity.results import TableInput
    from pairity.spark import SparkComparisonBackend

    frames = {
        "prod.s.a": spark.createDataFrame([("01", "a"), ("1", "b")], "id string, value string"),
        "dev.s.a": spark.createDataFrame([(1, "a")], "id long, value string"),
    }

    class Reader:
        api_version = 1

        def scope(self, *args):
            return nullcontext()

        def read(self, table, version=None):
            return TableInput(frames[table], table, version=3, snapshot_pinned=True)

    class Metadata:
        api_version = 1

        def primary_key(self, table):
            return ["id"]

    config = SuiteConfig.model_validate(
        {
            "schema_version": 1,
            "suite": "collision",
            "pairs": [{"id": "p", "baseline": "prod.s.a", "candidate": "dev.s.a"}],
        }
    )
    result = (
        PairityEngine(lambda: ComparisonRunner(Reader(), Metadata(), SparkComparisonBackend()))
        .run(config)
        .pairs[0]
    )
    assert result.status == "blocked"
    assert result.casts[0]["compare_as"] == "decimal(38,0)"
    assert result.inputs["baseline"]["version"] == 3
    assert result.keys == [{"baseline": "id", "candidate": "id"}]


def test_union_never_coerces_types_and_dedup_does_not_pick_key_survivor(spark):
    from pairity.config import DeduplicateSource, UnionSource
    from pairity.results import TableInput
    from pairity.spark_sources import SparkSourceResolver

    frames = {
        "a.s.a": spark.createDataFrame([(1, "a"), (1, "b")], "id long, value string"),
        "b.s.b": spark.createDataFrame([(1, 3)], "id long, value long"),
    }

    class Reader:
        def read(self, table, version=None):
            return TableInput(frames[table], table)

    resolver = SparkSourceResolver()
    with pytest.raises(BlockedComparison, match="identical named column types"):
        resolver.read(UnionSource(kind="union", sources=list(frames)), Reader(), {})
    dedup = resolver.read(DeduplicateSource(kind="deduplicate", source="a.s.a"), Reader(), {})
    assert dedup.frame.count() == 2
    with pytest.raises(BlockedComparison, match="duplicate"):
        compare(dedup.frame, dedup.frame)
