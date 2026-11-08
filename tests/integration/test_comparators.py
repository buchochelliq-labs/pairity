from datetime import datetime
from decimal import Decimal

import pytest

from pairity.config import ColumnMapping, ComparisonSettings, PairConfig
from pairity.plugins import PluginRegistry
from pairity.results import BlockedComparison, TableInput
from pairity.spark import SparkComparisonBackend

pytestmark = pytest.mark.spark


def compare(left, right, rules=None, *, comparators=None, **settings):
    pair = PairConfig(id="p", baseline="prod.s.t", candidate="dev.s.t")
    return SparkComparisonBackend(comparators).compare(
        pair,
        ComparisonSettings(column_rules=rules or {}, **settings),
        [ColumnMapping(baseline="id", candidate="id")],
        TableInput(left, pair.baseline),
        TableInput(right, pair.candidate),
    )


def frame(spark, rows, schema):
    return spark.createDataFrame(rows, schema)


def test_structs_compare_by_field_name_and_detect_nested_changes(spark):
    left = frame(spark, [(1, (1, "x")), (2, (2, "y"))], "id long, v struct<a:long,b:string>")
    right = frame(spark, [(1, ("x", 1)), (2, ("z", 2))], "id long, v struct<b:string,a:long>")
    result = compare(left, right)
    assert result.counts["changed_rows"] == 1
    assert result.comparators[0].pop("type_json").startswith('{"fields":')
    assert result.comparators == [
        {
            "column": "v",
            "candidate": "v",
            "type": "struct<a:bigint,b:string>",
            "comparator": "nested",
            "policy": {
                "struct_fields": "by_name",
                "map_entries": "by_key",
                "array_order": "ordered",
                "nested_nulls": "equal",
            },
        }
    ]
    renamed = frame(spark, [(1, (1, "x"))], "id long, v struct<a:long,c:string>")
    drift = compare(left, renamed)
    assert drift.status == "failed" and drift.counts is None
    assert drift.schema_differences == [
        "Type changed: v: struct<a:bigint,b:string> -> struct<a:bigint,c:string>"
    ]


def test_array_order_is_configurable_and_unordered_keeps_duplicates(spark):
    schema = "id long, v array<long>"
    left = frame(spark, [(1, [1, 2, 3]), (2, [1, 1, 2]), (3, None), (4, [])], schema)
    right = frame(spark, [(1, [3, 2, 1]), (2, [1, 2, 2]), (3, None), (4, [])], schema)
    assert compare(left, right).counts["changed_rows"] == 2
    unordered = compare(left, right, {"v": {"array_order": "unordered"}})
    assert unordered.counts["changed_rows"] == 1
    assert unordered.comparators[0]["policy"]["array_order"] == "unordered"


def test_maps_compare_by_content_independent_of_storage_order(spark):
    schema = "id long, v map<string,array<struct<k:long,m:map<string,long>>>>"
    a = {"x": [(1, {"p": 1, "q": 2})], "y": [(2, {})]}
    b = {"y": [(2, {})], "x": [(1, {"q": 2, "p": 1})]}
    changed = {"x": [(1, {"p": 1, "q": 3})], "y": [(2, {})]}
    left = frame(spark, [(1, a), (2, a), (3, {})], schema)
    right = frame(spark, [(1, b), (2, changed), (3, {})], schema)
    result = compare(left, right)
    assert result.counts["changed_rows"] == 1
    assert result.column_changes == {"v": 1}


def test_unordered_arrays_of_maps_and_structs(spark):
    schema = "id long, v array<struct<n:long,tags:map<string,long>>>"
    left = frame(spark, [(1, [(1, {"a": 1, "b": 2}), (2, {})])], schema)
    right = frame(spark, [(1, [(2, {}), (1, {"b": 2, "a": 1})])], schema)
    assert compare(left, right).counts["changed_rows"] == 1
    assert compare(left, right, {"v": {"array_order": "unordered"}}).counts["changed_rows"] == 0


def test_nested_nulls_equal_and_top_level_null_policy_is_respected(spark):
    schema = "id long, v struct<a:long,b:array<long>>"
    left = frame(spark, [(1, (None, [1, None])), (2, None), (3, (1, None))], schema)
    right = frame(spark, [(1, (None, [1, None])), (2, None), (3, (1, [1]))], schema)
    assert compare(left, right).counts["changed_rows"] == 1
    strict = compare(left, right, null_safe=False)
    # Only a top-level null participates in null_safe; nested nulls inside a present value
    # compare equal so that one null field cannot make the entire struct unknown.
    assert strict.counts["changed_rows"] == 2


def test_float_special_values_inside_nested_values(spark):
    schema = "id long, v array<double>"
    left = frame(spark, [(1, [float("nan")]), (2, [0.0]), (3, [1.0])], schema)
    right = frame(spark, [(1, [float("nan")]), (2, [-0.0]), (3, [1.0000001])], schema)
    result = compare(left, right)
    assert result.counts["changed_rows"] == 1
    assert result.column_changes == {"v": 1}


BIG = "123456789012345678901234567.000000000"


@pytest.mark.parametrize(
    ("dtype", "left", "right", "tolerance", "changed"),
    [
        ("decimal(12,2)", "10.00", "10.01", 0.01, 0),
        ("decimal(12,2)", "10.00", "10.02", 0.01, 1),
        # Tolerance finer than the column scale floors to the scale: 0.015 acts as 0.01.
        ("decimal(12,2)", "10.00", "10.02", 0.015, 1),
        ("decimal(37,10)", BIG + "0", BIG + "1", 1e-10, 0),
        ("decimal(37,10)", BIG + "0", BIG + "2", 1e-10, 1),
        ("long", 9223372036854775807, -9223372036854775808, 1.0, 1),
        ("long", 9007199254740993, 9007199254740992, 1.0, 0),
        ("int", 5, 7, 1.0, 1),
        ("decimal(12,2)", "10.00", "99999.00", 1e300, 0),
    ],
)  # fmt: skip
def test_decimal_and_integral_absolute_tolerance_is_exact(
    spark, dtype, left, right, tolerance, changed
):
    def value(item):
        return Decimal(item) if dtype.startswith("decimal") else item

    schema = f"id long, v {dtype}"
    result = compare(
        frame(spark, [(1, value(left)), (2, None)], schema),
        frame(spark, [(1, value(right)), (2, None)], schema),
        {"v": {"absolute_tolerance": tolerance}},
    )
    assert result.counts["changed_rows"] == changed
    assert result.comparators[0]["policy"]["arithmetic"] == "exact_decimal"


def test_decimal_tolerance_never_passes_null_against_value(spark):
    schema = "id long, v decimal(10,2)"
    result = compare(
        frame(spark, [(1, None), (2, Decimal("1.00"))], schema),
        frame(spark, [(1, Decimal("0.00")), (2, None)], schema),
        {"v": {"absolute_tolerance": 1000.0}},
    )
    assert result.counts["changed_rows"] == 2


def test_unsupported_combinations_block_and_list_every_affected_column(spark):
    schema = "id long, a decimal(38,2), b array<long>, c string, d map<string,long>"
    rows = [(1, Decimal("1.00"), [1], "x", {"k": 1})]
    data = frame(spark, rows, schema)
    rules = {
        "a": {"absolute_tolerance": 0.01},
        "b": {"relative_tolerance": 0.1},
        "c": {"array_order": "unordered"},
        "d": {"comparator": "missing"},
    }
    with pytest.raises(BlockedComparison) as error:
        compare(data, data, rules)
    message = str(error.value)
    assert message.startswith("Unsupported comparison policy: ")
    for column, reason in [
        ("a", "would lose precision"),
        ("b", "tolerances are not supported inside nested"),
        ("c", "array_order applies only"),
        ("d", "Unknown plugin: missing"),
    ]:
        assert f"{column}: " in message and reason in message


def test_compare_as_cannot_stringify_nested_columns(spark):
    data = frame(spark, [(1, (1,))], "id long, v struct<a:long>")
    with pytest.raises(BlockedComparison, match="compare_as is not supported .*: v"):
        compare(data, data, {"v": {"compare_as": "string"}})


def test_timestamps_compare_instants_and_ntz_is_a_distinct_type(spark):
    previous = spark.conf.get("spark.sql.session.timeZone")
    try:
        spark.conf.set("spark.sql.session.timeZone", "America/New_York")
        left = spark.sql("select 1L id, timestamp'2026-01-01 12:00:00+00:00' v")
        spark.conf.set("spark.sql.session.timeZone", "Asia/Tokyo")
        right = spark.sql("select 1L id, timestamp'2026-01-01 21:00:00+09:00' v")
        assert compare(left, right).counts["changed_rows"] == 0
        ntz = frame(spark, [(1, datetime(2026, 1, 1, 12))], "id long, v timestamp_ntz")
        result = compare(left, ntz, implicit_casts=False)
        assert result.schema_differences == ["Type changed: v: timestamp -> timestamp_ntz"]
    finally:
        spark.conf.set("spark.sql.session.timeZone", previous)


class CaseInsensitive:
    api_version = 1

    def problems(self, data_type, rule):
        return [] if data_type.typeName() == "string" else ["requires string"]

    def equal(self, left, right, data_type, rule, null_safe):
        from pyspark.sql import functions as F

        return F.lower(left).eqNullSafe(F.lower(right))

    def policy(self, data_type, rule):
        return {"case": "insensitive"}


def test_custom_comparators_extend_without_changing_orchestration(spark):
    registry = PluginRegistry()
    registry.register("case_insensitive", CaseInsensitive)
    left = frame(spark, [(1, "Hello", 1), (2, "a", 2)], "id long, v string, n long")
    right = frame(spark, [(1, "HELLO", 1), (2, "b", 2)], "id long, v string, n long")
    rules = {"v": {"comparator": "case_insensitive"}}
    result = compare(left, right, rules, comparators=registry)
    assert result.counts["changed_rows"] == 1
    assert result.comparators[0].pop("type_json") == '"string"'
    assert result.comparators == [
        {
            "column": "v",
            "candidate": "v",
            "type": "string",
            "comparator": "case_insensitive",
            "policy": {"case": "insensitive"},
        }
    ]
    with pytest.raises(BlockedComparison, match="n: requires string"):
        compare(left, right, {"n": {"comparator": "case_insensitive"}}, comparators=registry)


def test_custom_comparator_null_results_fail_closed_and_built_ins_cannot_be_shadowed(spark):
    class Unknown(CaseInsensitive):
        def equal(self, left, right, data_type, rule, null_safe):
            from pyspark.sql import functions as F

            return F.lit(None).cast("boolean")

    registry = PluginRegistry()
    registry.register("unknown", Unknown)
    data = frame(spark, [(1, "a")], "id long, v string")
    result = compare(data, data, {"v": {"comparator": "unknown"}}, comparators=registry)
    assert result.counts["changed_rows"] == 1
    shadow = PluginRegistry()
    shadow.register("exact", CaseInsensitive)
    with pytest.raises(ValueError, match="cannot replace built-ins"):
        SparkComparisonBackend(shadow)
