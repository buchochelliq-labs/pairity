import os
import time
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest

from pairity.config import ColumnFilter, SuiteConfig
from pairity.engine import ComparisonRunner, PairityEngine
from pairity.results import BlockedComparison, TableInput
from pairity.spark import SparkComparisonBackend, SparkTableReader
from pairity.spark_sources import SparkSourceResolver, apply_filters

pytestmark = pytest.mark.spark


def filters(*items):
    return [ColumnFilter.model_validate(item) for item in items]


@pytest.fixture
def frame(spark):
    return spark.createDataFrame(
        [
            (1, "EU", "2026-01-01", "2026-01-01 00:30:00", Decimal("1.10"), 1.5, True, None),
            (2, "US", "2026-01-15", "2026-01-15 12:00:00", Decimal("2.00"), 2.5, False, "x"),
            (3, "EU", "2026-02-01", "2026-02-01 00:00:00", Decimal("3.25"), None, None, None),
        ],
        "id int, region string, day string, ts string, amount decimal(10,2), score double, "
        "flag boolean, deleted string",
    ).selectExpr(
        "id",
        "region",
        "cast(day as date) day",
        "to_utc_timestamp(ts, 'UTC') ts",
        "cast(ts as timestamp_ntz) local_ts",
        "amount",
        "score",
        "flag",
        "deleted",
    )


def ids(result):
    return sorted(row.id for row in result.collect())


@pytest.mark.parametrize(
    ("items", "expected"),
    [
        ([{"column": "region", "op": "=", "value": "EU"}], [1, 3]),
        ([{"column": "region", "op": "!=", "value": "EU"}], [2]),
        ([{"column": "id", "op": "in", "value": [1, 3, 99]}], [1, 3]),
        ([{"column": "id", "op": "not_in", "value": [1]}], [2, 3]),
        ([{"column": "day", "op": "between", "value": ["2026-01-01", "2026-01-31"]}], [1, 2]),
        ([{"column": "day", "op": ">=", "value": "2026-01-15"}], [2, 3]),
        ([{"column": "amount", "op": ">", "value": "1.10"}], [2, 3]),
        ([{"column": "amount", "op": "<=", "value": 2}], [1, 2]),
        ([{"column": "score", "op": "<", "value": 2}], [1]),
        ([{"column": "flag", "op": "=", "value": True}], [1]),
        ([{"column": "deleted", "op": "is_null"}], [1, 3]),
        ([{"column": "deleted", "op": "is_not_null"}], [2]),
        # Instants with offsets compare as instants, independent of the session time zone.
        ([{"column": "ts", "op": "<", "value": "2026-01-01T09:30:00+09:00"}], []),
        ([{"column": "ts", "op": "<=", "value": "2026-01-01T09:30:00+09:00"}], [1]),
        ([{"column": "local_ts", "op": "<", "value": "2026-01-15T12:00:00"}], [1]),
        (
            [
                {"column": "region", "op": "=", "value": "EU"},
                {"column": "day", "op": "<", "value": "2026-02-01"},
            ],
            [1],
        ),
    ],
)
def test_typed_filters_select_expected_rows(spark, frame, items, expected):
    previous = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    try:
        assert ids(apply_filters(frame, filters(*items), "prod.s.t")) == expected
    finally:
        spark.conf.set("spark.sql.session.timeZone", previous)


def test_invalid_filters_block_and_list_every_problem(frame):
    items = filters(
        {"column": "missing", "op": "=", "value": 1},
        {"column": "id", "op": "=", "value": "1"},
        {"column": "id", "op": "=", "value": 2**40},
        {"column": "amount", "op": "=", "value": 1.1},
        {"column": "amount", "op": "=", "value": "1.105"},
        {"column": "day", "op": "=", "value": "01/02/2026"},
        {"column": "ts", "op": "=", "value": "2026-01-01T00:00:00"},
        {"column": "local_ts", "op": "=", "value": "2026-01-01T00:00:00Z"},
        {"column": "flag", "op": "=", "value": 1},
    )
    with pytest.raises(BlockedComparison) as error:
        apply_filters(frame, items, "prod.s.t")
    message = str(error.value)
    assert message.startswith("Invalid filter on prod.s.t: missing: column does not exist")
    for column in ("id", "amount", "day", "ts", "local_ts", "flag"):
        assert f"; {column}: " in message
    assert message.count(";") == len(items) - 1


class Reader:
    api_version = 1

    def __init__(self, frames):
        self.frames, self.reads = frames, []

    def scope(self, *args):
        return nullcontext()

    def read(self, table, version=None):
        self.reads.append((table, version))
        return TableInput(self.frames[table], table, 4, snapshot_pinned=True)


class Metadata:
    api_version = 1

    def primary_key(self, table):
        return ["id"]


def run(reader, baseline):
    config = SuiteConfig.model_validate(
        {
            "schema_version": 1,
            "suite": "scoped",
            "pairs": [
                {
                    "id": "p",
                    "baseline": baseline,
                    "candidate": {
                        "kind": "table",
                        "table": "dev.s.t",
                        "where": [{"column": "region", "op": "=", "value": "EU"}],
                    },
                    "keys": [{"baseline": "id", "candidate": "id"}],
                }
            ],
        }
    )
    return (
        PairityEngine(
            lambda: ComparisonRunner(
                reader, Metadata(), SparkComparisonBackend(), source_resolver=SparkSourceResolver()
            )
        )
        .run(config)
        .pairs[0]
    )


def test_engine_applies_scopes_and_records_them_in_provenance(frame):
    reader = Reader({"prod.s.t": frame, "dev.s.t": frame})
    scoped = {
        "kind": "table",
        "table": "prod.s.t",
        "where": [{"column": "region", "op": "in", "value": ["EU"]}],
    }
    result = run(reader, scoped)
    assert result.status == "passed", result.reason
    assert result.counts["matched_rows"] == 2
    assert result.inputs["baseline"]["filters"] == [
        {"column": "region", "op": "in", "value": ["EU"]}
    ]
    # A filtered and an unfiltered read of one snapshot share the cached read.
    union = {"kind": "union", "sources": [scoped, "prod.s.t"]}
    reader.reads.clear()
    run(reader, {"kind": "deduplicate", "source": union})
    assert reader.reads.count(("prod.s.t", None)) == 1


def test_readers_without_as_of_support_block(frame):
    reader = Reader({"prod.s.t": frame, "dev.s.t": frame})
    result = run(reader, {"kind": "table", "table": "prod.s.t", "as_of": "2026-01-01T00:00:00Z"})
    assert result.status == "blocked"
    assert "does not support as_of" in result.reason


delta = pytest.mark.skipif(
    not os.environ.get("PAIRITY_TEST_DELTA"), reason="Set PAIRITY_TEST_DELTA=1 for Delta"
)


@pytest.fixture
def delta_table(spark):
    name = f"spark_catalog.default.pairity_snap_{uuid4().hex[:8]}"
    spark.createDataFrame([(1, "a"), (2, "b")], "id long, v string").write.format(
        "delta"
    ).saveAsTable(name)
    time.sleep(1.1)  # Distinct commit timestamps for as_of boundaries.
    spark.createDataFrame([(3, "c")], "id long, v string").write.format("delta").mode(
        "append"
    ).saveAsTable(name)
    yield name
    spark.sql(f"DROP TABLE IF EXISTS {name}")


def commit_times(spark, name):
    rows = spark.sql(f"DESCRIBE HISTORY {name}").select("version", "timestamp").collect()
    return {
        row.version: row.timestamp.astimezone(timezone.utc)
        if row.timestamp.tzinfo
        else row.timestamp.astimezone().astimezone(timezone.utc)
        for row in rows
    }


@delta
def test_pinned_snapshot_is_stable_across_actions_writes_and_retries(spark, delta_table):
    reader = SparkTableReader(spark)
    pinned = reader.read(delta_table, 0)
    assert pinned.frame.count() == 2
    spark.createDataFrame([(4, "d")], "id long, v string").write.format("delta").mode(
        "append"
    ).saveAsTable(delta_table)
    # Later actions on the same frame and a retried read both see version 0 only.
    assert pinned.frame.count() == 2
    assert sorted(r.id for r in reader.read(delta_table, 0).frame.collect()) == [1, 2]
    latest = reader.read(delta_table)
    assert latest.version == 2 and latest.frame.count() == 4


@delta
def test_as_of_resolves_commit_versions_and_rejects_unreproducible_instants(spark, delta_table):
    reader = SparkTableReader(spark)
    times = commit_times(spark, delta_table)
    iso = lambda moment: moment.isoformat().replace("+00:00", "Z")  # noqa: E731
    between = times[0] + (times[1] - times[0]) / 2
    resolved = reader.read(delta_table, as_of=iso(between))
    assert (resolved.version, resolved.frame.count()) == (0, 2)
    assert resolved.metadata()["as_of"] == iso(between)
    assert reader.read(delta_table, as_of=iso(times[1])).version == 1
    # The same instant expressed in another offset resolves identically.
    tokyo = times[1].astimezone(timezone(timedelta(hours=9))).isoformat()
    assert reader.read(delta_table, as_of=tokyo).version == 1
    assert reader.read(delta_table, 1, as_of=iso(times[1])).version == 1
    with pytest.raises(BlockedComparison, match="conflicts with as_of"):
        reader.read(delta_table, 0, as_of=iso(times[1]))
    with pytest.raises(BlockedComparison, match="no retained Delta version"):
        reader.read(delta_table, as_of=iso(times[0] - timedelta(days=1)))
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    with pytest.raises(BlockedComparison, match="in the future"):
        reader.read(delta_table, as_of=iso(future))
