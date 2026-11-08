from contextlib import nullcontext

import pytest

from pairity.config import SuiteConfig
from pairity.engine import ComparisonRunner, PairityEngine
from pairity.execution import ThreadExecutionBackend
from pairity.results import TableInput
from pairity.spark import SparkComparisonBackend
from pairity.spark_sources import SparkSourceResolver
from pairity.sql_guard import query_hash

pytestmark = pytest.mark.spark


class Reader:
    """Serves fixed frames as pinned snapshots and records every requested read."""

    api_version = 1

    def __init__(self, frames, version=5, pinned=True):
        self.frames, self.version, self.pinned, self.reads = frames, version, pinned, []

    def scope(self, *args):
        return nullcontext()

    def read(self, table, version=None):
        self.reads.append((table, version))
        resolved = self.version if version is None else version
        return TableInput(self.frames[table], table, resolved, snapshot_pinned=self.pinned)


class NoMetadata:
    api_version = 1

    def primary_key(self, table):
        return ["id"]


def run(spark_frames, baseline, *, keys=True, reader=None):
    reader = reader or Reader(spark_frames)
    config = SuiteConfig.model_validate(
        {
            "schema_version": 1,
            "suite": "sql",
            "pairs": [
                {
                    "id": "p",
                    "baseline": baseline,
                    "candidate": "dev.s.expected",
                    **({"keys": [{"baseline": "id", "candidate": "id"}]} if keys else {}),
                }
            ],
        }
    )
    engine = PairityEngine(
        lambda: ComparisonRunner(
            reader, NoMetadata(), SparkComparisonBackend(), source_resolver=SparkSourceResolver()
        ),
        execution=ThreadExecutionBackend(),
    )
    return engine.run(config).pairs[0], reader


@pytest.fixture
def frames(spark):
    orders = spark.createDataFrame(
        [(1, 10, "2026-01-05", 5.0), (2, 10, "2026-02-01", 7.0), (3, 20, "2026-02-03", 1.0)],
        "id long, customer_id long, day string, amount double",
    )
    archive = spark.createDataFrame(
        [(3, 20, "2026-02-03", 1.0), (4, 30, "2026-03-01", 2.0)],
        "id long, customer_id long, day string, amount double",
    )
    customers = spark.createDataFrame(
        [(10, "O'Brien"), (20, "Ng"), (30, "Ito")], "id long, name string"
    )
    expected = spark.createDataFrame(
        [(2, "O'Brien", 7.0), (3, "Ng", 1.0), (4, "Ito", 2.0)],
        "id long, name string, amount double",
    )
    return {
        "prod.s.orders": orders,
        "prod.s.archive": archive,
        "prod.s.customers": customers,
        "dev.s.expected": expected,
    }


QUERY = """
WITH combined AS (
  SELECT * FROM {{orders}} UNION SELECT * FROM {{archive}}   -- set union deduplicates
)
SELECT o.id, c.name, o.amount
FROM combined o JOIN {{customers}} c ON o.customer_id = c.id
WHERE o.day >= :start
"""


def sql_source(**overrides):
    return {
        "kind": "sql",
        "query": QUERY,
        "inputs": {
            "orders": "prod.s.orders",
            "archive": {"kind": "table", "table": "prod.s.archive", "version": 2},
            "customers": "prod.s.customers",
        },
        "parameters": {"start": "2026-02-01"},
    } | overrides


def test_join_filter_union_dedup_query_compares_with_recorded_provenance(frames):
    result, reader = run(frames, sql_source())
    assert result.status == "passed", result.reason
    assert result.counts["matched_rows"] == 3
    baseline = result.inputs["baseline"]
    assert baseline["table"] == "sql" and baseline["snapshot_pinned"] is True
    assert baseline["leaves"] == [
        {"table": "prod.s.orders", "version": 5, "snapshot_pinned": True},
        {"table": "prod.s.archive", "version": 2, "snapshot_pinned": True},
        {"table": "prod.s.customers", "version": 5, "snapshot_pinned": True},
    ]
    assert baseline["queries"] == [
        {"query_sha256": query_hash(QUERY), "parameters": {"start": "2026-02-01"}}
    ]
    assert sorted(reader.reads) == [
        ("dev.s.expected", None),
        ("prod.s.archive", 2),
        ("prod.s.customers", None),
        ("prod.s.orders", None),
    ]


def test_parameters_are_bound_values_not_sql_text(frames):
    query = (
        "SELECT o.id, c.name, o.amount FROM {{orders}} o JOIN {{customers}} c "
        "ON o.customer_id = c.id WHERE c.name = :name OR c.name = :attack"
    )
    source = {
        "kind": "sql",
        "query": query,
        "inputs": {"orders": "prod.s.orders", "customers": "prod.s.customers"},
        "parameters": {"name": "O'Brien", "attack": "x' OR '1'='1"},
    }
    result, _ = run(frames, source)
    # Only O'Brien's orders 1 and 2 are selected; the injection string matches nothing.
    assert result.counts["baseline_rows"] == 2
    assert result.counts["missing_rows"] == 1  # order 1 is not in the candidate


def test_unpinned_inputs_are_reported(frames):
    result, _ = run(frames, sql_source(), reader=Reader(frames, pinned=False))
    assert result.inputs["baseline"]["snapshot_pinned"] is False


def test_derived_query_requires_explicit_keys_and_validates_duplicates(frames):
    missing, _ = run(frames, sql_source(), keys=False)
    assert missing.status == "blocked"
    assert "explicit key" in missing.reason
    duplicates = sql_source(
        query="SELECT o.id, c.name, o.amount FROM {{orders}} o JOIN {{customers}} c ON true "
        "WHERE o.day >= :start",
        inputs={"orders": "prod.s.orders", "customers": "prod.s.customers"},
    )
    duplicated, _ = run(frames, duplicates)
    assert duplicated.status == "blocked"
    assert "duplicate" in duplicated.reason


def test_query_analysis_errors_block(frames):
    source = sql_source(
        query="SELECT missing_column AS id FROM {{orders}} WHERE day >= :start",
        inputs={"orders": "prod.s.orders"},
    )
    result, _ = run(frames, source)
    assert result.status in {"blocked", "error"}
    assert "missing_column" in (result.reason or "")


def test_sql_source_composes_inside_union_and_records_nested_query(frames):
    nested = sql_source(
        query="SELECT id, customer_id, day, amount FROM {{orders}} WHERE day < :start",
        inputs={"orders": "prod.s.orders"},
    )
    union = {"kind": "union", "sources": [nested, "prod.s.archive"]}
    result, _ = run(frames, union)
    queries = result.inputs["baseline"]["queries"]
    assert [q["parameters"] for q in queries] == [{"start": "2026-02-01"}]
    assert [leaf["table"] for leaf in result.inputs["baseline"]["leaves"]] == [
        "prod.s.orders",
        "prod.s.archive",
    ]


def test_parallel_pairs_use_isolated_views(spark, frames):
    sessions = [spark.newSession() for _ in range(2)]
    per_session = [
        {name: s.createDataFrame(f.collect(), f.schema) for name, f in frames.items()}
        for s in sessions
    ]
    results = [run(fs, sql_source())[0] for fs in per_session]
    assert [r.status for r in results] == ["passed", "passed"]
