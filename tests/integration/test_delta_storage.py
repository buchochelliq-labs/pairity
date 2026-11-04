import os
from uuid import uuid4

import pytest

from pairity.config import SuiteConfig
from pairity.engine import ComparisonRunner, PairityEngine
from pairity.results import TableInput
from pairity.spark import SparkComparisonBackend

pytestmark = [
    pytest.mark.spark,
    pytest.mark.skipif(
        not os.environ.get("PAIRITY_TEST_DELTA"),
        reason="Set PAIRITY_TEST_DELTA=1 for Delta integration",
    ),
]


def result_fixture(spark):
    from contextlib import nullcontext

    class Reader:
        api_version = 1

        def scope(self, *args):
            return nullcontext()

        def read(self, table, version=None):
            value = "before" if table.startswith("prod") else "after"
            frame = spark.createDataFrame([(1, value)], "id long, value string")
            return TableInput(frame, table, version=4, snapshot_pinned=True)

    class Metadata:
        api_version = 1

        def primary_key(self, table):
            return ["id"]

    config = SuiteConfig.model_validate(
        {
            "schema_version": 1,
            "suite": "stored",
            "pairs": [{"id": "p", "baseline": "prod.s.t", "candidate": "dev.s.t"}],
        }
    )
    return PairityEngine(
        lambda: ComparisonRunner(Reader(), Metadata(), SparkComparisonBackend())
    ).run(config)


def test_delta_roundtrip_idempotent_summary_and_distributed_differences(spark):
    from pairity.delta_storage import DeltaResultStore

    table = "spark_catalog.default.results_" + uuid4().hex
    store = DeltaResultStore(spark, table, retain_differences=True)
    store.initialize()
    result = result_fixture(spark)
    store.write(result)
    store.write(result)
    assert store.load(result.run_id)["report"]["pairs"][0]["counts"]["changed_rows"] == 1
    assert store.differences(result.run_id, "p").count() == 1
    assert spark.table(table).count() == 2  # one report + one difference, not duplicated
    result.finished_at = "changed"
    with pytest.raises(ValueError, match="different"):
        store.write(result)
    spark.sql(f"DROP TABLE {table}")


def test_delta_summary_only_reports_unretained_differences(spark):
    from pairity.delta_storage import DeltaResultStore

    table = "spark_catalog.default.results_" + uuid4().hex
    store = DeltaResultStore(spark, table)
    store.initialize()
    result = result_fixture(spark)
    store.write(result)
    assert store.load(result.run_id)["differences_retained"] is False
    with pytest.raises(ValueError, match="not retained"):
        store.differences(result.run_id, "p")
    assert spark.table(table).count() == 1
    spark.sql(f"DROP TABLE {table}")


def test_delta_rejects_incomplete_or_changed_retry_evidence(spark):
    from pyspark.sql import functions as F

    from pairity.delta_storage import DeltaResultStore

    table = "spark_catalog.default.results_" + uuid4().hex
    store = DeltaResultStore(spark, table, retain_differences=True)
    store.initialize()
    result = result_fixture(spark)
    original = result.pairs[0].differences
    result.pairs[0].differences = original.limit(0)
    with pytest.raises(ValueError, match="counts"):
        store.write(result)
    with pytest.raises(FileNotFoundError):
        store.load(result.run_id)
    result.pairs[0].differences = original
    store.write(result)
    result.pairs[0].differences = original.withColumn(
        "candidate", F.struct(F.lit(1).alias("id"), F.lit("other").alias("value"))
    )
    with pytest.raises(ValueError, match="evidence"):
        store.write(result)
    spark.sql(f"DROP TABLE {table}")
