import os

import pytest


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    builder = (
        SparkSession.builder.master("local[4]")
        .appName("pairity-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.scheduler.mode", "FAIR")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )
    if os.environ.get("PAIRITY_TEST_DELTA"):
        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        if jars := os.environ.get("PAIRITY_TEST_DELTA_JARS"):
            builder = builder.config("spark.jars", jars)
        else:
            from delta import configure_spark_with_delta_pip

            builder = configure_spark_with_delta_pip(builder)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
