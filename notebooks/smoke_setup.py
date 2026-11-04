# Databricks notebook source
import io
import re
import runpy
from pathlib import Path

from databricks.sdk import WorkspaceClient

from pairity.delta_storage import DeltaResultStore
from pairity.serialization import dump_config


def setup(spark, dbutils):
    get = dbutils.widgets.get
    test_id = get("test_id")
    if not re.fullmatch(r"gha_[0-9]+_[0-9]+", test_id):
        raise ValueError("Use a GitHub Actions run/attempt test ID")
    root = Path(get("fixture_root"))
    loader = runpy.run_path(str(root / "load.py"))["load_fixture"]
    configs = loader(spark, get("catalog"), get("schema"), root=root, table_prefix=test_id)
    DeltaResultStore(spark, get("result_table")).initialize()
    directory = get("artifact_root").rstrip("/") + "/smoke/" + test_id
    client = WorkspaceClient()
    client.files.create_directory(directory)
    for name, config in configs.items():
        client.files.upload(
            directory + "/" + name + ".yaml",
            io.BytesIO(dump_config(config, "yaml").encode()),
            overwrite=False,
        )


if __name__ == "__main__":
    setup(spark, dbutils)  # noqa: F821
