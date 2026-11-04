"""Thin notebook entry points; task values carry references, never row data."""

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from pairity.artifacts import ArtifactBundle, VolumeArtifactStore, VolumeExecutionRegistry
from pairity.config import SuiteConfig
from pairity.serialization import load_config
from pairity.serverless import (
    aggregate,
    child_run_id,
    create_task_engine,
    load_child,
    prepare_config,
)


def _store(spark, dbutils, *, retain=False):
    from pairity.delta_storage import DeltaResultStore

    return DeltaResultStore(spark, dbutils.widgets.get("result_table"), retain_differences=retain)


def _artifacts(dbutils):
    from databricks.sdk import WorkspaceClient

    return VolumeArtifactStore(dbutils.widgets.get("artifact_root"), WorkspaceClient())


def prepare(spark, dbutils):
    from pairity.metadata import quote_table

    path = Path(dbutils.widgets.get("config_path"))
    config = load_config(path.read_text(), path.suffix.lstrip("."))
    expected_hash = dbutils.widgets.get("config_hash")
    if expected_hash and expected_hash != config.fingerprint:
        raise ValueError("Submitted configuration hash mismatch")
    limit = int(dbutils.widgets.get("parallel_pairs"))
    if not 1 <= limit <= 64 or config.execution.max_parallel_pairs < limit:
        raise ValueError("Job concurrency must be 1..64 and not exceed config max_parallel_pairs")
    indexes = list(range(len(config.pairs)))
    if len(json.dumps(indexes).encode()) > 48000:
        raise ValueError("Pair list exceeds Jobs task-value limit; split the suite")
    run_id = "job:" + dbutils.widgets.get("job_run_id")
    artifacts = _artifacts(dbutils)

    def publish():
        frozen = prepare_config(
            config,
            lambda table: int(
                spark.sql(f"DESCRIBE HISTORY {quote_table(table)} LIMIT 1").first()["version"]
            ),
        )
        manifest = {
            "config": frozen.model_dump(exclude_none=True),
            "run_id": run_id,
            "submitted_config_hash": config.fingerprint,
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        return artifacts.publish(run_id, {"execution.json": json.dumps(manifest).encode()})

    ref = VolumeExecutionRegistry(artifacts).get_or_create(run_id, publish)
    saved = json.loads(artifacts.download(ref)["execution.json"])
    if saved["submitted_config_hash"] != config.fingerprint:
        raise ValueError("Job run already prepared with a different configuration; start a new run")
    dbutils.jobs.taskValues.set(key="manifest", value=asdict(ref))
    dbutils.jobs.taskValues.set(key="pairs", value=indexes)


def _manifest(dbutils):
    ref = ArtifactBundle(**json.loads(dbutils.widgets.get("manifest")))
    data = json.loads(_artifacts(dbutils).download(ref)["execution.json"])
    config = SuiteConfig.model_validate(data["config"])
    if data["run_id"] != "job:" + dbutils.widgets.get("job_run_id"):
        raise ValueError("Manifest belongs to another Job run")
    return data, config


def compare(spark, dbutils):
    data, config = _manifest(dbutils)
    index = int(dbutils.widgets.get("pair_index"))
    if not 0 <= index < len(config.pairs):
        raise ValueError("Unknown pair index")
    retain = dbutils.widgets.get("retain_differences")
    if retain not in {"true", "false"}:
        raise ValueError("retain_differences must be true or false")
    store = _store(spark, dbutils, retain=retain == "true")
    try:
        outcome = load_child(
            store, config, data["run_id"], index, retain_differences=retain == "true"
        )
    except FileNotFoundError:
        result = create_task_engine(spark, sink=store).run(
            config, pair_ids=[config.pairs[index].id], run_id=child_run_id(data["run_id"], index)
        )
        outcome = result.pairs[0]
    if outcome.status not in {"passed", "skipped"}:
        raise RuntimeError(f"Pair {outcome.pair_id}: {outcome.status}")


def finish(spark, dbutils):
    data, config = _manifest(dbutils)
    store = _store(spark, dbutils)
    result = aggregate(config, data["run_id"], data["started_at"], store)
    try:
        existing = store.load(result.run_id)["report"]
    except FileNotFoundError:
        store.write(result)
    else:
        candidate = result.to_dict()
        candidate["finished_at"] = existing["finished_at"]
        if candidate != existing:
            raise ValueError("Completed suite evidence is immutable; start a new Job run")
        result.finished_at = existing["finished_at"]
    print(json.dumps(result.to_dict(), sort_keys=True))
    if result.status != "passed":
        raise RuntimeError(f"Suite {result.suite}: {result.status}")
