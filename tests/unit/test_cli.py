import json
import os
import subprocess
import sys

import pytest

from pairity.cli import main
from pairity.config_storage import FileConfigStore


def write_config(tmp_path):
    path = tmp_path / "suite.yaml"
    path.write_text("""schema_version: 1
suite: release
execution:
  max_parallel_pairs: 3
pairs:
  - id: orders
    baseline: prod.s.orders
    candidate: dev.s.orders
    keys:
      - baseline: id
        candidate: id
    tags: [sales]
""")
    return path


def test_validate_and_convert_use_same_config_without_spark(tmp_path, capsys):
    source = write_config(tmp_path)
    assert main(["--json", "validate", str(source)]) == 0
    assert json.loads(capsys.readouterr().out)["valid"] is True
    output = tmp_path / "suite.json"
    assert main(["convert", str(source), "--format", "json", "--output", str(output)]) == 0
    assert json.loads(output.read_text())["execution"]["max_parallel_pairs"] == 3


def test_import_defaults_to_preview_then_apply_requires_revision(tmp_path, capsys):
    source = write_config(tmp_path)
    root = tmp_path / "configs"
    command = ["--json", "import", str(source), "--store", str(root), "--expected-revision", "0"]
    assert main(command) == 0
    assert not root.exists()
    assert json.loads(capsys.readouterr().out)["preview"]["added"] == ["orders"]
    assert main(command + ["--apply"]) == 0
    assert FileConfigStore(root).load().revision == 1
    assert main(command + ["--apply"]) == 2
    assert FileConfigStore(root).load().revision == 1


def test_patch_and_export_historical_revision(tmp_path, capsys):
    source = write_config(tmp_path)
    root = tmp_path / "configs"
    assert (
        main(["import", str(source), "--store", str(root), "--expected-revision", "0", "--apply"])
        == 0
    )
    patch = tmp_path / "patch.json"
    patch.write_text(json.dumps({"enabled": False}))
    assert (
        main(
            [
                "patch",
                str(patch),
                "--store",
                str(root),
                "--expected-revision",
                "1",
                "--tag",
                "sales",
                "--apply",
            ]
        )
        == 0
    )
    assert FileConfigStore(root).load().config.pairs[0].enabled is False
    output = tmp_path / "old.json"
    assert (
        main(
            [
                "export",
                "--store",
                str(root),
                "--revision",
                "1",
                "--format",
                "json",
                "--output",
                str(output),
            ]
        )
        == 0
    )
    assert json.loads(output.read_text())["pairs"][0]["enabled"] is True


def test_invalid_config_returns_nonzero_without_traceback(tmp_path, capsys):
    source = tmp_path / "broken.yaml"
    source.write_text("schema_version: 999")
    assert main(["validate", str(source)]) == 2
    assert "Traceback" not in capsys.readouterr().err


def test_configuration_cli_import_does_not_load_optional_runtime():
    env = os.environ | {"PYTHONPATH": "src"}
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import pairity.cli, sys; "
            "assert 'pyspark' not in sys.modules; assert 'databricks.sdk' not in sys.modules",
        ],
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, result.stderr


def test_databricks_job_entrypoint_raises_when_validation_fails(tmp_path):
    from pairity.cli import job_main

    path = tmp_path / "invalid.yaml"
    path.write_text("schema_version: 99")
    with pytest.raises(RuntimeError, match="exit code 2"):
        job_main(["validate", str(path)])


def test_default_cli_uses_human_rs_rich_report(tmp_path, capsys):
    assert main(["validate", str(write_config(tmp_path))]) == 0
    output = capsys.readouterr().out
    assert "Pairity" in output
    assert "release" in output
    assert not output.lstrip().startswith("{")


def test_import_new_template_into_existing_suite_and_preview_template_change(tmp_path, capsys):
    source = write_config(tmp_path)
    root = tmp_path / "configs"
    assert (
        main(
            [
                "--json",
                "import",
                str(source),
                "--store",
                str(root),
                "--expected-revision",
                "0",
                "--apply",
            ]
        )
        == 0
    )
    capsys.readouterr()
    config = FileConfigStore(root).load().config.model_dump(exclude_none=True)
    config["templates"] = {"orders": {"source": "prod.s.orders"}}
    config["pairs"][0]["baseline"] = {"kind": "template", "name": "orders"}
    path = tmp_path / "new.json"
    path.write_text(json.dumps(config))
    command = ["--json", "import", str(path), "--store", str(root), "--expected-revision", "1"]
    assert main(command + ["--apply"]) == 0
    capsys.readouterr()
    config["templates"]["orders"]["source"] = "prod.s.new_orders"
    path.write_text(json.dumps(config))
    assert main(command[:-1] + ["2"]) == 0
    assert json.loads(capsys.readouterr().out)["suite_settings_changed"] is True


def test_incident_download_and_volume_publish_use_nested_archive(tmp_path, monkeypatch, capsys):
    import databricks.sdk
    from test_incidents import report

    from pairity.artifacts import LocalArtifactStore

    local = LocalArtifactStore(tmp_path / "volume")
    monkeypatch.setattr(databricks.sdk, "WorkspaceClient", lambda: object())
    monkeypatch.setattr("pairity.artifacts.VolumeArtifactStore", lambda root, client: local)
    source = tmp_path / "report.json"
    source.write_text(json.dumps(report()))
    output = tmp_path / "incident.zip"
    assert (
        main(
            [
                "--json",
                "incident",
                str(source),
                "--output",
                str(output),
                "--artifact-root",
                "/Volumes/c/s/v",
            ]
        )
        == 0
    )
    result = json.loads(capsys.readouterr().out)
    from pairity.artifacts import ArtifactBundle

    files = local.download(ArtifactBundle(**result["artifact"]))
    assert files == {"incident.zip": output.read_bytes()}


@pytest.mark.parametrize("apply", [False, True])
def test_results_prune_previews_unless_apply(monkeypatch, capsys, apply):
    pytest.importorskip("pyspark")
    import pyspark.sql

    import pairity.delta_storage
    from pairity.cli import main

    calls = []

    class Store:
        def __init__(self, spark, table):
            calls.append(("init", table))

        def retention_plan(self, **kwargs):
            calls.append(("plan", kwargs))
            return {"table": "c.s.t", "runs": [{"run_id": "old"}], "truncated": False}

        def apply_retention(self, plan):
            calls.append(("apply", plan["runs"]))
            return {"deleted": ["old"], "kept": []}

    class Builder:
        @staticmethod
        def getOrCreate():
            return "spark"

    monkeypatch.setattr(pairity.delta_storage, "DeltaResultStore", Store)
    monkeypatch.setattr(pyspark.sql.SparkSession, "builder", Builder)
    argv = ["--json", "results-prune", "--result-table", "c.s.t", "--older-than-days", "90"]
    assert main(argv + (["--apply"] if apply else [])) == 0
    output = json.loads(capsys.readouterr().out)
    assert calls[1] == (
        "plan",
        {"older_than_days": 90, "keep_latest_per_suite": 1, "suite": None, "max_runs": 1000},
    )
    assert (("apply", [{"run_id": "old"}]) in calls) is apply
    assert output.get("deleted") == (["old"] if apply else None)
    assert output.get("preview") is (None if apply else True)
