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
