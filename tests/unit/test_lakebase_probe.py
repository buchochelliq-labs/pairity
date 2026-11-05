import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


def load_probe():
    spec = importlib.util.spec_from_file_location(
        "lakebase_probe", Path("scripts/probe_lakebase_endpoint.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("accepted", [True, False])
def test_probe_sends_exact_requested_value_without_local_format_validation(accepted, capsys):
    endpoint = "pairity-workbench/production/primary"
    calls = []

    class Rejected(Exception):
        error_code = "INVALID_PARAMETER_VALUE"

    def generate(**kwargs):
        calls.append(kwargs)
        if not accepted:
            raise Rejected("must-not-log-driver-message")
        return SimpleNamespace(token="must-not-log-token")

    client = SimpleNamespace(postgres=SimpleNamespace(generate_database_credential=generate))
    assert load_probe().probe(client, "requested", endpoint) is accepted
    assert calls == [{"endpoint": endpoint}]
    output = capsys.readouterr().out
    report = json.loads(output)
    assert report["endpoint"] == endpoint
    assert report["outcome"] == ("credential_issued" if accepted else "request_failed")
    assert "must-not-log" not in output
    if not accepted:
        assert report["error_code"] == "INVALID_PARAMETER_VALUE"


def test_probe_does_not_report_success_without_a_credential(capsys):
    client = SimpleNamespace(
        postgres=SimpleNamespace(
            generate_database_credential=lambda **kw: SimpleNamespace(token=None)
        )
    )
    assert not load_probe().probe(
        client, "control", "projects/pairity-state/branches/production/endpoints/primary"
    )
    assert json.loads(capsys.readouterr().out)["outcome"] == "no_credential_returned"


def test_failed_requested_value_does_not_skip_the_control(monkeypatch, capsys):
    for name, value in {
        "DATABRICKS_HOST": "https://example.invalid",
        "DATABRICKS_CLIENT_ID": "deployer",
        "DATABRICKS_CLIENT_SECRET": "must-not-log-secret",
        "PAIRITY_PROBE_ENDPOINT": "pairity-workbench/production/primary",
        "PAIRITY_CONTROL_ENDPOINT": "projects/pairity-state/branches/production/endpoints/primary",
    }.items():
        monkeypatch.setenv(name, value)
    calls = []

    def generate(*, endpoint):
        calls.append(endpoint)
        if len(calls) == 1:
            raise ValueError("must-not-log-error-message")
        return SimpleNamespace(token="must-not-log-token")

    client = SimpleNamespace(postgres=SimpleNamespace(generate_database_credential=generate))
    monkeypatch.setattr("databricks.sdk.WorkspaceClient", lambda: client)
    with pytest.raises(SystemExit) as result:
        load_probe().main()
    assert result.value.code == 1
    assert calls == [
        "pairity-workbench/production/primary",
        "projects/pairity-state/branches/production/endpoints/primary",
    ]
    output = capsys.readouterr().out
    assert [json.loads(line)["outcome"] for line in output.splitlines()] == [
        "request_failed",
        "credential_issued",
    ]
    assert "must-not-log" not in output


def test_workflow_profile_requests_only_postgres_scope(monkeypatch):
    import yaml
    from databricks.sdk.config import Config
    from databricks.sdk.credentials_provider import credentials_strategy

    workflow = yaml.safe_load(Path(".github/workflows/databricks-lakebase-probe.yml").read_text())
    step = next(s for s in workflow["jobs"]["probe"]["steps"] if "env" in s)
    config_file = step["env"]["DATABRICKS_CONFIG_FILE"].replace(
        "${{ github.workspace }}", str(Path.cwd())
    )
    monkeypatch.setenv("DATABRICKS_CONFIG_FILE", config_file)
    monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", step["env"]["DATABRICKS_CONFIG_PROFILE"])
    monkeypatch.setattr(Config, "_resolve_host_metadata", lambda self: None)

    @credentials_strategy("test", [])
    def offline_auth(cfg):
        return lambda: {}

    config = Config(host="https://example.invalid", credentials_strategy=offline_auth)
    assert config.get_scopes() == ["postgres"]
    assert config.http_timeout_seconds is not None and 0 < config.http_timeout_seconds <= 30
    assert config.retry_timeout_seconds is not None and 0 < config.retry_timeout_seconds <= 60
