import json
from types import SimpleNamespace

import pytest

from pairity.web.database import connection_factory


def environment():
    return {
        "PAIRITY_DATABASE_AUTH": "lakebase",
        "PAIRITY_LAKEBASE_ENDPOINT": "projects/pairity-state/branches/production/endpoints/primary",
        "PGHOST": "db.example.invalid",
        "PGDATABASE": "databricks_postgres",
        "PGUSER": "app-id",
        "DATABRICKS_CLIENT_ID": "app-id",
        "PGPORT": "5432",
    }


def test_lakebase_refreshes_credentials_for_every_connection():
    tokens = iter(["first-secret", "second-secret"])
    endpoints, connections = [], []

    def generate(**kwargs):
        endpoints.append(kwargs)
        return SimpleNamespace(token=next(tokens))

    def connect(**kwargs):
        connections.append(kwargs)
        return object()

    factory = connection_factory(
        environ=environment(),
        connect=connect,
        client=SimpleNamespace(postgres=SimpleNamespace(generate_database_credential=generate)),
    )
    factory()
    factory()
    assert [c["password"] for c in connections] == ["first-secret", "second-secret"]
    assert all(c["sslmode"] == "require" and c["connect_timeout"] == 30 for c in connections)
    assert endpoints == [{"endpoint": environment()["PAIRITY_LAKEBASE_ENDPOINT"]}] * 2


@pytest.mark.parametrize(
    "change",
    [
        {"PGHOST": ""},
        {"PGUSER": "other"},
        {"PGSSLMODE": "disable"},
        {"PAIRITY_LAKEBASE_ENDPOINT": "not-an-endpoint"},
        {"PAIRITY_DATABASE_AUTH": "unknown"},
    ],
)
def test_invalid_lakebase_settings_fail_before_connect(change):
    with pytest.raises(ValueError):
        connection_factory(environ=environment() | change, client=object(), connect=object())


def test_missing_token_does_not_connect_or_fall_back_to_dsn():
    def connect(*args, **kwargs):
        pytest.fail("Must not connect with an empty credential")

    factory = connection_factory(
        environ=environment() | {"PAIRITY_POSTGRES_DSN": "ignored-secret"},
        connect=connect,
        client=SimpleNamespace(
            postgres=SimpleNamespace(
                generate_database_credential=lambda **kw: SimpleNamespace(token=None)
            )
        ),
    )
    with pytest.raises(ValueError, match="no database credential"):
        factory()


def test_legacy_dsn_factory_remains_explicit():
    calls = []
    factory = connection_factory(
        environ={"PAIRITY_POSTGRES_DSN": "postgresql://configured"},
        connect=lambda *args, **kw: calls.append((args, kw)),
    )
    factory()
    assert calls == [(("postgresql://configured",), {"connect_timeout": 30})]


@pytest.mark.parametrize("stage", ["credential generation", "database connection"])
def test_lakebase_failure_identifies_stage_without_exposing_credentials(stage):
    class Denied(RuntimeError):
        sqlstate = "42501"

    def fail(**kwargs):
        raise Denied("password=must-not-be-logged")

    factory = connection_factory(
        environ=environment(),
        connect=fail if stage == "database connection" else object(),
        client=SimpleNamespace(
            postgres=SimpleNamespace(
                generate_database_credential=(
                    fail
                    if stage == "credential generation"
                    else lambda **kw: SimpleNamespace(token="must-not-be-logged")
                )
            )
        ),
    )
    with pytest.raises(Exception) as exc:
        factory()
    assert stage in str(exc.value)
    assert "SQLSTATE=42501" in str(exc.value)
    assert "must-not-be-logged" not in str(exc.value)


def test_bootstrap_initializes_before_serving_and_fails_closed(monkeypatch, capsys):
    from pairity.web.bootstrap import main

    events = []
    monkeypatch.setattr(
        "pairity.web.database.application_documents",
        lambda: SimpleNamespace(initialize=lambda: events.append("initialize")),
    )
    monkeypatch.setattr("pairity.web.app.main", lambda: events.append("serve"))
    main()
    assert events == ["initialize", "serve"]

    def failed():
        raise RuntimeError("must-not-expose-password")

    monkeypatch.setattr(
        "pairity.web.database.application_documents", lambda: SimpleNamespace(initialize=failed)
    )
    events.clear()
    with pytest.raises(SystemExit) as exc:
        main()
    assert not events
    assert "must-not-expose-password" not in str(exc.value) + capsys.readouterr().out


@pytest.mark.parametrize(
    "endpoint",
    [
        None,
        "${resources.postgres_projects.state.name}/branches/production/endpoints/primary",
        "bad\nendpoint",
    ],
)
def test_bootstrap_logs_received_endpoint_before_configuration_failure(
    monkeypatch, capsys, endpoint
):
    from pairity.web.bootstrap import main

    for name, value in environment().items():
        monkeypatch.setenv(name, value)
    if endpoint is None:
        monkeypatch.delenv("PAIRITY_LAKEBASE_ENDPOINT")
    else:
        monkeypatch.setenv("PAIRITY_LAKEBASE_ENDPOINT", endpoint)
    monkeypatch.setenv("PAIRITY_POSTGRES_DSN", "must-not-expose-dsn")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "must-not-expose-secret")
    monkeypatch.setattr(
        "pairity.web.app.main", lambda: pytest.fail("Invalid database configuration must not serve")
    )
    with pytest.raises(SystemExit, match="Pairity database initialization failed"):
        main()

    lines = capsys.readouterr().out.splitlines()
    assert lines == [f"Pairity Lakebase endpoint (app startup): {json.dumps(endpoint)}"]


def test_bundle_provisions_state_and_binds_database_without_password():
    import configparser
    from pathlib import Path

    import yaml

    bundle = yaml.safe_load(Path("app/databricks.yml").read_text())
    project = bundle["resources"]["postgres_projects"]["state"]
    assert project["lifecycle"]["prevent_destroy"] is True
    assert project["default_endpoint_settings"]["suspend_timeout_duration"] == "300s"
    app = bundle["resources"]["apps"]["workbench"]
    database = next(r["postgres"] for r in app["resources"] if "postgres" in r)
    assert database["permission"] == "CAN_CONNECT_AND_CREATE"
    assert not any("secret" in r for r in app["resources"])
    assert "bootstrap" in app["config"]["command"][-1]
    profiles = configparser.ConfigParser()
    profiles.read(".github/databrickscfg")
    assert "postgres" in profiles["pairity-app-ci"]["scopes"].split(",")
    assert "postgres" not in profiles["pairity-ci"]["scopes"].split(",")
