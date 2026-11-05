import json
from types import SimpleNamespace as NS

import pytest
from databricks.sdk.service.apps import (
    App,
    AppResource,
    AppResourcePostgres,
    AppResourcePostgresPostgresPermission,
)

from pairity.web.database import DatabaseSetupError
from pairity.web.lakebase_check import check_resources, main, resolve_endpoint_name


def resources():
    branch = "projects/pairity/branches/production"
    resource = AppResourcePostgres(
        branch=branch,
        database=branch + "/databases/databricks-postgres",
        permission=AppResourcePostgresPostgresPermission.CAN_CONNECT_AND_CREATE,
    )
    app = App(
        name="pairity-workbench",
        service_principal_client_id="app-client-id",
        resources=[AppResource(name="postgres", postgres=resource)],
    )
    client = NS(
        apps=NS(get=lambda name: app),
        postgres=NS(
            get_endpoint=lambda name: NS(status=NS(hosts=NS(host="database.invalid"))),
            get_database=lambda name: NS(status=NS(postgres_database="databricks_postgres")),
            generate_database_credential=lambda **kw: NS(token="diagnostic-secret"),
        ),
    )
    args = dict(
        app_name=app.name,
        endpoint_name=branch + "/endpoints/primary",
        schema="pairity",
        deployer="deployer-client-id",
    )
    return client, resource, args


@pytest.mark.parametrize("fault", ["permission", "branch", "database"])
def test_invalid_resource_binding_stops_before_database_connection(fault):
    client, resource, args = resources()
    if fault == "permission":
        resource.permission = None
    elif fault == "branch":
        resource.branch = "projects/other/branches/production"
    else:
        resource.database = "projects/other/branches/production/databases/other"
    with pytest.raises(DatabaseSetupError):
        check_resources(client, lambda **kw: pytest.fail("Must not connect"), **args)


def test_deployer_connection_checks_the_app_role_and_is_read_only():
    client, _, args = resources()
    statements, connection_options = [], []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, sql, params):
            statements.append((sql, params))
            row = None if "pg_namespace" in sql else (True,)
            return NS(fetchone=lambda: row)

    def connect(**kwargs):
        connection_options.append(kwargs)
        return Connection()

    assert "schema will be created" in check_resources(client, connect, **args)
    assert connection_options[0]["user"] == "deployer-client-id"
    assert "default_transaction_read_only=on" in connection_options[0]["options"]
    assert connection_options[0]["sslmode"] == "require"
    assert statements[0][1] == ("app-client-id",)
    assert all(params[0] == "app-client-id" for sql, params in statements if "privilege" in sql)
    assert all(sql.startswith("SELECT") for sql, _ in statements)


@pytest.mark.parametrize(
    "endpoint",
    [
        "${resources.postgres_projects.state.name}/branches/production/endpoints/primary",
        "projects/pairity/branches/production/endpoints/primary",
    ],
)
def test_summary_check_resolves_project_output_before_checking_app_branch(
    tmp_path, monkeypatch, capsys, endpoint
):
    # CLI 1.23.0 summary populates the deployed project's id but leaves the
    # ${resources.*.name} reference in App.config.env unresolved.
    summary = {
        "resources": {
            "postgres_projects": {"state": {"id": "projects/pairity", "project_id": "pairity"}},
            "apps": {
                "workbench": {
                    "name": "pairity-workbench",
                    "config": {
                        "env": [
                            {"name": "PAIRITY_LAKEBASE_ENDPOINT", "value": endpoint},
                            {"name": "PAIRITY_POSTGRES_SCHEMA", "value": "pairity"},
                        ]
                    },
                }
            },
        }
    }
    path = tmp_path / "summary.json"
    path.write_text(json.dumps(summary))
    client, _, _ = resources()
    endpoints, queries = [], []

    def get_endpoint(name):
        endpoints.append(name)
        return NS(status=NS(hosts=NS(host="database.invalid")))

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, sql, params):
            queries.append((sql, params))
            return NS(fetchone=lambda: None if "pg_namespace" in sql else (True,))

    client.postgres.get_endpoint = get_endpoint
    monkeypatch.setattr("databricks.sdk.WorkspaceClient", lambda: client)
    monkeypatch.setattr("psycopg.connect", lambda **kw: Connection())
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "deployer-client-id")
    monkeypatch.setattr("sys.argv", ["lakebase_check", str(path)])

    main()

    assert endpoints == ["projects/pairity/branches/production/endpoints/primary"]
    assert queries[0][1] == ("app-client-id",)
    assert "CONNECT/CREATE verified" in capsys.readouterr().out


@pytest.mark.parametrize("project_id", [None, "", "pairity", "${var.project}"])
def test_project_reference_requires_a_deployed_project_id(project_id):
    summary = {"resources": {"postgres_projects": {"state": {"id": project_id}}}}
    with pytest.raises(DatabaseSetupError, match="deployed Lakebase project ID"):
        resolve_endpoint_name(
            summary,
            "${resources.postgres_projects.state.name}/branches/production/endpoints/primary",
        )


@pytest.mark.parametrize(
    "endpoint",
    [
        None,
        "",
        "${resources.postgres_projects.other.name}/branches/production/endpoints/primary",
        "projects/pairity/branches/production",
        "projects/pairity/branches/production/endpoints/",
    ],
)
def test_unresolved_or_malformed_endpoint_has_a_configuration_error(endpoint):
    with pytest.raises(DatabaseSetupError, match="endpoint is unresolved or malformed"):
        resolve_endpoint_name({}, endpoint)


def test_resolving_project_reference_does_not_hide_a_real_branch_mismatch():
    client, _, args = resources()
    args["endpoint_name"] = resolve_endpoint_name(
        {"resources": {"postgres_projects": {"state": {"id": "projects/pairity"}}}},
        "${resources.postgres_projects.state.name}/branches/other/endpoints/primary",
    )
    with pytest.raises(DatabaseSetupError, match="different branches"):
        check_resources(client, lambda **kw: pytest.fail("Must not connect"), **args)
