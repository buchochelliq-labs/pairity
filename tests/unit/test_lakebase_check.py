from types import SimpleNamespace as NS

import pytest
from databricks.sdk.service.apps import (
    App,
    AppResource,
    AppResourcePostgres,
    AppResourcePostgresPostgresPermission,
)

from pairity.web.database import DatabaseSetupError
from pairity.web.lakebase_check import check_resources


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
