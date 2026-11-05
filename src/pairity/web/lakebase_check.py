"""Read-only deployment check of the App binding and PostgreSQL grants."""

import json
import os
import re
import sys
from pathlib import Path

from pairity.web.database import DatabaseSetupError, database_failure


def resolve_endpoint_name(summary, endpoint_name):
    """Resolve this bundle's project output using its deployed resource ID."""
    # CLI 1.23 summary loads resource IDs, but preserves ${resources.*} in
    # config.env. For a postgres_project, the deployed ID is its full name.
    reference = "${resources.postgres_projects.state.name}"
    if isinstance(endpoint_name, str) and endpoint_name.startswith(reference):
        project = summary.get("resources", {}).get("postgres_projects", {}).get("state", {})
        project_name = project.get("id")
        if not isinstance(project_name, str) or not re.fullmatch(
            r"projects/[a-zA-Z0-9_-]+", project_name
        ):
            raise DatabaseSetupError("Bundle summary has no valid deployed Lakebase project ID")
        endpoint_name = project_name + endpoint_name[len(reference) :]
    if not isinstance(endpoint_name, str) or not re.fullmatch(
        r"projects/[a-zA-Z0-9_-]+/branches/[a-zA-Z0-9_-]+/endpoints/[a-zA-Z0-9_-]+",
        endpoint_name,
    ):
        raise DatabaseSetupError("Configured Lakebase endpoint is unresolved or malformed")
    return endpoint_name


def check_grants(connection, role, schema):
    """Inspect the App role through a separate, read-only deployer connection."""
    if not connection.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,)).fetchone():
        raise DatabaseSetupError("App service principal has no PostgreSQL role")
    for privilege in ("CONNECT", "CREATE"):
        allowed = connection.execute(
            "SELECT has_database_privilege(%s, current_database(), %s)", (role, privilege)
        ).fetchone()[0]
        if not allowed:
            raise DatabaseSetupError(f"App role lacks database {privilege}")
    namespace = connection.execute(
        "SELECT oid FROM pg_namespace WHERE nspname = %s", (schema,)
    ).fetchone()
    if namespace is None:
        return "Database CONNECT/CREATE verified; Pairity schema will be created at startup"
    for privilege in ("USAGE", "CREATE"):
        allowed = connection.execute(
            "SELECT has_schema_privilege(%s, %s::oid, %s)", (role, namespace[0], privilege)
        ).fetchone()[0]
        if not allowed:
            raise DatabaseSetupError(f"App role lacks Pairity schema {privilege}")
    for table, privileges in (
        ("schema_versions", ("SELECT", "INSERT")),
        ("document_heads", ("SELECT", "INSERT", "UPDATE")),
        ("document_revisions", ("SELECT", "INSERT")),
    ):
        relation = connection.execute(
            "SELECT oid FROM pg_class WHERE relnamespace = %s::oid AND relname = %s",
            (namespace[0], table),
        ).fetchone()
        if relation is None:
            continue
        for privilege in privileges:
            allowed = connection.execute(
                "SELECT has_table_privilege(%s, %s::oid, %s)", (role, relation[0], privilege)
            ).fetchone()[0]
            if not allowed:
                raise DatabaseSetupError(f"App role lacks {table} {privilege}")
    return "App role database, schema and existing document-table grants verified"


def check_resources(client, connect, *, app_name, endpoint_name, schema, deployer):
    app = client.apps.get(app_name)
    if not app.service_principal_client_id:
        raise DatabaseSetupError("App has no service principal client ID")
    resources = [r.postgres for r in (app.resources or []) if r.name == "postgres" and r.postgres]
    if len(resources) != 1:
        raise DatabaseSetupError("App must have one Lakebase resource named postgres")
    resource = resources[0]
    if getattr(resource.permission, "value", None) != "CAN_CONNECT_AND_CREATE":
        raise DatabaseSetupError("App Lakebase resource is missing CAN_CONNECT_AND_CREATE")
    if endpoint_name.rsplit("/endpoints/", 1)[0] != resource.branch:
        raise DatabaseSetupError("App endpoint and database resource refer to different branches")
    if not resource.database or not resource.database.startswith(resource.branch + "/databases/"):
        raise DatabaseSetupError("App database resource does not belong to its branch")
    endpoint = client.postgres.get_endpoint(endpoint_name)
    database = client.postgres.get_database(resource.database)
    if not endpoint.status or not endpoint.status.hosts or not endpoint.status.hosts.host:
        raise DatabaseSetupError("Lakebase endpoint has no connection host")
    if not database.status or not database.status.postgres_database:
        raise DatabaseSetupError("Lakebase database has no PostgreSQL database name")
    credential = client.postgres.generate_database_credential(endpoint=endpoint_name)
    if not credential.token:
        raise DatabaseSetupError("Lakebase returned no deployer diagnostic credential")
    with connect(
        host=endpoint.status.hosts.host,
        dbname=database.status.postgres_database,
        user=deployer,
        password=credential.token,
        sslmode="require",
        connect_timeout=30,
        options="-c default_transaction_read_only=on -c statement_timeout=10000",
    ) as connection:
        result = check_grants(connection, app.service_principal_client_id, schema)
    return result


def main():
    from databricks.sdk import WorkspaceClient
    from psycopg import connect

    try:
        summary = json.loads(Path(sys.argv[1]).read_text())
        app = summary["resources"]["apps"]["workbench"]
        env = {item["name"]: item.get("value") for item in app["config"]["env"]}
        endpoint_name = resolve_endpoint_name(summary, env.get("PAIRITY_LAKEBASE_ENDPOINT"))
        result = check_resources(
            WorkspaceClient(),
            connect,
            app_name=app["name"],
            endpoint_name=endpoint_name,
            schema=env["PAIRITY_POSTGRES_SCHEMA"],
            deployer=os.environ["DATABRICKS_CLIENT_ID"],
        )
    except Exception as exc:
        raise SystemExit(str(database_failure("Lakebase deployment access check", exc))) from None
    print(result)
    print("Check used the deployer to inspect App grants; App OAuth login is verified at startup.")


if __name__ == "__main__":
    main()
