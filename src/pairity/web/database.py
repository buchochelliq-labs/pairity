"""Connection factories for App-owned state; never uses the caller's data token."""

import os
import re


def connection_factory(*, environ=None, client=None, connect=None):
    env = os.environ if environ is None else environ
    if connect is None:
        from psycopg import connect
    mode = env.get("PAIRITY_DATABASE_AUTH", "dsn")
    if mode == "dsn":
        dsn = env.get("PAIRITY_POSTGRES_DSN")
        if not dsn:
            raise ValueError("PAIRITY_POSTGRES_DSN is required for dsn database authentication")
        return lambda: connect(dsn, connect_timeout=30)
    if mode != "lakebase":
        raise ValueError("Unknown PAIRITY_DATABASE_AUTH mode")
    required = ["PGHOST", "PGDATABASE", "PGUSER", "PAIRITY_LAKEBASE_ENDPOINT"]
    missing = [name for name in required if not env.get(name)]
    if missing:
        raise ValueError("Missing Lakebase App settings: " + ", ".join(missing))
    endpoint = env["PAIRITY_LAKEBASE_ENDPOINT"]
    if not re.fullmatch(r"projects/[^/]+/branches/[^/]+/endpoints/[^/]+", endpoint):
        raise ValueError("PAIRITY_LAKEBASE_ENDPOINT must identify a project/branch/endpoint")
    if env.get("PGSSLMODE", "require") not in {"require", "verify-ca", "verify-full"}:
        raise ValueError("Lakebase requires TLS")
    if env.get("DATABRICKS_CLIENT_ID") and env["PGUSER"] != env["DATABRICKS_CLIENT_ID"]:
        raise ValueError("Lakebase PGUSER must match the App service principal")
    params = dict(
        host=env["PGHOST"],
        dbname=env["PGDATABASE"],
        user=env["PGUSER"],
        port=int(env.get("PGPORT", "5432")),
        sslmode=env.get("PGSSLMODE", "require"),
        connect_timeout=30,
        application_name=env.get("PGAPPNAME", "pairity"),
    )
    if client is None:
        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient()

    def open_connection():
        # Generate per connection: no persisted password or expired token reuse.
        credential = client.postgres.generate_database_credential(endpoint=endpoint)
        if not credential.token:
            raise ValueError("Lakebase returned no database credential")
        return connect(**params, password=credential.token)

    return open_connection


def application_documents(*, client=None):
    from pairity.postgres import PostgresDocumentStore

    return PostgresDocumentStore(
        connection_factory(client=client),
        schema=os.environ.get("PAIRITY_POSTGRES_SCHEMA", "pairity"),
    )
