"""Connection factories for App-owned state; never uses the caller's data token."""

import os
import re


class DatabaseSetupError(ValueError):
    """A diagnostic containing only settings names, stages and error codes."""


def database_failure(stage, exc):
    if isinstance(exc, DatabaseSetupError):
        return exc
    code = getattr(exc, "sqlstate", None)
    detail = type(exc).__name__
    if isinstance(code, str) and re.fullmatch(r"[0-9A-Z]{5}", code):
        detail += f", SQLSTATE={code}"
    return DatabaseSetupError(f"{stage} failed ({detail})")


def connection_factory(*, environ=None, client=None, connect=None):
    env = os.environ if environ is None else environ
    if connect is None:
        from psycopg import connect
    mode = env.get("PAIRITY_DATABASE_AUTH", "dsn")
    if mode == "dsn":
        dsn = env.get("PAIRITY_POSTGRES_DSN")
        if not dsn:
            raise DatabaseSetupError("PAIRITY_POSTGRES_DSN is required for dsn authentication")
        return lambda: connect(dsn, connect_timeout=30)
    if mode != "lakebase":
        raise DatabaseSetupError("Unknown PAIRITY_DATABASE_AUTH mode")
    required = ["PGHOST", "PGDATABASE", "PGUSER", "PAIRITY_LAKEBASE_ENDPOINT"]
    missing = [name for name in required if not env.get(name)]
    if missing:
        raise DatabaseSetupError("Missing Lakebase App settings: " + ", ".join(missing))
    endpoint = env["PAIRITY_LAKEBASE_ENDPOINT"]
    if not re.fullmatch(r"projects/[^/]+/branches/[^/]+/endpoints/[^/]+", endpoint):
        raise DatabaseSetupError(
            "PAIRITY_LAKEBASE_ENDPOINT must identify a project/branch/endpoint"
        )
    if env.get("PGSSLMODE", "require") not in {"require", "verify-ca", "verify-full"}:
        raise DatabaseSetupError("Lakebase requires TLS")
    if env.get("DATABRICKS_CLIENT_ID") and env["PGUSER"] != env["DATABRICKS_CLIENT_ID"]:
        raise DatabaseSetupError("Lakebase PGUSER must match the App service principal")
    try:
        port = int(env.get("PGPORT", "5432"))
        if not 1 <= port <= 65535:
            raise ValueError
    except ValueError:
        raise DatabaseSetupError("PGPORT must be a valid port number") from None
    params = dict(
        host=env["PGHOST"],
        dbname=env["PGDATABASE"],
        user=env["PGUSER"],
        port=port,
        sslmode=env.get("PGSSLMODE", "require"),
        connect_timeout=30,
        application_name=env.get("PGAPPNAME", "pairity"),
    )
    if client is None:
        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient()

    def open_connection():
        # Generate per connection: no persisted password or expired token reuse.
        try:
            credential = client.postgres.generate_database_credential(endpoint=endpoint)
        except Exception as exc:
            raise database_failure("credential generation", exc) from None
        if not credential.token:
            raise DatabaseSetupError("Lakebase returned no database credential")
        try:
            return connect(**params, password=credential.token)
        except Exception as exc:
            raise database_failure("database connection", exc) from None

    return open_connection


def application_documents(*, client=None):
    from pairity.postgres import PostgresDocumentStore

    return PostgresDocumentStore(
        connection_factory(client=client),
        schema=os.environ.get("PAIRITY_POSTGRES_SCHEMA", "pairity"),
    )
