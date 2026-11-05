import os
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest

from pairity.config_storage import RevisionConflict
from pairity.serialization import load_config


@pytest.fixture(scope="module")
def pg_connect(tmp_path_factory):
    psycopg = pytest.importorskip("psycopg")
    dsn = os.environ.get("PAIRITY_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("Set PAIRITY_TEST_POSTGRES_DSN for real PostgreSQL integration tests")
    yield lambda: psycopg.connect(dsn)


@pytest.fixture
def document_store(pg_connect):
    from pairity.postgres import PostgresDocumentStore

    store = PostgresDocumentStore(pg_connect, schema="test_" + uuid4().hex)
    store.initialize()
    return store


def test_revision_conflict_history_and_namespaces(document_store):
    store = document_store
    first = store.save("settings", "ui", {"theme": "dark"}, expected_revision=0)
    assert first.revision == 1
    store.save("settings", "ui", {"theme": "light"}, expected_revision=1)
    assert store.load("settings", "ui", revision=1).payload == {"theme": "dark"}
    assert store.load("settings", "ui").payload == {"theme": "light"}
    with pytest.raises(RevisionConflict):
        store.save("settings", "ui", {"bad": True}, expected_revision=1)
    assert store.load("settings", "ui").revision == 2
    store.save("schedule", "ui", {"cron": "0 0 * * *"}, expected_revision=0)
    assert store.load("schedule", "ui").revision == 1
    assert store.keys("settings") == ["ui"]
    assert store.keys("suite") == []


def test_simultaneous_first_writers_only_one_wins(document_store):
    gate = Barrier(2)

    def save(value):
        gate.wait(timeout=5)
        try:
            document_store.save("incident", "same", {"value": value}, expected_revision=0)
            return "saved"
        except RevisionConflict:
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(save, [1, 2])) == ["conflict", "saved"]
    assert document_store.load("incident", "same").revision == 1


def test_postgres_config_store_revalidates_and_roundtrips_templates(document_store):
    from pathlib import Path

    from pairity.postgres import PostgresConfigStore

    config = load_config(Path("examples/templates.yaml").read_text(), "yaml")
    store = PostgresConfigStore(document_store, config.suite)
    assert store.save(config, expected_revision=0).revision == 1
    assert store.load().config.fingerprint == config.fingerprint
    config.suite = "wrong"
    with pytest.raises(ValueError):
        store.save(config, expected_revision=1)
    assert store.load().revision == 1


def test_failed_save_does_not_leave_head_or_revision(document_store):
    with pytest.raises(RevisionConflict):
        document_store.save("settings", "missing", {"x": 1}, expected_revision=3)
    with pytest.raises(FileNotFoundError):
        document_store.load("settings", "missing")
    assert document_store.save("settings", "missing", {"x": 2}, expected_revision=0).revision == 1


def test_postgres_preserves_json_numeric_representation(document_store):
    import math

    payload = {"large": 1e20, "small": 1e-20, "negative_zero": -0.0}
    stored = document_store.save("settings", "numeric", payload, expected_revision=0)
    loaded = document_store.load("settings", "numeric")
    assert loaded.sha256 == stored.sha256
    assert loaded.payload == payload
    assert isinstance(loaded.payload["large"], float)
    assert math.copysign(1, loaded.payload["negative_zero"]) == -1


def test_app_access_check_uses_grants_of_the_app_role(pg_connect):
    from psycopg import sql

    from pairity.web.database import DatabaseSetupError
    from pairity.web.lakebase_check import check_grants

    role = "app_check_" + uuid4().hex
    schema = "app_check_" + uuid4().hex
    # Everything in the fixture, including the role and grants, rolls back.
    with pg_connect() as connection:
        database = connection.execute("SELECT current_database()").fetchone()[0]
        connection.execute(sql.SQL("CREATE ROLE {}").format(sql.Identifier(role)))
        with pytest.raises(DatabaseSetupError, match="database CREATE"):
            check_grants(connection, role, schema)
        connection.execute(
            sql.SQL("GRANT CONNECT, CREATE ON DATABASE {} TO {}").format(
                sql.Identifier(database), sql.Identifier(role)
            )
        )
        assert "schema will be created" in check_grants(connection, role, schema)
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        with pytest.raises(DatabaseSetupError, match="schema USAGE"):
            check_grants(connection, role, schema)
        connection.execute(
            sql.SQL("GRANT USAGE, CREATE ON SCHEMA {} TO {}").format(
                sql.Identifier(schema), sql.Identifier(role)
            )
        )
        table = sql.Identifier(schema, "document_heads")
        connection.execute(sql.SQL("CREATE TABLE {} (id integer)").format(table))
        with pytest.raises(DatabaseSetupError, match="document_heads SELECT"):
            check_grants(connection, role, schema)
        connection.execute(
            sql.SQL("GRANT SELECT, INSERT, UPDATE ON TABLE {} TO {}").format(
                table, sql.Identifier(role)
            )
        )
        assert "grants verified" in check_grants(connection, role, schema)
        connection.rollback()


def test_repeated_concurrent_initialization_preserves_documents(pg_connect):
    from pairity.postgres import PostgresDocumentStore

    document_store = PostgresDocumentStore(pg_connect, schema="test_" + uuid4().hex)
    gate = Barrier(2)

    def initialize(_):
        gate.wait(timeout=5)
        document_store.initialize()

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(initialize, range(2)))
    document_store.save("settings", "preserved", {"value": 42}, expected_revision=0)
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda _: document_store.initialize(), range(2)))
    assert document_store.load("settings", "preserved").payload == {"value": 42}
