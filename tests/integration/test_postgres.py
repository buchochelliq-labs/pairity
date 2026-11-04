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
