"""Transactional application documents and suite revisions for Postgres/Lakebase.

The host supplies a fresh authenticated connection per operation. Credentials are
never suite configuration; the same hook can refresh Lakebase OAuth tokens.
"""

import hashlib
import json
from dataclasses import dataclass

from psycopg import sql
from psycopg.types.json import Jsonb

from pairity.config import SuiteConfig
from pairity.config_storage import RevisionConflict, StoredSuite


@dataclass(frozen=True)
class StoredDocument:
    revision: int
    payload: dict
    sha256: str


def _encoded(payload):
    if not isinstance(payload, dict):
        raise ValueError("Application documents must be JSON objects")
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return json.loads(encoded), hashlib.sha256(encoded.encode()).hexdigest()


class PostgresDocumentStore:
    """Namespaced JSONB documents with immutable history and row-locked updates."""

    api_version = 1

    def __init__(self, connect, *, schema="pairity"):
        if not isinstance(schema, str) or not schema or "\x00" in schema:
            raise ValueError("A nonempty Postgres schema name is required")
        self.connect = connect
        self.schema = schema

    def _table(self, name):
        return sql.Identifier(self.schema, name)

    def initialize(self):
        """Explicit provisioning operation; normal reads/writes never perform DDL."""
        with self.connect() as connection, connection.transaction():
            connection.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self.schema))
            )
            connection.execute(
                sql.SQL("""CREATE TABLE IF NOT EXISTS {} (
                version integer PRIMARY KEY,
                installed_at timestamptz NOT NULL DEFAULT now())""").format(
                    self._table("schema_versions")
                )
            )
            versions = connection.execute(
                sql.SQL("SELECT version FROM {}").format(self._table("schema_versions"))
            ).fetchall()
            if versions and versions != [(1,)]:
                raise ValueError("Unsupported Pairity Postgres schema version")
            connection.execute(
                sql.SQL("""CREATE TABLE IF NOT EXISTS {} (
                kind text NOT NULL, document_key text NOT NULL,
                revision bigint NOT NULL CHECK (revision >= 0),
                PRIMARY KEY (kind, document_key))""").format(self._table("document_heads"))
            )
            connection.execute(
                sql.SQL("""CREATE TABLE IF NOT EXISTS {} (
                kind text NOT NULL, document_key text NOT NULL,
                revision bigint NOT NULL CHECK (revision > 0),
                payload jsonb NOT NULL, payload_json text NOT NULL, sha256 text NOT NULL,
                created_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (kind, document_key, revision),
                FOREIGN KEY (kind, document_key) REFERENCES {} (kind, document_key))""").format(
                    self._table("document_revisions"), self._table("document_heads")
                )
            )
            connection.execute(
                sql.SQL("INSERT INTO {} (version) VALUES (1) ON CONFLICT DO NOTHING").format(
                    self._table("schema_versions")
                )
            )

    @staticmethod
    def _key(kind, key):
        if kind not in {"suite", "settings", "schedule", "incident", "artifact"}:
            raise ValueError("Unsupported application document kind")
        if not isinstance(key, str) or not key or "\x00" in key:
            raise ValueError("A nonempty document key is required")

    def load(self, kind, key, *, revision=None):
        self._key(kind, key)
        if revision is not None and (type(revision) is not int or revision < 1):
            raise ValueError("revision must be a positive integer")
        with self.connect() as connection:
            query = sql.SQL("""SELECT revision, payload_json, sha256 FROM {}
                WHERE kind = %s AND document_key = %s""").format(self._table("document_revisions"))
            params = [kind, key]
            if revision is not None:
                query += sql.SQL(" AND revision = %s")
                params.append(revision)
            query += sql.SQL(" ORDER BY revision DESC LIMIT 1")
            row = connection.execute(query, params).fetchone()
        if row is None:
            raise FileNotFoundError("No application document at the requested revision")
        payload, digest = _encoded(json.loads(row[1]))
        if digest != row[2]:
            raise ValueError("Application document checksum mismatch")
        return StoredDocument(row[0], payload, digest)

    def save(self, kind, key, payload, *, expected_revision):
        self._key(kind, key)
        if type(expected_revision) is not int or expected_revision < 0:
            raise ValueError("expected_revision must be a nonnegative integer")
        payload, digest = _encoded(payload)
        with self.connect() as connection, connection.transaction():
            connection.execute(
                sql.SQL("""INSERT INTO {} (kind, document_key, revision)
                VALUES (%s, %s, 0) ON CONFLICT DO NOTHING""").format(self._table("document_heads")),
                (kind, key),
            )
            current = connection.execute(
                sql.SQL("""SELECT revision FROM {}
                WHERE kind = %s AND document_key = %s FOR UPDATE""").format(
                    self._table("document_heads")
                ),
                (kind, key),
            ).fetchone()[0]
            if current != expected_revision:
                raise RevisionConflict(f"Expected revision {expected_revision}, found {current}")
            revision = current + 1
            connection.execute(
                sql.SQL("""INSERT INTO {}
                (kind, document_key, revision, payload, payload_json, sha256)
                VALUES (%s, %s, %s, %s, %s, %s)""").format(self._table("document_revisions")),
                (
                    kind,
                    key,
                    revision,
                    Jsonb(payload),
                    json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False),
                    digest,
                ),
            )
            connection.execute(
                sql.SQL("""UPDATE {} SET revision = %s
                WHERE kind = %s AND document_key = %s""").format(self._table("document_heads")),
                (revision, kind, key),
            )
        return StoredDocument(revision, payload, digest)


class PostgresConfigStore:
    api_version = 1

    def __init__(self, documents, suite):
        self.documents = documents
        self.suite = suite

    def load(self, revision=None):
        stored = self.documents.load("suite", self.suite, revision=revision)
        config = SuiteConfig.model_validate(stored.payload)
        if config.suite != self.suite:
            raise ValueError("Stored suite identity mismatch")
        return StoredSuite(stored.revision, config)

    def save(self, config, *, expected_revision):
        config = SuiteConfig.model_validate(config.model_dump(exclude_none=True))
        if config.suite != self.suite:
            raise ValueError("This store belongs to a different suite")
        stored = self.documents.save(
            "suite",
            self.suite,
            config.model_dump(exclude_none=True),
            expected_revision=expected_revision,
        )
        return StoredSuite(stored.revision, config)
