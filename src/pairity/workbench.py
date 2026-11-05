"""Shared application facade: configuration, submission and historical evidence.

No Spark, HTTP framework, credential or presentation dependency belongs here.
"""

import hashlib
from dataclasses import asdict
from pathlib import Path

from pairity.bulk import patch_pairs, preview_update, select_pairs
from pairity.config import SuiteConfig, resolve_settings
from pairity.config_storage import FileConfigStore, RevisionConflict
from pairity.contracts import CatalogDiscovery
from pairity.metadata import MissingKeys, resolve_keys
from pairity.serialization import load_config


class FileSuiteCatalog:
    """Explicit local-development catalog; never a hosted concurrent store."""

    def __init__(self, root):
        self.root = Path(root)

    def store(self, suite):
        return FileConfigStore(self.root / hashlib.sha256(suite.encode()).hexdigest())

    def list(self):
        result = []
        for directory in sorted(self.root.glob("*")):
            if directory.is_dir():
                try:
                    result.append(FileConfigStore(directory).load())
                except FileNotFoundError:
                    continue
        return result


class PostgresSuiteCatalog:
    def __init__(self, documents):
        self.documents = documents

    def store(self, suite):
        from pairity.postgres import PostgresConfigStore

        return PostgresConfigStore(self.documents, suite)

    def list(self):
        return [self.store(key).load() for key in self.documents.keys("suite")]


class NoKeyMetadata:
    api_version = 1

    def primary_key(self, table):
        return None


class Workbench:
    """Adapters are injected per caller; authorization belongs at the host boundary."""

    def __init__(
        self,
        catalog,
        *,
        metadata=None,
        submitter=None,
        results=None,
        authorize_sources=None,
        artifacts=None,
        discovery: CatalogDiscovery | None = None,
    ):
        self.catalog = catalog
        self.metadata = metadata or NoKeyMetadata()
        self.submitter = submitter
        self.results = results
        self.authorize_sources = authorize_sources or (lambda config: None)
        self.artifacts = artifacts
        if discovery is not None and discovery.api_version != 1:
            raise ValueError("Unsupported discovery adapter API version")
        self.discovery = discovery

    def catalog_discovery(self):
        if self.discovery is None:
            raise ValueError("Unity Catalog discovery is available in the Databricks-hosted app")
        return self.discovery

    def inspect_pair(self, content, format, pair_id):
        from pairity.inspection import inspect_pair

        if len(content.encode()) > 2 * 1024 * 1024:
            raise ValueError("Configuration exceeds 2 MiB")
        return inspect_pair(load_config(content, format), pair_id, self.catalog_discovery())

    @staticmethod
    def document(stored):
        return {
            "revision": stored.revision,
            "config": stored.config.model_dump(exclude_none=True),
            "config_hash": stored.config.fingerprint,
        }

    def list_suites(self):
        return [
            {
                "suite": s.config.suite,
                "revision": s.revision,
                "pairs": len(s.config.pairs),
                "config_hash": s.config.fingerprint,
            }
            for s in self.catalog.list()
        ]

    def get(self, suite, revision=None):
        return self.document(self.catalog.store(suite).load(revision))

    def effective(self, config):
        result = []
        for pair in config.pairs:
            try:
                keys = [key.model_dump() for key in resolve_keys(pair, self.metadata)]
                reason = None
            except MissingKeys as exc:
                keys, reason = [], str(exc)
            result.append(
                {
                    "pair_id": pair.id,
                    "settings": resolve_settings(config, pair).model_dump(),
                    "keys": keys,
                    "needs_keys": not keys,
                    "reason": reason,
                }
            )
        return result

    def validate(self, content, format):
        if len(content.encode()) > 2 * 1024 * 1024:
            raise ValueError("Configuration exceeds 2 MiB")
        config = load_config(content, format)
        return {
            "config": config.model_dump(exclude_none=True),
            "config_hash": config.fingerprint,
            "effective": self.effective(config),
        }

    def _candidate(self, suite, content, format, expected_revision):
        config = SuiteConfig.model_validate(self.validate(content, format)["config"])
        if config.suite != suite:
            raise ValueError("Suite name must match the destination")
        try:
            previous = self.catalog.store(suite).load()
        except FileNotFoundError:
            previous = None
        if expected_revision != (previous.revision if previous else 0):
            raise RevisionConflict("Configuration changed; reload before applying edits")
        return config, previous

    def preview(self, suite, content, format, expected_revision):
        config, previous = self._candidate(suite, content, format, expected_revision)
        preview = (
            preview_update(previous.config, config)
            if previous
            else {
                "added": [p.id for p in config.pairs],
                "changed": [],
                "unchanged": [],
                "removed": [],
            }
        )
        return {
            "preview": preview,
            "config_hash": config.fingerprint,
            "effective": self.effective(config),
            "revision": expected_revision,
            "suite_settings_changed": bool(
                previous
                and (
                    previous.config.defaults != config.defaults
                    or previous.config.templates != config.templates
                    or previous.config.execution != config.execution
                )
            ),
        }

    def save(self, suite, content, format, expected_revision):
        config, _ = self._candidate(suite, content, format, expected_revision)
        return self.document(
            self.catalog.store(suite).save(config, expected_revision=expected_revision)
        )

    def patch(self, suite, patch, *, expected_revision, pair_ids=None, tags=None, apply=False):
        stored = self.catalog.store(suite).load()
        if stored.revision != expected_revision:
            raise RevisionConflict("Configuration changed; reload before applying edits")
        config = patch_pairs(stored.config, patch, ids=pair_ids or None, tags=tags or None)
        preview = preview_update(stored.config, config)
        revision = stored.revision
        if apply:
            revision = self.catalog.store(suite).save(config, expected_revision=revision).revision
        return {
            "preview": preview,
            "applied": apply,
            "revision": revision,
            "config_hash": config.fingerprint,
            "suite_settings_changed": False,
        }

    def submit(self, suite, *, revision, request_id, pair_ids=None, tags=None):
        config = self.catalog.store(suite).load(revision).config
        selected = select_pairs(config, pair_ids or None, tags or None)
        if not any(p.enabled for p in selected):
            raise ValueError("Select at least one enabled pair")
        data = config.model_dump(exclude_none=True)
        data["pairs"] = [p.model_dump(exclude_none=True) for p in selected]
        config = SuiteConfig.model_validate(data)
        self.authorize_sources(config)
        for pair in config.pairs:
            if pair.enabled:
                try:
                    pair.keys = resolve_keys(pair, self.metadata)
                except MissingKeys as exc:
                    raise ValueError(f"Supply primary keys for {pair.id}: {exc}") from exc
        if self.submitter is None:
            raise ValueError("No submission backend configured")
        handle = self.submitter.submit(config, request_id=request_id)
        return asdict(handle) | {
            "suite": config.suite,
            "revision": revision,
            "result_run_id": (
                f"job:{handle.run_id}" if handle.backend == "databricks-jobs" else handle.run_id
            ),
            "pairs": [
                {"pair_index": index, "pair_id": pair.id, "enabled": pair.enabled}
                for index, pair in enumerate(config.pairs)
            ],
        }

    def report(self, run_id):
        if self.results is None:
            raise ValueError("No result store configured")
        document = self.results.load(run_id)
        report = document.get("report", document)
        self.authorize_sources(SuiteConfig.model_validate(report["configuration"]))
        return report

    def incident(self, run_id, *, pair_ids=None):
        from pairity.incidents import IncidentExporter

        return IncidentExporter().export(self.report(run_id), pair_ids=pair_ids)
