"""Atomic, revisioned local storage; remote stores implement the same contract."""

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from pairity.config import SuiteConfig
from pairity.serialization import dump_config, load_config


class RevisionConflict(ValueError):
    """The caller's revision is stale or another writer owns the store lock."""


@dataclass(frozen=True)
class StoredSuite:
    revision: int
    config: SuiteConfig


class FileConfigStore:
    """One suite per local directory. Not a distributed/UC-volume locking backend."""

    api_version = 1

    def __init__(self, root: str | Path):
        self.root = Path(root)

    def _latest(self) -> int:
        return max((int(p.stem) for p in self.root.glob("*.json") if p.stem.isdigit()), default=0)

    def load(self, revision: int | None = None) -> StoredSuite:
        revision = self._latest() if revision is None else revision
        if type(revision) is not int or revision < 1:
            raise FileNotFoundError("No configuration at the requested revision")
        path = self.root / f"{revision:08d}.json"
        return StoredSuite(revision, load_config(path.read_text(encoding="utf-8"), "json"))

    def save(self, config: SuiteConfig, *, expected_revision: int) -> StoredSuite:
        # Revalidate even if a caller mutated an already constructed model.
        config = SuiteConfig.model_validate(config.model_dump(exclude_none=True))
        if type(expected_revision) is not int or expected_revision < 0:
            raise ValueError("expected_revision must be a nonnegative integer")
        self.root.mkdir(parents=True, exist_ok=True)
        lock = self.root / ".write.lock"
        try:
            lock_fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError as exc:
            raise RevisionConflict("Another configuration writer holds the lock") from exc
        temporary = None
        try:
            latest = self._latest()
            if latest != expected_revision:
                raise RevisionConflict(f"Expected revision {expected_revision}, found {latest}")
            if latest and self.load(latest).config.suite != config.suite:
                raise ValueError("A store directory belongs to one suite")
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=self.root, prefix=".pending-", delete=False
            ) as stream:
                temporary = Path(stream.name)
                stream.write(dump_config(config, "json"))
                stream.flush()
                os.fsync(stream.fileno())
            revision = latest + 1
            os.replace(temporary, self.root / f"{revision:08d}.json")
            return StoredSuite(revision, config)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
            os.close(lock_fd)
            lock.unlink()
