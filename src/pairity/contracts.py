"""Plugin boundaries inspired by DQX's core/facade/storage separation.

Factories create one set of adapters per active pair. Only execution backends
and sinks are shared; sinks are invoked by the orchestrator after workers finish.
"""

from collections.abc import Callable, Iterable
from contextlib import AbstractContextManager
from typing import Protocol, TypeVar

from pairity.config import ColumnMapping, ComparisonSettings, PairConfig, SourceSpec, SuiteConfig
from pairity.config_storage import StoredSuite
from pairity.results import PairResult, RunResult, TableInput

T = TypeVar("T")
R = TypeVar("R")
API_VERSION = 1


class MetadataProvider(Protocol):
    api_version: int

    def primary_key(self, table: str) -> list[str] | None: ...


class TableReader(Protocol):
    api_version: int

    def scope(self, run_id: str, pair_id: str) -> AbstractContextManager: ...

    def read(self, table: str, version: int | None = None) -> TableInput: ...


class ComparisonBackend(Protocol):
    api_version: int

    def compare(
        self,
        pair: PairConfig,
        settings: ComparisonSettings,
        keys: list[ColumnMapping],
        baseline: TableInput,
        candidate: TableInput,
    ) -> PairResult: ...


class ConfigStore(Protocol):
    api_version: int

    def load(self, revision: int | None = None) -> StoredSuite: ...

    def save(self, config: SuiteConfig, *, expected_revision: int) -> StoredSuite: ...


class ResultSink(Protocol):
    api_version: int

    def write(self, result: RunResult) -> None: ...


class ExecutionBackend(Protocol):
    api_version: int

    def map(
        self, function: Callable[[T], R], items: Iterable[T], *, max_parallel_pairs: int
    ) -> list[R]: ...


class PairRunner(Protocol):
    api_version: int

    def run(self, pair: PairConfig, settings: ComparisonSettings, run_id: str) -> PairResult: ...


class SourceResolver(Protocol):
    api_version: int

    def read(self, source: SourceSpec, reader: TableReader, cache: dict) -> TableInput: ...


class ArtifactStore(Protocol):
    api_version: int

    def publish(self, run_id: str, files: dict[str, bytes]): ...

    def download(self, reference) -> dict[str, bytes]: ...


class ResultStore(ResultSink, Protocol):
    def load(self, run_id: str) -> dict: ...

    def differences(self, run_id: str, pair_id: str): ...


class SubmissionBackend(Protocol):
    """Launch/observe/cancel runs; distinct from scheduling pairs within a run."""

    api_version: int

    def submit(self, config: SuiteConfig, *, request_id: str): ...

    def status(self, handle) -> dict: ...

    def cancel(self, handle) -> None: ...
