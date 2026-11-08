"""DQX-inspired facade: orchestration composes metadata, I/O, core and sinks."""

from datetime import datetime, timezone
from uuid import uuid4

from pairity.bulk import select_pairs
from pairity.config import SuiteConfig, TableSource, resolve_settings
from pairity.execution import ThreadExecutionBackend
from pairity.metadata import resolve_keys
from pairity.plugins import require_api
from pairity.results import BlockedComparison, PairResult, RunResult, validate_pair_result


class ComparisonRunner:
    api_version = 1

    def __init__(self, reader, metadata, backend, *, source_resolver=None):
        self.reader = require_api(reader)
        self.metadata = require_api(metadata)
        self.backend = require_api(backend)
        self.source_resolver = require_api(source_resolver) if source_resolver is not None else None

    def run(self, pair, settings, run_id):
        with self.reader.scope(run_id, pair.id):
            keys = resolve_keys(pair, self.metadata)
            cache = {}

            def read(source, version):
                if isinstance(source, str):
                    source = TableSource(table=source, version=version)
                if isinstance(source, TableSource) and (source.as_of or source.where):
                    if self.source_resolver is None:
                        raise BlockedComparison("as_of and where need a source resolver")
                    return self.source_resolver.read(source, self.reader, cache)
                if isinstance(source, TableSource):
                    key = (source.table, source.version)
                    if key not in cache:
                        cache[key] = self.reader.read(*key)
                    return cache[key]
                if self.source_resolver is None:
                    raise BlockedComparison("This runner has no composed source resolver")
                return self.source_resolver.read(source, self.reader, cache)

            left = read(pair.baseline, pair.baseline_version)
            right = read(pair.candidate, pair.candidate_version)
            try:
                result = self.backend.compare(pair, settings, keys, left, right)
            except BlockedComparison as exc:
                exc.inputs = {"baseline": left.metadata(), "candidate": right.metadata()}
                exc.keys = [k.model_dump() for k in keys]
                raise
            result.keys = [k.model_dump() for k in keys]
            result.inputs = {"baseline": left.metadata(), "candidate": right.metadata()}
            return result


class PairityEngine:
    """A factory must return a fresh runner and mutable adapters for each active pair."""

    def __init__(self, runner_factory, *, execution=None, sink=None):
        self.runner_factory = runner_factory
        self.execution = require_api(
            execution if execution is not None else ThreadExecutionBackend()
        )
        self.sink = require_api(sink) if sink is not None else None

    def run(self, config: SuiteConfig, *, pair_ids=None, tags=None, run_id=None) -> RunResult:
        # Freeze a semantic copy before any worker starts or caller changes the config.
        config = SuiteConfig.model_validate(config.model_dump(exclude_none=True))
        selected = select_pairs(config, pair_ids, tags)
        run_id = str(uuid4()) if run_id is None else run_id
        if not isinstance(run_id, str) or not run_id:
            raise ValueError("run_id must be a nonempty string")
        started = datetime.now(timezone.utc).isoformat()

        def compare_pair(pair):
            settings = resolve_settings(config, pair)
            if not pair.enabled:
                return PairResult(
                    pair.id,
                    "skipped",
                    reason="Pair is disabled",
                    effective_settings=settings.model_dump(),
                )
            try:
                runner = require_api(self.runner_factory())
                from pairity.sources import expand_source

                expanded = pair.model_copy(deep=True)
                for side in ("baseline", "candidate"):
                    source = getattr(pair, side)
                    if not isinstance(source, str):
                        setattr(expanded, side, expand_source(source, config.templates))
                result = runner.run(expanded, settings, run_id)
                validate_pair_result(result, pair, settings)
            except BlockedComparison as exc:
                result = PairResult(
                    pair.id,
                    "blocked",
                    reason=str(exc),
                    casts=getattr(exc, "casts", []),
                    ignored_columns=getattr(exc, "ignored_columns", {}),
                    inputs=getattr(exc, "inputs", {}),
                    keys=getattr(exc, "keys", []),
                )
            except Exception as exc:
                result = PairResult(pair.id, "error", reason=f"{type(exc).__name__}: {exc}")
            result.effective_settings = settings.model_dump()
            return result

        pairs = self.execution.map(
            compare_pair, selected, max_parallel_pairs=config.execution.max_parallel_pairs
        )
        expected = {pair.id for pair in selected}
        if (
            not isinstance(pairs, list)
            or len(pairs) != len(selected)
            or any(not isinstance(p, PairResult) or not isinstance(p.pair_id, str) for p in pairs)
            or {p.pair_id for p in pairs} != expected
        ):
            raise ValueError("Execution backend returned incomplete or duplicate pair coverage")
        indexed = {p.pair_id: p for p in pairs}
        pairs = [indexed[pair.id] for pair in selected]
        for pair, outcome in zip(selected, pairs, strict=True):
            validate_pair_result(outcome, pair, resolve_settings(config, pair))
        result = RunResult(
            run_id,
            config.suite,
            config.fingerprint,
            config.model_dump(exclude_none=True),
            pairs,
            started,
            datetime.now(timezone.utc).isoformat(),
        )
        if self.sink is not None:
            self.sink.write(result)
        return result
