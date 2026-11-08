"""Job-level fan-out: immutable inputs, isolated pair tasks, fail-closed aggregation."""

from datetime import datetime, timezone

from pairity.config import SuiteConfig, TableSource, resolve_settings
from pairity.results import PairResult, RunResult, validate_pair_result
from pairity.sources import child_sources, expand_source, replace_children


def prepare_config(config, latest_version, version_at=None):
    """Expand templates and pin each table once for the entire suite.

    Unversioned tables pin to their latest version; ``as_of`` tables pin to the version
    that instant resolves to, keeping ``as_of`` so workers re-check the resolution.
    """
    frozen = config.model_copy(deep=True)
    versions = {}

    def pin(source):
        if isinstance(source, TableSource):
            if source.version is None:
                key = (source.table, source.as_of)
                if key not in versions:
                    if source.as_of is None:
                        versions[key] = latest_version(source.table)
                    elif version_at is None:
                        raise ValueError("as_of sources need a version_at resolver")
                    else:
                        versions[key] = version_at(source.table, source.as_of)
                source.version = versions[key]
            return source
        return replace_children(source, [pin(child) for child in child_sources(source)])

    for pair in frozen.pairs:
        if not pair.enabled:
            continue
        for side in ("baseline", "candidate"):
            source = expand_source(getattr(pair, side), frozen.templates)
            explicit = getattr(pair, side + "_version")
            if explicit is not None:
                source.version = explicit
            setattr(pair, side, pin(source))
            setattr(pair, side + "_version", None)
    return SuiteConfig.model_validate(frozen.model_dump(exclude_none=True))


def child_run_id(run_id, index):
    if type(index) is not int or index < 0:
        raise ValueError("Pair index must be a nonnegative integer")
    return f"{run_id}:pair:{index}"


def load_child(store, config, run_id, index, *, retain_differences=None):
    document = store.load(child_run_id(run_id, index))
    if (
        retain_differences is not None
        and document.get("differences_retained") != retain_differences
    ):
        raise ValueError("Child result retention policy mismatch")
    report = document["report"]
    pair = config.pairs[index]
    if (
        report["run_id"] != child_run_id(run_id, index)
        or report["config_hash"] != config.fingerprint
        or report["configuration"] != config.model_dump(exclude_none=True)
        or report["suite"] != config.suite
        or len(report["pairs"]) != 1
        or report["pairs"][0]["pair_id"] != pair.id
    ):
        raise ValueError("Child result identity or configuration mismatch")
    outcome = PairResult(**report["pairs"][0])
    validate_pair_result(outcome, pair, resolve_settings(config, pair))
    return outcome


def aggregate(config, run_id, started_at, store):
    outcomes = []
    for index, pair in enumerate(config.pairs):
        try:
            outcome = load_child(store, config, run_id, index)
        except FileNotFoundError:
            outcome = PairResult(pair.id, "error", reason="Job task did not publish a result")
        outcomes.append(outcome)
    return RunResult(
        run_id,
        config.suite,
        config.fingerprint,
        config.model_dump(exclude_none=True),
        outcomes,
        started_at,
        datetime.now(timezone.utc).isoformat(),
    )


class InlineExecution:
    """A task owns one pair; Databricks Jobs owns parallel scheduling."""

    api_version = 1

    def map(self, function, items, *, max_parallel_pairs):
        if len(items) != 1:
            raise ValueError("A serverless worker must execute exactly one pair")
        return [function(items[0])]


def create_task_engine(spark, *, sink=None):
    from contextlib import nullcontext

    from pairity.engine import ComparisonRunner, PairityEngine
    from pairity.metadata import UnityCatalogMetadata
    from pairity.spark import SparkComparisonBackend, SparkTableReader
    from pairity.spark_sources import SparkSourceResolver

    class ConnectReader(SparkTableReader):
        def scope(self, run_id, pair_id):
            return nullcontext()

    def factory():
        return ComparisonRunner(
            ConnectReader(spark),
            UnityCatalogMetadata(
                lambda sql, params: (r.asDict() for r in spark.sql(sql, args=params).collect())
            ),
            SparkComparisonBackend(),
            source_resolver=SparkSourceResolver(),
        )

    return PairityEngine(factory, execution=InlineExecution(), sink=sink)
