import copy

import pytest

from pairity.config import SuiteConfig
from pairity.results import PairResult, RunResult
from pairity.serverless import aggregate, child_run_id, prepare_config


def config():
    return SuiteConfig.model_validate(
        {
            "schema_version": 1,
            "suite": "jobs",
            "pairs": [
                {
                    "id": name,
                    "baseline": "cat.sch.a",
                    "candidate": "cat.sch.b",
                    "keys": [{"baseline": "id", "candidate": "id"}],
                }
                for name in ["one", "two"]
            ],
        }
    )


def test_prepare_pins_shared_leaves_once_without_mutating_config():
    original = config()
    calls = []

    def version(table):
        calls.append(table)
        return 7

    frozen = prepare_config(original, version)
    assert calls == ["cat.sch.a", "cat.sch.b"]
    assert frozen.pairs[0].baseline.version == 7
    assert original.pairs[0].baseline == "cat.sch.a"


def test_aggregate_missing_results_fail_closed_and_keep_order():
    suite = config()

    class Store:
        def load(self, run_id):
            raise FileNotFoundError(run_id)

    result = aggregate(suite, "job:1", "start", Store())
    assert result.status == "error"
    assert [p.pair_id for p in result.pairs] == ["one", "two"]


def test_aggregate_rejects_wrong_config_or_duplicate_pair():
    suite = config()

    class Store:
        def load(self, run_id):
            pair = suite.pairs[0]
            report = RunResult(
                run_id,
                suite.suite,
                "wrong",
                suite.model_dump(),
                [PairResult(pair.id, "error")],
                "start",
                "end",
            ).to_dict()
            return {"report": report}

    with pytest.raises(ValueError, match="identity"):
        aggregate(suite, "job:1", "start", Store())


def test_aggregate_accepts_exact_children_and_reports_failure():
    suite = config()
    reports = {}
    for i, pair in enumerate(suite.pairs):
        run_id = child_run_id("job:1", i)
        reports[run_id] = {
            "report": RunResult(
                run_id,
                suite.suite,
                suite.fingerprint,
                suite.model_dump(exclude_none=True),
                [PairResult(pair.id, "blocked", reason="bad key")],
                "start",
                "end",
            ).to_dict()
        }

    class Store:
        def load(self, run_id):
            return copy.deepcopy(reports[run_id])

    assert aggregate(suite, "job:1", "start", Store()).status == "blocked"


def test_task_engine_never_uses_classic_session_or_context():
    from pairity.serverless import create_task_engine

    class ConnectSession:
        def __getattr__(self, name):
            raise AssertionError(f"Unexpected Spark API: {name}")

    suite = config()
    for pair in suite.pairs:
        pair.enabled = False
    engine = create_task_engine(ConnectSession())
    assert engine.run(suite, pair_ids=["one"]).status == "skipped"
    with pytest.raises(ValueError, match="exactly one"):
        engine.run(suite)
