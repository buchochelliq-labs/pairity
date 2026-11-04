from threading import Barrier, Lock

import pytest

from pairity.config import SuiteConfig
from pairity.engine import PairityEngine
from pairity.results import BlockedComparison, PairResult


def config(n=6, parallel=2):
    return SuiteConfig.model_validate(
        {
            "schema_version": 1,
            "suite": "parallel",
            "execution": {"max_parallel_pairs": parallel},
            "pairs": [
                {"id": str(i), "baseline": f"prod.s.t{i}", "candidate": f"dev.s.t{i}"}
                for i in range(n)
            ],
        }
    )


def passed(pair_id):
    return PairResult(
        pair_id,
        "passed",
        counts={
            "baseline_rows": 0,
            "candidate_rows": 0,
            "matched_rows": 0,
            "changed_rows": 0,
            "missing_rows": 0,
            "extra_rows": 0,
        },
    )


def test_parallel_pairs_really_overlap_obey_limit_and_have_fresh_plugins():
    gate = Barrier(2)
    lock = Lock()
    active = 0
    peak = 0

    class Runner:
        api_version = 1

        def __init__(self):
            self.used = False

        def run(self, pair, settings, run_id):
            nonlocal active, peak
            assert not self.used, "The runner was shared between pairs"
            self.used = True
            with lock:
                active += 1
                peak = max(peak, active)
            try:
                gate.wait(timeout=5)
                return passed(pair.id)
            finally:
                with lock:
                    active -= 1

    run = PairityEngine(Runner).run(config())
    assert run.status == "passed", run.to_dict()
    assert peak == 2
    assert [p.pair_id for p in run.pairs] == ["0", "1", "2", "3", "4", "5"]
    assert run.configuration["execution"]["max_parallel_pairs"] == 2


def test_pair_failure_does_not_cancel_others_or_yield_success():
    class Runner:
        api_version = 1

        def run(self, pair, settings, run_id):
            if pair.id == "0":
                raise PermissionError("denied")
            if pair.id == "1":
                raise BlockedComparison("Supply PKs")
            return passed(pair.id)

    run = PairityEngine(Runner).run(config(3))
    assert [p.status for p in run.pairs] == ["error", "blocked", "passed"]
    assert run.status == "error"
    assert run.pairs[1].reason == "Supply PKs"


def test_disabled_pairs_never_execute_and_empty_selection_never_passes():
    def forbidden():
        raise AssertionError("Disabled pairs must not instantiate plugins")

    suite = config(1)
    suite.pairs[0].enabled = False
    assert PairityEngine(forbidden).run(suite).status == "skipped"
    with pytest.raises(ValueError):
        PairityEngine(forbidden).run(suite, pair_ids=["missing"])


def test_result_persistence_failure_is_visible():
    class Runner:
        api_version = 1

        def run(self, pair, settings, run_id):
            return passed(pair.id)

    class BrokenSink:
        api_version = 1

        def write(self, result):
            raise OSError("cannot persist")

    with pytest.raises(OSError, match="cannot persist"):
        PairityEngine(Runner, sink=BrokenSink()).run(config(1))


def test_invalid_plugin_output_cannot_pass_a_suite():
    class Runner:
        api_version = 1

        def run(self, pair, settings, run_id):
            return PairResult("wrong-pair", "passed")

    assert PairityEngine(Runner).run(config(1)).status == "error"


@pytest.mark.parametrize("mode", ["omit", "duplicate", "wrong_id"])
def test_executor_cannot_silently_drop_or_replace_selected_pairs(mode):
    class Runner:
        api_version = 1

        def run(self, pair, settings, run_id):
            return passed(pair.id)

    class BrokenExecutor:
        api_version = 1

        def map(self, function, items, **kwargs):
            first = function(list(items)[0])
            return {
                "omit": [first],
                "duplicate": [first, first],
                "wrong_id": [first, passed("unrequested")],
            }[mode]

    with pytest.raises(ValueError, match="coverage"):
        PairityEngine(Runner, execution=BrokenExecutor()).run(config(2))


def test_orchestrator_restores_configured_order_for_unordered_executor():
    class Runner:
        api_version = 1

        def run(self, pair, settings, run_id):
            return passed(pair.id)

    class UnorderedExecutor:
        api_version = 1

        def map(self, function, items, **kwargs):
            return list(reversed([function(item) for item in items]))

    result = PairityEngine(Runner, execution=UnorderedExecutor()).run(config(3))
    assert [p.pair_id for p in result.pairs] == ["0", "1", "2"]


@pytest.mark.parametrize(
    "alter",
    [
        lambda r: setattr(r, "counts", {}),
        lambda r: r.counts.update({"changed_rows": -1}),
        lambda r: r.counts.update({"baseline_rows": 1}),
        lambda r: r.counts.update({"matched_rows": True}),
        lambda r: r.schema_differences.append("missing column"),
        lambda r: r.counts.update(
            {"baseline_rows": 1, "candidate_rows": 1, "matched_rows": 1, "changed_rows": 1}
        ),
    ],
)
def test_malformed_or_threshold_violating_pass_from_plugin_is_error(alter):
    class Runner:
        api_version = 1

        def run(self, pair, settings, run_id):
            result = passed(pair.id)
            alter(result)
            return result

    assert PairityEngine(Runner).run(config(1)).status == "error"
