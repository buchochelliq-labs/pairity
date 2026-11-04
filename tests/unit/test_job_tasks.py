from types import SimpleNamespace

import pytest
from test_engine import config, passed

from pairity import job_tasks
from pairity.results import RunResult


def test_final_task_replay_is_idempotent_but_changed_evidence_rejected(monkeypatch):
    suite = config(1)
    report = RunResult(
        "job:1",
        suite.suite,
        suite.fingerprint,
        suite.model_dump(exclude_none=True),
        [passed("0")],
        "start",
        "end",
    )

    class Store:
        def load(self, run_id):
            return {"report": report.to_dict()}

        def write(self, result):
            raise AssertionError("Must not rewrite completed evidence")

    monkeypatch.setattr(
        job_tasks, "_manifest", lambda _: ({"run_id": "job:1", "started_at": "start"}, suite)
    )
    monkeypatch.setattr(job_tasks, "_store", lambda *_: Store())
    monkeypatch.setattr(
        job_tasks,
        "aggregate",
        lambda *_: RunResult(
            "job:1",
            suite.suite,
            suite.fingerprint,
            suite.model_dump(exclude_none=True),
            [passed("0")],
            "start",
            "later",
        ),
    )
    job_tasks.finish(None, None)
    report.pairs[0].status = "error"
    with pytest.raises(ValueError, match="immutable"):
        job_tasks.finish(None, None)


def test_prepare_rejects_config_hash_before_spark(monkeypatch, tmp_path):
    from pairity.serialization import dump_config

    path = tmp_path / "suite.yaml"
    path.write_text(dump_config(config(), "yaml"))
    values = {"config_path": str(path), "config_hash": "wrong"}
    dbutils = SimpleNamespace(widgets=SimpleNamespace(get=values.__getitem__))
    with pytest.raises(ValueError, match="hash mismatch"):
        job_tasks.prepare(None, dbutils)
