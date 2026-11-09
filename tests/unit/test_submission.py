from types import SimpleNamespace

from test_engine import config

from pairity.submission import DatabricksJobSubmitter, DirectSubmitter


def test_job_submission_uses_config_hash_and_idempotency_token():
    calls = []

    class Jobs:
        def run_now(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(run_id=42)

        def get_run(self, **kwargs):
            return SimpleNamespace(
                job_id=7,
                job_parameters=[
                    SimpleNamespace(
                        name="config_hash", value=calls[0]["job_parameters"]["config_hash"]
                    )
                ],
                state=SimpleNamespace(
                    life_cycle_state=SimpleNamespace(value="TERMINATED"),
                    result_state=SimpleNamespace(value="FAILED"),
                ),
                run_page_url="https://example/run",
            )

        def cancel_run(self, **kwargs):
            calls.append(kwargs)

    adapter = DatabricksJobSubmitter(
        SimpleNamespace(jobs=Jobs()), 7, lambda cfg: "/Volumes/c/s/v/suite.json"
    )
    suite = config()
    handle = adapter.submit(suite, request_id="request-1")
    assert calls[0]["idempotency_token"] == "request-1"
    assert calls[0]["job_parameters"]["config_hash"] == suite.fingerprint
    assert adapter.status(handle)["status"] == "failed"
    adapter.cancel(handle)
    assert calls[-1] == {"run_id": 42}


def test_direct_submission_preserves_engine_contract_and_stable_id():
    calls = []

    class Engine:
        def run(self, config, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(status="passed")

    adapter = DirectSubmitter(Engine())
    handle = adapter.submit(config(), request_id="direct-1")
    assert calls == [{"run_id": "direct-1"}]
    assert adapter.status(handle)["status"] == "passed"
    assert adapter.result(handle).status == "passed"


def test_jobs_rejects_token_reused_for_different_config():
    import pytest

    class Jobs:
        def run_now(self, **kwargs):
            return SimpleNamespace(run_id=42)

        def get_run(self, **kwargs):
            return SimpleNamespace(
                job_id=7, job_parameters=[SimpleNamespace(name="config_hash", value="different")]
            )

    adapter = DatabricksJobSubmitter(SimpleNamespace(jobs=Jobs()), 7, lambda cfg: "/suite.json")
    with pytest.raises(ValueError, match="different configuration"):
        adapter.submit(config(), request_id="reused")


def test_each_submission_step_reports_its_failure_and_whether_a_run_exists():
    import pytest

    from pairity.submission import SubmissionStepFailed

    class Denied(Exception):
        pass

    def jobs(run_now=None, get_run=None):
        def default_run_now(**kwargs):
            return SimpleNamespace(run_id=42)

        def denied(**kwargs):
            raise Denied("principal lacks permission")

        return SimpleNamespace(run_now=run_now or default_run_now, get_run=get_run or denied)

    def publish(cfg):
        raise Denied("no WRITE VOLUME")

    cases = [
        (jobs(), publish, "Publishing the suite snapshot", "No Job was started"),
        (
            jobs(run_now=lambda **kwargs: (_ for _ in ()).throw(Denied("no CAN_MANAGE_RUN"))),
            lambda cfg: "/Volumes/c/s/v/suite.json",
            "Starting Job 7 failed",
            "No run was confirmed",
        ),
        (jobs(), lambda cfg: "/Volumes/c/s/v/suite.json", "Job run 42 was started", "Check"),
    ]
    for client_jobs, publisher, step, outcome in cases:
        adapter = DatabricksJobSubmitter(SimpleNamespace(jobs=client_jobs), 7, publisher)
        with pytest.raises(SubmissionStepFailed) as error:
            adapter.submit(config(), request_id="request-1")
        message = str(error.value)
        assert message.startswith(step) and outcome in message
        assert "Details: Denied:" in message
