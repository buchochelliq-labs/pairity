import json
from types import SimpleNamespace

import pytest


def test_hosted_submission_uses_caller_identity_and_no_privileged_fallback(monkeypatch):
    import databricks.sdk
    from test_workbench import suite

    from pairity.web.runtime import service_for_request

    caller = SimpleNamespace(
        current_user=SimpleNamespace(me=lambda: SimpleNamespace(user_name="user"))
    )
    privileged = SimpleNamespace(files=object())
    clients = []

    def client(**kwargs):
        clients.append(kwargs)
        return caller if kwargs else privileged

    monkeypatch.setattr(databricks.sdk, "WorkspaceClient", client)
    for key, value in {
        "PAIRITY_APP_MODE": "databricks",
        "DATABRICKS_HOST": "https://example.invalid",
        "PAIRITY_SQL_WAREHOUSE_ID": "warehouse",
        "PAIRITY_POSTGRES_DSN": "postgresql://example.invalid/test",
        "PAIRITY_JOB_ID": "123",
        "PAIRITY_RESULT_TABLE": "cat.sch.results",
        "PAIRITY_ARTIFACT_ROOT": "/Volumes/cat/sch/evidence",
    }.items():
        monkeypatch.setenv(key, value)
    user, workbench = service_for_request(
        SimpleNamespace(headers={"x-forwarded-access-token": "caller-token"})
    )
    assert user == "user"
    assert clients[0]["auth_type"] == "pat"
    assert workbench.submitter.submitter.client is caller
    assert workbench.submitter.observer.client is caller

    def denied(**kwargs):
        raise PermissionError("Caller cannot manage Job runs")

    caller.jobs = SimpleNamespace(run_now=denied)
    workbench.submitter.submitter.config_publisher = lambda c: "/Volumes/c/s/v/config.json"
    with pytest.raises(PermissionError):
        workbench.submitter.submit(suite(), request_id="denied")


def test_local_reader_can_load_older_than_history_limit(tmp_path):
    from pairity.web.runtime import FileResultReader

    for i in range(101):
        (tmp_path / f"{i}.json").write_text(
            json.dumps(
                {
                    "result_schema_version": 1,
                    "run_id": str(i),
                    "suite": "s",
                    "finished_at": f"{i:03}",
                }
            )
        )
    reader = FileResultReader(tmp_path)
    assert len(reader.history(limit=100)) == 100
    assert reader.load("0")["report"]["run_id"] == "0"
