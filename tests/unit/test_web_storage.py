import hashlib
import json
from types import SimpleNamespace

import pytest


def sql_response(state, *, data=None, error=None, sql_state=None, statement_id="stmt-1"):
    from databricks.sdk.service.sql import StatementResponse

    value = {"statement_id": statement_id, "status": {"state": state}}
    if error:
        value["status"]["error"] = error
        value["status"]["sql_state"] = sql_state
    if state == "SUCCEEDED":
        value["manifest"] = {"schema": {"columns": [{"name": "name"}]}, "truncated": False}
        value["result"] = {"data_array": data or [], "row_count": len(data or [])}
    return StatementResponse.from_dict(value)


def sql_client(monkeypatch, *responses):
    from pairity.web import storage

    clock, calls, replies = [0], [], iter(responses)
    monkeypatch.setattr(storage.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        storage.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )

    def execute(**kwargs):
        calls.append(("execute", kwargs))
        clock[0] += 30
        return next(replies)

    def get(**kwargs):
        calls.append(("get", kwargs))
        return next(replies)

    client = SimpleNamespace(
        statement_execution=SimpleNamespace(
            execute_statement=execute,
            get_statement=get,
            cancel_execution=lambda **kwargs: calls.append(("cancel", kwargs)),
        )
    )
    return client, calls


def test_sql_waits_for_cold_warehouse_without_reexecuting(monkeypatch):
    from pairity.web.storage import SqlQuery

    client, calls = sql_client(
        monkeypatch,
        sql_response("PENDING"),
        sql_response("RUNNING"),
        sql_response("SUCCEEDED", data=[["ready"]]),
    )
    assert SqlQuery(client, "warehouse")("SELECT :name AS name", {"name": "bound"}) == [
        {"name": "ready"}
    ]
    assert [c[0] for c in calls] == ["execute", "get", "get"]
    assert calls[0][1]["on_wait_timeout"].value == "CONTINUE"
    assert calls[0][1]["parameters"][0].value == "bound"
    assert calls[0][1]["row_limit"] == 1000
    assert calls[0][1]["byte_limit"] == 4 * 1024 * 1024
    assert all(c[1]["statement_id"] == "stmt-1" for c in calls[1:])


def test_sql_timeout_cancels_same_statement(monkeypatch):
    from pairity.web.storage import SqlQuery

    client, calls = sql_client(monkeypatch, *[sql_response("PENDING")] * 5)
    with pytest.raises(ValueError, match="timed out.*stmt-1"):
        SqlQuery(client, "warehouse", timeout_seconds=35)("SELECT 1", {})
    assert [c for c in calls if c[0] == "cancel"] == [("cancel", {"statement_id": "stmt-1"})]
    assert len([c for c in calls if c[0] == "execute"]) == 1


def test_sql_terminal_failure_reports_safe_diagnostics(monkeypatch, caplog):
    from pairity.web.storage import SqlQuery

    client, calls = sql_client(
        monkeypatch,
        sql_response(
            "FAILED",
            sql_state="42501",
            error={
                "error_code": "BAD_REQUEST",
                "message": "[INSUFFICIENT_PERMISSIONS] secret SQL and token",
            },
        ),
    )
    with pytest.raises(ValueError) as exc:
        SqlQuery(client, "warehouse")("SELECT secret_value", {})
    for marker in ["FAILED", "42501", "INSUFFICIENT_PERMISSIONS", "stmt-1", "SELECT"]:
        assert marker in str(exc.value)
    assert "secret" not in str(exc.value) + caplog.text
    assert "stmt-1" in caplog.text
    assert [c[0] for c in calls] == ["execute"]


def test_sql_pending_without_handle_is_not_success(monkeypatch):
    from pairity.web.storage import SqlQuery

    client, calls = sql_client(monkeypatch, sql_response("PENDING", statement_id=None))
    with pytest.raises(ValueError, match="statement ID"):
        SqlQuery(client, "warehouse")("SELECT 1", {})
    assert len(calls) == 1


def test_sql_timeout_does_not_claim_cancellation_if_api_rejects_it(monkeypatch):
    from pairity.web.storage import SqlQuery

    client, _ = sql_client(monkeypatch, sql_response("PENDING"))

    def rejected(**kwargs):
        raise PermissionError("do not echo credentials")

    client.statement_execution.cancel_execution = rejected
    with pytest.raises(ValueError, match="Cancellation could not be confirmed") as exc:
        SqlQuery(client, "warehouse", timeout_seconds=30)("SELECT 1", {})
    assert "credentials" not in str(exc.value)


@pytest.mark.parametrize("failed", [False, True])
def test_source_sql_checks_gate_job_submission(monkeypatch, tmp_path, failed):
    from test_workbench import suite

    from pairity.serialization import dump_config
    from pairity.submission import RunHandle
    from pairity.web.storage import SqlQuery, source_authorizer
    from pairity.workbench import FileSuiteCatalog, Workbench

    final = (
        sql_response(
            "FAILED",
            error={"error_code": "BAD_REQUEST", "message": "[TABLE_OR_VIEW_NOT_FOUND] hidden"},
        )
        if failed
        else sql_response("SUCCEEDED")
    )
    client, calls = sql_client(
        monkeypatch, sql_response("PENDING"), final, sql_response("SUCCEEDED")
    )

    def submit(config, request_id):
        calls.append(("job", request_id))
        return RunHandle("databricks-jobs", "123")

    service = Workbench(
        FileSuiteCatalog(tmp_path),
        submitter=SimpleNamespace(submit=submit),
        authorize_sources=source_authorizer(SqlQuery(client, "warehouse")),
    )
    service.save("daily", dump_config(suite(), "json"), "json", 0)
    if failed:
        with pytest.raises(ValueError, match="TABLE_OR_VIEW_NOT_FOUND"):
            service.submit("daily", revision=1, request_id="test")
        assert not any(c[0] == "job" for c in calls)
    else:
        assert service.submit("daily", revision=1, request_id="test")["run_id"] == "123"
        assert [c[0] for c in calls] == ["execute", "get", "execute", "job"]


def test_result_queries_bind_values_and_verify_checksums():
    from pairity.web.storage import SqlResultReader

    report = {"run_id": "job:1", "suite": "suite", "configuration": {}}
    payload = json.dumps({"storage_schema_version": 1, "report": report})
    calls = []

    def query(sql, params):
        calls.append((sql, params))
        return [{"payload": payload, "report_hash": hashlib.sha256(payload.encode()).hexdigest()}]

    reader = SqlResultReader(query, "cat.sch.results")
    assert reader.load("job:'1")["report"] == report
    assert "job:'1" not in calls[0][0]
    assert calls[0][1]["run_id"] == "job:'1"
    with pytest.raises(ValueError):
        SqlResultReader(query, "table; DROP TABLE result")
    bad = SqlResultReader(
        lambda *_: [{"payload": payload, "report_hash": "wrong"}], "cat.sch.results"
    )
    with pytest.raises(ValueError, match="checksum"):
        bad.load("job:1")


def test_authorization_cache_is_request_scoped():
    from test_workbench import suite

    from pairity.web.storage import source_authorizer

    calls = []

    def query(sql, params):
        return calls.append(sql)

    authorize = source_authorizer(query)
    authorize(suite())
    authorize(suite())
    assert len(calls) == 2
    source_authorizer(query)(suite())
    assert len(calls) == 4


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        (
            "SQL check failed (FAILED; BAD_REQUEST; SQLSTATE 42501; INSUFFICIENT_PERMISSIONS).",
            "You cannot read source table cat.sch.orders. Ask its owner to grant your "
            "Databricks user (alice@example.com), directly or through a group, USE CATALOG "
            "on cat, USE SCHEMA on cat.sch and SELECT on cat.sch.orders",
        ),
        (
            "SQL check failed (FAILED; BAD_REQUEST; SQLSTATE 42P01; TABLE_OR_VIEW_NOT_FOUND).",
            "Source table cat.sch.orders was not found, or your Databricks user "
            "(alice@example.com) has no privileges on it.",
        ),
    ],
)
def test_refused_source_checks_name_the_table_and_required_grants(message, expected):
    from pairity.config import SuiteConfig
    from pairity.web.storage import source_authorizer

    def query(sql, params):
        if "cat`.`sch`.`orders" in sql:
            raise ValueError(message)
        return []

    config = SuiteConfig.model_validate(
        {
            "schema_version": 1,
            "suite": "s",
            "pairs": [
                {
                    "id": "p",
                    "baseline": "cat.sch.orders",
                    "candidate": "cat.dev.orders",
                    "keys": [{"baseline": "id", "candidate": "id"}],
                }
            ],
        }
    )
    with pytest.raises(ValueError) as error:
        source_authorizer(query, "alice@example.com")(config)
    assert str(error.value).startswith(expected)
    assert message in str(error.value)
