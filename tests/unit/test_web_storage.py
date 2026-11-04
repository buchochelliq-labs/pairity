import hashlib
import json

import pytest


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
