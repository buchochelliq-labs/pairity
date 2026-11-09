from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from pairity.workbench import FileSuiteCatalog, Workbench


def client(tmp_path):
    from pairity.web.app import create_app

    service = Workbench(FileSuiteCatalog(tmp_path))
    return TestClient(create_app(lambda request: ("tester", service), mode="local"))


def test_import_preview_revision_and_download(tmp_path):
    c = client(tmp_path)
    text = Path("examples/suite.yaml").read_text()
    headers = {"X-Pairity-Request": "1"}
    validated = c.post(
        "/api/v1/validate", json={"content": text, "format": "yaml"}, headers=headers
    )
    assert validated.status_code == 200
    suite = validated.json()["config"]["suite"]
    body = {"content": text, "format": "yaml", "expected_revision": 0}
    assert (
        c.post("/api/v1/preview", json=body | {"suite": suite}, headers=headers).status_code == 200
    )
    assert c.put("/api/v1/suites/" + suite, json=body, headers=headers).json()["revision"] == 1
    assert c.put("/api/v1/suites/" + suite, json=body, headers=headers).status_code == 409
    assert c.get("/api/v1/exports/" + suite + "?format=json").json()["suite"] == suite
    assert len(c.get("/api/v1/suites").json()["items"]) == 1


def test_reject_mutation_without_same_origin_marker(tmp_path):
    c = client(tmp_path)
    assert c.post("/api/v1/validate", json={"content": "{}", "format": "json"}).status_code == 403


def test_explorer_endpoints_use_injected_discovery_and_keep_schema_only_adapters(tmp_path):
    from types import SimpleNamespace

    from pairity.web.app import create_app

    calls = []

    def describe(table):
        calls.append(table)
        return {
            "table": table,
            "columns": [{"name": "id", "type": "INT", "nullable": False}],
            "primary_key": ["id"],
        }

    provider = SimpleNamespace(
        api_version=1,
        describe=describe,
        tables=lambda catalog, schema, **kw: {
            "items": [{"name": "member"}],
            "next_after": "member",
        },
    )
    service = Workbench(FileSuiteCatalog(tmp_path), discovery=provider)
    c = TestClient(create_app(lambda request: ("tester", service), mode="local"))
    assert c.get("/api/v1/discovery/search").status_code == 422
    result = c.get("/api/v1/discovery/search?catalog=cat&schema=sch").json()
    assert result == {
        "items": [
            {"catalog": "cat", "schema": "sch", "name": "member", "full_name": "cat.sch.member"}
        ],
        "next_after": "cat.sch.member",
    }
    result = c.get("/api/v1/discovery/table?table=cat.sch.member").json()
    assert result["primary_key"] == ["id"]
    assert calls == ["cat.sch.member"]


@pytest.mark.parametrize("name", ["2026_sales", "customer's_sales"])
def test_explorer_v1_adapter_can_resume_every_emitted_table_cursor(tmp_path, name):
    from types import SimpleNamespace

    from pairity.web.app import create_app

    cursors = []

    def tables(catalog, schema, **paging):
        cursors.append(paging["after"])
        if not paging["after"]:
            return {"items": [{"name": name}], "next_after": name}
        return {"items": [{"name": "z_sales"}], "next_after": None}

    service = Workbench(
        FileSuiteCatalog(tmp_path), discovery=SimpleNamespace(api_version=1, tables=tables)
    )
    c = TestClient(create_app(lambda request: ("tester", service), mode="local"))
    params = {"catalog": "cat", "schema": "sch"}
    page = c.get("/api/v1/discovery/search", params=params).json()
    result = c.get("/api/v1/discovery/search", params=params | {"after": page["next_after"]})
    assert result.status_code == 200
    assert result.json()["items"][0]["name"] == "z_sales"
    assert cursors == ["", name]
    assert (
        c.get(
            "/api/v1/discovery/search", params=params | {"after": f"cat.other.{name}"}
        ).status_code
        == 422
    )
    assert len(cursors) == 2


def test_hosted_app_requires_forwarded_identity(tmp_path):
    from pairity.web.app import create_app

    c = TestClient(create_app(mode="databricks"))
    assert c.get("/api/v1/suites").status_code == 401


def test_history_omits_inaccessible_sources_without_exposing_report(tmp_path):
    from types import SimpleNamespace

    from test_incidents import report

    from pairity.web.app import create_app

    good = report()
    bad = report() | {"run_id": "private"}
    bad["configuration"] = dict(bad["configuration"], suite="inaccessible")

    def authorize(config):
        if config.suite == "inaccessible":
            raise PermissionError("revoked")

    service = Workbench(
        FileSuiteCatalog(tmp_path),
        results=SimpleNamespace(
            history=lambda **kw: [bad, good], load=lambda run: {"report": good}
        ),
        authorize_sources=authorize,
    )
    c = TestClient(create_app(lambda request: ("tester", service), mode="local"))
    response = c.get("/api/v1/runs")
    assert response.status_code == 200
    assert response.json()["items"] == [good]
    assert response.json()["unavailable_count"] == 1
    archive = c.get("/api/v1/runs/run/incident?pair_id=a")
    assert archive.status_code == 200
    assert archive.content.startswith(b"PK")


def test_suite_names_with_slashes_and_invalid_request_redaction(tmp_path):
    from test_workbench import suite

    from pairity.serialization import dump_config

    c = client(tmp_path)
    config = suite().model_copy(update={"suite": "team/export"})
    headers = {"X-Pairity-Request": "1"}
    body = {"content": dump_config(config, "json"), "format": "json", "expected_revision": 0}
    assert c.put("/api/v1/suites/team%2Fexport", json=body, headers=headers).status_code == 200
    assert c.get("/api/v1/suites/team%2Fexport").json()["revision"] == 1
    assert c.get("/api/v1/exports/team%2Fexport?format=json").json()["suite"] == "team/export"
    invalid = c.post("/api/v1/validate", json={"secret": "do-not-echo"}, headers=headers)
    assert invalid.status_code == 422
    assert "do-not-echo" not in invalid.text


def test_unexpected_errors_are_logged_and_name_the_exception_type(tmp_path, caplog):
    import logging
    from types import SimpleNamespace

    from pairity.web.app import create_app

    class PermissionDenied(Exception):
        pass

    def fail(*args, **kwargs):
        raise PermissionDenied("App principal cannot read job permissions")

    service = SimpleNamespace(list_suites=fail)
    client = TestClient(
        create_app(lambda request: ("tester", service), mode="local"), raise_server_exceptions=False
    )
    with caplog.at_level(logging.ERROR, logger="pairity.web.app"):
        response = client.get("/api/v1/suites")
    assert response.status_code == 500
    assert response.json()["detail"] == (
        "Operation failed (PermissionDenied); check the App logs and resource permissions"
    )
    assert "Unhandled error on GET /api/v1/suites" in caplog.text
    assert "App principal cannot read job permissions" in caplog.text
