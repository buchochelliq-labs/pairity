from pathlib import Path

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
