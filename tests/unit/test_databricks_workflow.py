import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]


def module(path):
    spec = importlib.util.spec_from_file_location("smoke_module", ROOT / path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def test_preflight_lists_missing_names_without_secret_values():
    validate = module("scripts/databricks_preflight.py").validate
    with pytest.raises(ValueError) as error:
        validate({"DATABRICKS_TOKEN": "secret-must-not-appear"})
    assert "DATABRICKS_HOST" in str(error.value)
    assert "secret-must-not-appear" not in str(error.value)
    env = {
        "DATABRICKS_HOST": "https://example.databricks.com",
        "DATABRICKS_TOKEN": "secret",
        "DATABRICKS_AUTH_TYPE": "pat",
        "PAIRITY_CATALOG": "workspace",
        "PAIRITY_SCHEMA": "pairity_test",
        "PAIRITY_ARTIFACT_ROOT": "/Volumes/workspace/pairity_test/artifacts",
        "PAIRITY_RESULT_TABLE": "workspace.pairity_test.results",
    }
    validate(env)
    with pytest.raises(ValueError, match="HTTPS"):
        validate(env | {"DATABRICKS_HOST": "http://example.com"})


def test_expected_job_failure_still_requires_exact_terminal_state():
    from databricks.sdk.errors import OperationFailed

    script = module("scripts/databricks_smoke.py")

    class Waiter:
        run_id = 42

        def result(self, **kwargs):
            raise OperationFailed("regression")

    class Jobs:
        def run_now(self, **kwargs):
            return Waiter()

        def get_run(self, **kwargs):
            return SimpleNamespace(
                state=SimpleNamespace(result_state=SimpleNamespace(value="FAILED")),
                run_page_url="https://example/run",
            )

    records = []
    client = SimpleNamespace(jobs=Jobs())
    assert script.run(client, 1, {}, "FAILED", records) == 42
    with pytest.raises(RuntimeError, match="expected SUCCESS"):
        script.run(client, 1, {}, "SUCCESS", records)


def test_pair_overlap_requires_two_and_rejects_serial_execution():
    script = module("scripts/databricks_smoke.py")
    intervals = [(1, 4), (2, 5), (6, 9), (7, 10)]

    class Jobs:
        def get_run(self, run_id):
            if run_id == 1:
                return SimpleNamespace(tasks=[SimpleNamespace(task_key="comparisons", run_id=2)])
            return SimpleNamespace(
                iterations=[
                    SimpleNamespace(run_id=i + 10, start_time=a, end_time=b)
                    for i, (a, b) in enumerate(intervals)
                ]
            )

    assert script.overlap(SimpleNamespace(jobs=Jobs()), 1)["peak_pair_tasks"] == 2
    intervals[:] = [(1, 2), (2, 3), (3, 4), (4, 5)]
    with pytest.raises(ValueError, match="observed 1"):
        script.overlap(SimpleNamespace(jobs=Jobs()), 1)


def fixture_reports():
    import json

    from pairity.config import SuiteConfig

    expected = json.loads((ROOT / "examples/tuva/expected.json").read_text())
    reports = {}
    for variant, run_id in [("identical", "1"), ("regression", "2")]:
        config = json.loads((ROOT / f"examples/tuva/tuva-{variant}.json").read_text())
        pairs = []
        for pair in config["pairs"]:
            inputs = {}
            for side in ("baseline", "candidate"):
                source = "baseline" if side == "baseline" or variant == "identical" else "changed"
                table = f"workspace.pairity_test.gha_1_1_{source}_{pair['id']}"
                pair[side] = {"kind": "table", "table": table, "version": 0}
                inputs[side] = {"table": table, "version": 0, "snapshot_pinned": True}
            counts = expected[pair["id"]].copy()
            if variant == "identical":
                counts.update(
                    candidate_rows=counts["baseline_rows"],
                    matched_rows=counts["baseline_rows"],
                    changed_rows=0,
                    missing_rows=0,
                    extra_rows=0,
                )
            status = (
                "failed"
                if any(counts[k] for k in ("changed_rows", "missing_rows", "extra_rows"))
                else "passed"
            )
            pairs.append(
                {"pair_id": pair["id"], "counts": counts, "status": status, "inputs": inputs}
            )
        config = SuiteConfig.model_validate(config)
        reports["job:" + run_id] = {
            "report": {
                "run_id": "job:" + run_id,
                "configuration": config.model_dump(exclude_none=True),
                "config_hash": config.fingerprint,
                "suite": config.suite,
                "pairs": pairs,
                "status": "passed" if variant == "identical" else "failed",
            }
        }
    return reports


@pytest.mark.parametrize("corruption", [None, "table", "version", "counts"])
def test_verifier_checks_evidence_and_actual_input_identity(monkeypatch, corruption):
    verifier = module("notebooks/smoke_verify.py")
    reports = fixture_reports()
    pair = reports["job:2"]["report"]["pairs"][0]
    if corruption == "table":
        pair["inputs"]["baseline"]["table"] = "workspace.other.old_table"
    elif corruption == "version":
        pair["inputs"]["baseline"]["version"] = 99
    elif corruption == "counts":
        pair["counts"]["missing_rows"] = 0

    class Store:
        def load(self, run_id):
            return reports[run_id]

    writes = []
    monkeypatch.setattr(verifier, "DeltaResultStore", lambda *_: Store())
    monkeypatch.setattr(
        verifier,
        "WorkspaceClient",
        lambda: SimpleNamespace(
            files=SimpleNamespace(upload=lambda *args, **kwargs: writes.append(args))
        ),
    )
    values = {
        "test_id": "gha_1_1",
        "catalog": "workspace",
        "schema": "pairity_test",
        "fixture_root": str(ROOT / "examples/tuva"),
        "result_table": "workspace.pairity_test.results",
        "artifact_root": "/Volumes/workspace/pairity_test/artifacts",
        "identical_run_id": "1",
        "regression_run_id": "2",
    }
    dbutils = SimpleNamespace(widgets=SimpleNamespace(get=values.__getitem__))
    if corruption:
        with pytest.raises(ValueError):
            verifier.verify(None, dbutils)
        assert not writes
    else:
        verifier.verify(None, dbutils)
        assert len(writes) == 1


def test_service_principal_auth_needs_no_pat_and_reports_missing_secret():
    validate = module("scripts/databricks_preflight.py").validate
    env = {
        "DATABRICKS_HOST": "https://example.databricks.com",
        "DATABRICKS_CLIENT_ID": "test-client",
        "DATABRICKS_CLIENT_SECRET": "private-value",
        "PAIRITY_CATALOG": "workspace",
        "PAIRITY_SCHEMA": "pairity_test",
        "PAIRITY_ARTIFACT_ROOT": "/Volumes/workspace/pairity_test/artifacts",
        "PAIRITY_RESULT_TABLE": "workspace.pairity_test.results",
    }
    validate(env)
    validate(env | {"DATABRICKS_AUTH_TYPE": "oauth-m2m"})
    with pytest.raises(ValueError) as error:
        validate(env | {"DATABRICKS_CLIENT_SECRET": ""})
    assert "DATABRICKS_CLIENT_SECRET" in str(error.value)
    assert "DATABRICKS_TOKEN" not in str(error.value)
    assert "private-value" not in str(error.value)
