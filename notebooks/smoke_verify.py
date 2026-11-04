# Databricks notebook source
import io
import json
import re
from pathlib import Path

from databricks.sdk import WorkspaceClient

from pairity.config import SuiteConfig
from pairity.delta_storage import DeltaResultStore


def verify(spark, dbutils):
    get = dbutils.widgets.get
    test_id = get("test_id")
    if not re.fullmatch(r"gha_[0-9]+_[0-9]+", test_id):
        raise ValueError("Invalid test identity")
    store = DeltaResultStore(spark, get("result_table"))
    expected = json.loads((Path(get("fixture_root")) / "expected.json").read_text())
    reports = {}
    for variant in ("identical", "regression"):
        run_id = get(variant + "_run_id")
        if not run_id.isdigit():
            raise ValueError("Expected a numeric Job run ID")
        report = store.load("job:" + run_id)["report"]
        config = SuiteConfig.model_validate(report["configuration"])
        if (
            report["run_id"] != "job:" + run_id
            or report["config_hash"] != config.fingerprint
            or report["suite"] != "tuva-" + variant
        ):
            raise ValueError("Report identity mismatch")
        pairs = report["pairs"]
        if len(pairs) != len(expected) or {p["pair_id"] for p in pairs} != set(expected):
            raise ValueError("Incomplete or duplicate comparison coverage")
        configured = {p["id"]: p for p in report["configuration"]["pairs"]}
        for pair in pairs:
            wanted = expected[pair["pair_id"]].copy()
            if variant == "identical":
                wanted.update(
                    candidate_rows=wanted["baseline_rows"],
                    matched_rows=wanted["baseline_rows"],
                    changed_rows=0,
                    missing_rows=0,
                    extra_rows=0,
                )
            failed = any(wanted[k] for k in ("changed_rows", "missing_rows", "extra_rows"))
            if pair["counts"] != wanted or pair["status"] != ("failed" if failed else "passed"):
                raise ValueError("Unexpected Tuva outcome: " + pair["pair_id"])
            for side in ("baseline", "candidate"):
                target = configured[pair["pair_id"]][side]
                table = target if isinstance(target, str) else target["table"]
                source_variant = (
                    "baseline" if side == "baseline" or variant == "identical" else "changed"
                )
                expected_table = f"{test_id}_{source_variant}_{pair['pair_id']}"
                expected_table = f"{get('catalog')}.{get('schema')}.{expected_table}"
                if table != expected_table:
                    raise ValueError("Report belongs to another fixture")
                observed = pair["inputs"][side]
                version = target.get("version") if isinstance(target, dict) else None
                if (
                    type(version) is not int
                    or version < 0
                    or not observed["snapshot_pinned"]
                    or observed["table"] != table
                    or observed["version"] != version
                ):
                    raise ValueError("Input snapshot provenance does not match the pinned config")
        if report["status"] != ("passed" if variant == "identical" else "failed"):
            raise ValueError("Unexpected suite status")
        reports[variant] = report
    path = get("artifact_root").rstrip("/") + "/smoke/" + test_id + "/verified-results.json"
    WorkspaceClient().files.upload(path, io.BytesIO(json.dumps(reports).encode()), overwrite=False)
    print("Both Tuva suite reports verified against the committed expected counts")


if __name__ == "__main__":
    verify(spark, dbutils)  # noqa: F821
