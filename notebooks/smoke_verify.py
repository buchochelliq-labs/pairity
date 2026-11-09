# Databricks notebook source
import io
import json
import re
from pathlib import Path

from databricks.sdk import WorkspaceClient

from pairity.config import SuiteConfig
from pairity.delta_storage import DeltaResultStore


def verify_features(report, run_id, expected, test_id, get):
    """Scoping, SQL sources and nested/decimal comparators ran with recorded provenance."""
    from pairity.sql_guard import query_hash

    config = SuiteConfig.model_validate(report["configuration"])
    if (
        report["run_id"] != run_id
        or report["config_hash"] != config.fingerprint
        or report["suite"] != "tuva-features"
        or report["status"] != "failed"
    ):
        raise ValueError("Features report identity or status mismatch")
    if {p["pair_id"] for p in report["pairs"]} != set(expected) or len(report["pairs"]) != len(
        expected
    ):
        raise ValueError("Incomplete features coverage")
    prefix = f"{get('catalog')}.{get('schema')}.{test_id}_"
    configured = {p.id: p for p in config.pairs}
    for pair in report["pairs"]:
        wanted = expected[pair["pair_id"]]
        if pair["status"] != wanted["status"] or pair["counts"] != wanted["counts"]:
            raise ValueError("Unexpected features outcome: " + pair["pair_id"])
        if "column_changes" in wanted and pair["column_changes"] != wanted["column_changes"]:
            raise ValueError("Unexpected column changes: " + pair["pair_id"])
        chosen = {c["column"]: c["comparator"] for c in pair["comparators"]}
        if chosen != wanted["comparators"]:
            raise ValueError("Unexpected comparator selection: " + pair["pair_id"])
        for side, variant in (("baseline", "baseline"), ("candidate", "changed")):
            source = getattr(configured[pair["pair_id"]], side)
            observed = pair["inputs"][side]
            table = prefix + variant + "_core__medical_claim"
            if not observed["snapshot_pinned"]:
                raise ValueError("Features input was not pinned")
            if source.kind == "table":
                # Serverless preparation resolved as_of to the version created by setup.
                if (
                    observed["table"] != table
                    or observed["version"] != 0
                    or source.version != 0
                    or not source.as_of
                    or observed.get("as_of") != source.as_of
                    or observed.get("filters")
                    != [f.model_dump(exclude_none=True) for f in source.where]
                ):
                    raise ValueError("Scoped input provenance mismatch")
            else:
                leaves = observed.get("leaves", [])
                if (
                    observed["table"] != "sql"
                    or [(leaf["table"], leaf["version"]) for leaf in leaves] != [(table, 0)]
                    or observed.get("queries")
                    != [{"query_sha256": query_hash(source.query), "parameters": {"min_line": 1}}]
                ):
                    raise ValueError("SQL source provenance mismatch")
    return report


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
    run_id = get("features_run_id")
    if not run_id.isdigit():
        raise ValueError("Expected a numeric features Job run ID")
    features = json.loads((Path(get("fixture_root")) / "expected-features.json").read_text())
    reports["features"] = verify_features(
        store.load("job:" + run_id)["report"], "job:" + run_id, features, test_id, get
    )
    path = get("artifact_root").rstrip("/") + "/smoke/" + test_id + "/verified-results.json"
    WorkspaceClient().files.upload(path, io.BytesIO(json.dumps(reports).encode()), overwrite=False)
    print("All three Tuva suite reports verified against the committed expectations")


if __name__ == "__main__":
    verify(spark, dbutils)  # noqa: F821
