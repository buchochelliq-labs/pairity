import importlib.util
import json
from pathlib import Path

import pytest

from pairity.config import SuiteConfig, resolve_settings
from pairity.results import TableInput
from pairity.serialization import load_config
from pairity.spark import SparkComparisonBackend

ROOT = Path(__file__).resolve().parents[2] / "examples" / "tuva"


def module(name):
    spec = importlib.util.spec_from_file_location(f"tuva_{name}", ROOT / f"{name}.py")
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def test_generated_files_are_reproducible_and_configs_equivalent(tmp_path):
    module("generate").generate(tmp_path)
    for generated in tmp_path.rglob("*"):
        if generated.is_file():
            assert generated.read_bytes() == (ROOT / generated.relative_to(tmp_path)).read_bytes()
    for name in ("tuva-regression", "tuva-identical"):
        assert load_config((ROOT / f"{name}.json").read_text(), "json") == load_config(
            (ROOT / f"{name}.yaml").read_text(), "yaml"
        )


@pytest.mark.spark
def test_tuva_expected_differences_and_identical_controls(spark):
    data = module("load").frames(spark)
    expected = json.loads((ROOT / "expected.json").read_text())
    config = SuiteConfig.model_validate(json.loads((ROOT / "tuva-regression.json").read_text()))
    backend = SparkComparisonBackend()
    for pair in config.pairs:
        left = TableInput(data[("baseline", pair.id)], pair.baseline)
        right = TableInput(data[("changed", pair.id)], pair.candidate)
        result = backend.compare(pair, resolve_settings(config, pair), pair.keys, left, right)
        assert result.counts == expected[pair.id]
        assert result.status == ("passed" if pair.id == "core__pharmacy_claim" else "failed")
        assert result.differences.count() == sum(
            expected[pair.id][key] for key in ("changed_rows", "missing_rows", "extra_rows")
        )
        assert (
            backend.compare(pair, resolve_settings(config, pair), pair.keys, left, left).status
            == "passed"
        )


def test_features_suite_formats_agree_and_loader_pins_as_of():
    assert load_config((ROOT / "tuva-features.json").read_text(), "json") == load_config(
        (ROOT / "tuva-features.yaml").read_text(), "yaml"
    )
    targets = {
        (variant, "core__medical_claim"): f"c.s.t_{variant}_core__medical_claim"
        for variant in ("baseline", "changed")
    }
    document = json.loads((ROOT / "tuva-features.json").read_text())
    config = module("load").features_config(document, targets, "2026-01-01T00:00:00Z")
    scoped, nested = config.pairs
    assert (scoped.baseline.table, scoped.baseline.as_of) == (
        "c.s.t_baseline_core__medical_claim",
        "2026-01-01T00:00:00Z",
    )
    # SQL inputs stay plain table names: preparation pins them by version, not as_of.
    assert nested.candidate.inputs["claims"] == "c.s.t_changed_core__medical_claim"


def features_report(spark):
    from contextlib import nullcontext

    from pairity.engine import ComparisonRunner, PairityEngine
    from pairity.serverless import prepare_config
    from pairity.spark_sources import SparkSourceResolver

    data = module("load").frames(spark)
    targets = {key: f"cat.sch.gha_1_1_{key[0]}_{key[1]}" for key in data}
    frames = {targets[key]: frame for key, frame in data.items()}
    document = json.loads((ROOT / "tuva-features.json").read_text())
    config = module("load").features_config(document, targets, "2026-01-01T00:00:00Z")
    # Mirror the serverless Job: preparation pins every table, then the engine runs.
    prepared = prepare_config(config, lambda table: 0, lambda table, as_of: 0)

    class Reader:
        api_version = 1

        def scope(self, *args):
            return nullcontext()

        def read(self, table, version=None, *, as_of=None):
            details = {"as_of": as_of} if as_of else {}
            return TableInput(frames[table], table, version, snapshot_pinned=True, details=details)

    class Metadata:
        api_version = 1

        def primary_key(self, table):
            return None

    engine = PairityEngine(
        lambda: ComparisonRunner(
            Reader(), Metadata(), SparkComparisonBackend(), source_resolver=SparkSourceResolver()
        )
    )
    return engine.run(prepared, run_id="job:7").to_dict()


@pytest.mark.spark
def test_features_suite_matches_committed_expectations_and_verifier(spark):
    import runpy

    report = features_report(spark)
    expected = json.loads((ROOT / "expected-features.json").read_text())
    for pair in report["pairs"]:
        assert pair["counts"] == expected[pair["pair_id"]]["counts"]
        assert pair["status"] == expected[pair["pair_id"]]["status"]
    verify = runpy.run_path(str(ROOT.parents[1] / "notebooks" / "smoke_verify.py"))[
        "verify_features"
    ]
    widgets = {"catalog": "cat", "schema": "sch"}
    assert verify(report, "job:7", expected, "gha_1_1", widgets.get) is report
    tampered = json.loads(json.dumps(report))
    tampered["pairs"][1]["inputs"]["baseline"]["queries"][0]["parameters"] = {"min_line": 2}
    with pytest.raises(ValueError, match="SQL source provenance"):
        verify(tampered, "job:7", expected, "gha_1_1", widgets.get)
    tampered = json.loads(json.dumps(report))
    tampered["pairs"][0]["inputs"]["candidate"]["as_of"] = None
    with pytest.raises(ValueError, match="Scoped input provenance"):
        verify(tampered, "job:7", expected, "gha_1_1", widgets.get)
