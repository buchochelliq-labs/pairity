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
