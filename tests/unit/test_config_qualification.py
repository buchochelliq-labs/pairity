"""Guard notebook inputs and qualification coverage without requiring Spark."""

import importlib.util
from pathlib import Path

import pytest

from pairity.serialization import dump_config, load_config

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "config_qualification", ROOT / "notebooks/config_qualification.py"
)
qualification = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qualification)


def test_every_case_roundtrips_both_codecs():
    cases = qualification.qualification_cases()
    assert len(cases) == 15
    assert len({config.pairs[0].id for config, _, _ in cases}) == 15
    for config, _, _ in cases:
        for codec in ("json", "yaml"):
            assert load_config(dump_config(config, codec), codec) == config
    assert qualification.schema_validation_evidence()["rejected_documents"] == 6


@pytest.mark.parametrize(
    "root,identity",
    [
        ("/tmp/results", "gha_1_1"),
        ("/Volumes/c/s/v/../escape", "gha_1_1"),
        ("/Volumes/c/s/v", "../other"),
        ("/Volumes/c/s", "gha_1_1"),
    ],
)
def test_artifact_identity_and_volume_path_are_validated(root, identity):
    with pytest.raises(ValueError):
        qualification.artifact_path(root, identity)


def test_evidence_path_is_run_scoped():
    assert qualification.artifact_path("/Volumes/c/s/v/", "gha_12_2") == (
        "/Volumes/c/s/v/smoke/gha_12_2/config-qualification.json"
    )
