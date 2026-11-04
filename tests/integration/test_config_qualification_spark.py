"""Execute the identical notebook harness against real local Spark."""

import importlib.util
from pathlib import Path

import pytest

pytestmark = pytest.mark.spark


def test_config_qualification_harness(spark):
    path = Path(__file__).resolve().parents[2] / "notebooks/config_qualification.py"
    spec = importlib.util.spec_from_file_location("config_qualification_integration", path)
    qualification = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(qualification)
    evidence = qualification.run_qualification(spark)
    assert len(evidence["cases"]) == 15
    assert evidence["denied_persistence"] == {"adapter": "controlled", "propagated": True}
