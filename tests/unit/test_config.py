import json

import pytest

from pairity.config import SuiteConfig, resolve_settings
from pairity.serialization import dump_config, load_config


def suite_data():
    return {
        "schema_version": 1,
        "suite": "release",
        "pairs": [
            {"id": "orders", "baseline": "prod.sales.orders", "candidate": "dev.sales.orders"}
        ],
    }


def test_json_yaml_have_identical_values_and_fingerprints():
    data = suite_data()
    data["pairs"][0]["tags"] = ["on", "2026-11-04", "01"]
    original = load_config(json.dumps(data), "json")
    for format in ("json", "yaml", "yml"):
        converted = load_config(dump_config(original, format), format)
        assert converted == original
        assert converted.fingerprint == original.fingerprint
    assert original.execution.max_parallel_pairs == 4


def test_yaml_uses_json_scalar_rules_not_yaml_11_dates_or_booleans():
    config = load_config(
        """schema_version: 1
suite: on
pairs:
  - id: orders
    baseline: prod.sales.orders
    candidate: dev.sales.orders
    tags: [yes, off, 2026-11-04, 01]
""",
        "yaml",
    )
    assert config.suite == "on"
    assert config.pairs[0].tags == ["yes", "off", "2026-11-04", "01"]


@pytest.mark.parametrize(
    "format,text",
    [
        ("json", '{"schema_version":1,"schema_version":1}'),
        ("yaml", "schema_version: 1\nschema_version: 1"),
        ("yaml", "!!python/object/apply:os.system ['echo forbidden']"),
        ("yaml", "x: &x [*x]"),
        ("json", '{"value": NaN}'),
        ("yaml", "suite: release\n1: value"),
    ],
)
def test_rejects_ambiguous_or_unsafe_documents(format, text):
    with pytest.raises(ValueError):
        load_config(text, format)


@pytest.mark.parametrize(
    "change",
    [
        {"schema_version": 2},
        {"schema_version": True},
        {"surprise": 1},
        {"execution": {"max_parallel_pairs": 0}},
        {"execution": {"max_parallel_pairs": "4"}},
        {"execution": {"max_parallel_pairs": True}},
        {"execution": {"max_parallel_pairs": 65}},
        {"defaults": {"max_changed_rows": -1}},
        {"defaults": {"column_rules": {"price": {"absolute_tolerance": -0.1}}}},
        {"defaults": {"column_rules": {"price": {"absolute_tolerance": float("inf")}}}},
    ],
)
def test_rejects_invalid_config(change):
    with pytest.raises(ValueError):
        SuiteConfig.model_validate(suite_data() | change)


def test_rejects_duplicate_pair_ids_and_key_targets():
    data = suite_data()
    data["pairs"] *= 2
    with pytest.raises(ValueError, match="duplicate"):
        SuiteConfig.model_validate(data)
    data = suite_data()
    data["pairs"][0]["keys"] = [
        {"baseline": "a", "candidate": "x"},
        {"baseline": "b", "candidate": "x"},
    ]
    with pytest.raises(ValueError, match="duplicate"):
        SuiteConfig.model_validate(data)


def test_overrides_replace_lists_and_merge_column_rules():
    data = suite_data()
    data["defaults"] = {
        "exclude_columns": ["audit"],
        "column_rules": {
            "price": {"absolute_tolerance": 0.01},
            "tax": {"absolute_tolerance": 0.02},
        },
    }
    data["pairs"][0]["compare"] = {
        "exclude_columns": [],
        "column_rules": {"price": {"absolute_tolerance": 0.1}},
    }
    config = SuiteConfig.model_validate(data)
    effective = resolve_settings(config, config.pairs[0])
    assert effective.exclude_columns == []
    assert effective.column_rules["price"].absolute_tolerance == 0.1
    assert effective.column_rules["tax"].absolute_tolerance == 0.02
    assert config.defaults.exclude_columns == ["audit"]


def test_empty_explicit_keys_rejected_instead_of_discovery_fallback():
    data = suite_data()
    data["pairs"][0]["keys"] = []
    with pytest.raises(ValueError):
        SuiteConfig.model_validate(data)
