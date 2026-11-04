import pytest
from pydantic import ValidationError

from pairity.config import SuiteConfig


def suite_data():
    return {
        "schema_version": 1,
        "suite": "templates",
        "templates": {
            "merge_unique": {
                "parameters": ["first", "second"],
                "source": {
                    "kind": "deduplicate",
                    "source": {
                        "kind": "union",
                        "sources": [
                            {"kind": "parameter", "name": "first"},
                            {"kind": "parameter", "name": "second"},
                        ],
                    },
                },
            }
        },
        "pairs": [
            {
                "id": "orders",
                "baseline": {
                    "kind": "template",
                    "name": "merge_unique",
                    "inputs": {"first": "prod.s.current", "second": "prod.s.archive"},
                },
                "candidate": "dev.s.orders",
                "keys": [{"baseline": "id", "candidate": "id"}],
            }
        ],
    }


def test_parameterized_source_config_validates_and_expands():
    suite = SuiteConfig.model_validate(suite_data())
    from pairity.sources import expand_source

    source = expand_source(suite.pairs[0].baseline, suite.templates)
    assert source.kind == "deduplicate"
    assert [node.table for node in source.source.sources] == ["prod.s.current", "prod.s.archive"]


@pytest.mark.parametrize("case", ["unknown", "missing_input", "cycle", "unbound", "version"])
def test_invalid_template_references_and_version_placement(case):
    data = suite_data()
    if case == "unknown":
        data["pairs"][0]["baseline"]["name"] = "typo"
    elif case == "missing_input":
        del data["pairs"][0]["baseline"]["inputs"]["first"]
    elif case == "cycle":
        data["templates"]["merge_unique"]["source"] = data["pairs"][0]["baseline"]
    elif case == "unbound":
        data["pairs"][0]["baseline"] = {"kind": "parameter", "name": "first"}
    else:
        data["pairs"][0]["baseline_version"] = 1
    with pytest.raises(ValidationError):
        SuiteConfig.model_validate(data)


def test_partial_pair_column_rule_preserves_global_cast_opt_out():
    from pairity.config import resolve_settings

    data = suite_data()
    data["defaults"] = {
        "column_rules": {
            "value": {
                "implicit_casts": False,
                "compare_as": "decimal(20,2)",
                "absolute_tolerance": 0.1,
            }
        }
    }
    data["pairs"][0]["compare"] = {"column_rules": {"value": {"null_safe": False}}}
    suite = SuiteConfig.model_validate(data)
    # Persistence and engine freezing must preserve the same semantics.
    suite = SuiteConfig.model_validate(suite.model_dump(exclude_none=True))
    rule = resolve_settings(suite, suite.pairs[0]).column_rules["value"]
    assert rule.implicit_casts is False
    assert rule.compare_as == "decimal(20,2)"
    assert rule.absolute_tolerance == 0.1
    assert rule.null_safe is False
