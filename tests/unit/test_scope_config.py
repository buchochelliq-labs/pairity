import json

import pytest
from pydantic import ValidationError

from pairity.config import SuiteConfig, TableSource, parse_instant


def table(**fields):
    return {"kind": "table", "table": "prod.s.orders", **fields}


def suite(baseline):
    return {
        "schema_version": 1,
        "suite": "scoped",
        "pairs": [
            {
                "id": "orders",
                "baseline": baseline,
                "candidate": "dev.s.orders",
                "keys": [{"baseline": "id", "candidate": "id"}],
            }
        ],
    }


@pytest.mark.parametrize(
    "value", ["2026-01-01T00:00:00Z", "2026-01-01T09:00:00+09:00", "2026-01-01T00:00:00.123456Z"]
)
def test_as_of_accepts_instants_with_offsets(value):
    TableSource.model_validate(table(as_of=value))


def test_as_of_normalizes_to_utc():
    assert parse_instant("2026-01-01T09:00:00+09:00") == parse_instant("2026-01-01T00:00:00Z")


@pytest.mark.parametrize("value", ["2026-01-01", "2026-01-01T00:00:00", "yesterday", ""])
def test_as_of_rejects_naive_or_invalid_values(value):
    with pytest.raises(ValidationError):
        TableSource.model_validate(table(as_of=value))


@pytest.mark.parametrize(
    "item",
    [
        {"column": "day", "op": "=", "value": "2026-01-01"},
        {"column": "day", "op": ">=", "value": 3},
        {"column": "region", "op": "in", "value": ["EU", "US"]},
        {"column": "region", "op": "not_in", "value": ["XX"]},
        {"column": "amount", "op": "between", "value": [1, 2.5]},
        {"column": "deleted_at", "op": "is_null"},
        {"column": "flag", "op": "!=", "value": True},
    ],
)
def test_filters_accept_operator_value_shapes(item):
    TableSource.model_validate(table(where=[item]))


@pytest.mark.parametrize(
    "item",
    [
        {"column": "day", "op": "=", "value": None},
        {"column": "day", "op": "=", "value": ["a"]},
        {"column": "day", "op": "like", "value": "a%"},
        {"column": "day", "op": "in", "value": []},
        {"column": "day", "op": "in", "value": "EU"},
        {"column": "day", "op": "in", "value": list(range(1001))},
        {"column": "day", "op": "between", "value": [1]},
        {"column": "day", "op": "is_null", "value": 1},
        {"column": "day", "op": "=", "value": float("nan")},
        {"column": "", "op": "is_null"},
        {"column": "day", "op": "=", "value": {"sql": "1=1"}},
    ],
)
def test_filters_reject_invalid_shapes(item):
    with pytest.raises(ValidationError):
        TableSource.model_validate(table(where=[item]))


def test_scoped_sources_roundtrip_and_validate_against_schema():
    from jsonschema import Draft202012Validator

    from pairity.schema import configuration_schema
    from pairity.serialization import dump_config, load_config, load_document

    config = SuiteConfig.model_validate(
        suite(
            table(
                as_of="2026-01-01T00:00:00Z",
                where=[{"column": "day", "op": "between", "value": ["2026-01-01", "2026-01-31"]}],
            )
        )
    )
    for fmt in ("json", "yaml"):
        text = dump_config(config, fmt)
        assert load_config(text, fmt) == config
        Draft202012Validator(configuration_schema()).validate(load_document(text, fmt))


def test_serverless_preparation_resolves_as_of_once_and_keeps_it_for_rechecks():
    from pairity.serverless import prepare_config

    calls = []

    def version_at(name, as_of):
        calls.append((name, as_of))
        return 3

    scoped = table(as_of="2026-01-01T00:00:00Z")
    data = suite({"kind": "union", "sources": [scoped, scoped]})
    prepared = prepare_config(SuiteConfig.model_validate(data), lambda name: 9, version_at)
    sources = prepared.pairs[0].baseline.sources
    assert [(s.version, s.as_of) for s in sources] == [(3, "2026-01-01T00:00:00Z")] * 2
    assert calls == [("prod.s.orders", "2026-01-01T00:00:00Z")]
    assert prepared.pairs[0].candidate.version == 9


def test_serverless_preparation_requires_an_as_of_resolver():
    from pairity.serverless import prepare_config

    config = SuiteConfig.model_validate(suite(table(as_of="2026-01-01T00:00:00Z")))
    with pytest.raises(ValueError, match="version_at"):
        prepare_config(config, lambda name: 1)


def report(leaf_extra):
    from pairity.results import PairResult, RunResult

    config = SuiteConfig.model_validate(
        suite(
            table(
                as_of="2026-01-01T00:00:00Z",
                where=[
                    {"column": "region", "op": "in", "value": ["EU", "O'Hare\\x"]},
                    {"column": "day", "op": ">=", "value": "2026-01-01"},
                    {"column": "deleted_at", "op": "is_null"},
                ],
            )
        )
    )
    filters = [f.model_dump(exclude_none=True) for f in config.pairs[0].baseline.where]
    leaf = {
        "table": "prod.s.orders",
        "version": 3,
        "snapshot_pinned": True,
        "as_of": "2026-01-01T00:00:00Z",
        "filters": filters,
    } | leaf_extra
    pair = PairResult(
        "orders",
        "failed",
        counts={
            "baseline_rows": 1,
            "candidate_rows": 1,
            "matched_rows": 1,
            "changed_rows": 1,
            "missing_rows": 0,
            "extra_rows": 0,
        },
        inputs={
            "baseline": leaf,
            "candidate": {"table": "dev.s.orders", "version": 2, "snapshot_pinned": True},
        },
        keys=[{"baseline": "id", "candidate": "id"}],
    )
    return RunResult(
        "run", "scoped", config.fingerprint, config.model_dump(exclude_none=True), [pair], "s", "e"
    ).to_dict()


def test_incident_reproduction_renders_pinned_version_and_escaped_filters():
    from pairity.incidents import IncidentExporter

    files = IncidentExporter().export(report({}))
    assert json.loads(files["manifest.json"])["reproduction"]["ready"]
    sql = files["reproduce.sql"].decode()
    assert (
        "SELECT * FROM `prod`.`s`.`orders` VERSION AS OF 3 WHERE `region` IN ('EU', "
        "'O\\'Hare\\\\x') AND `day` >= '2026-01-01' AND `deleted_at` IS NULL"
    ) in sql


@pytest.mark.parametrize(
    "changed", [{"as_of": "2026-02-01T00:00:00Z"}, {"filters": []}, {"as_of": None}]
)
def test_incident_reproduction_rejects_mismatched_scope(changed):
    from pairity.incidents import IncidentExporter

    files = IncidentExporter().export(report(changed))
    assert not json.loads(files["manifest.json"])["reproduction"]["ready"]
