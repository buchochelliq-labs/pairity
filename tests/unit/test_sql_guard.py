import copy
import json
import re

import pytest
from pydantic import ValidationError

from pairity.config import SuiteConfig
from pairity.sources import expand_source, leaf_tables
from pairity.sql_guard import UnsafeQuery, check_query, render


def suite(source=None, templates=None):
    return {
        "schema_version": 1,
        "suite": "sql",
        "templates": templates or {},
        "pairs": [
            {
                "id": "orders",
                "baseline": source
                or {
                    "kind": "sql",
                    "query": "SELECT o.id, o.amount FROM {{orders}} o "
                    "JOIN {{customers}} c ON o.customer_id = c.id WHERE o.day >= :start",
                    "inputs": {
                        "orders": "prod.s.orders",
                        "customers": {"kind": "table", "table": "prod.s.customers", "version": 4},
                    },
                    "parameters": {"start": "2026-01-01"},
                },
                "candidate": "dev.s.orders",
                "keys": [{"baseline": "id", "candidate": "id"}],
            }
        ],
    }


@pytest.mark.parametrize(
    "query",
    [
        "SELECT * FROM {{a}}",
        "select id from {{a}} where x = :p",
        "WITH t AS (SELECT * FROM {{a}}) SELECT * FROM t JOIN t AS u USING (id)",
        "SELECT * FROM {{a}} UNION ALL SELECT * FROM {{a}}",
        "SELECT DISTINCT * FROM (SELECT * FROM {{a}}) x",
        "SELECT extract(YEAR FROM d), trim(BOTH ' ' FROM s), substring(s FROM 2) FROM {{a}}",
        "SELECT * FROM {{a}} WHERE id IN (SELECT id FROM {{a}} WHERE ok)",
        "SELECT raw:field.x, n::int, m['k'], s.update FROM {{a}} s",
        "SELECT ';DROP TABLE x' AS s, 'it''s' AS t FROM {{a}} -- DROP; DELETE",
        "/* hint */ SELECT * FROM {{a}};",
        "SELECT * FROM ((SELECT * FROM {{a}})) x",
        "SELECT * FROM {{a}} a LATERAL VIEW explode(a.items) e AS item",
        "SELECT `set`, `user` FROM {{a}}",
        "SELECT * FROM (WITH t AS (SELECT * FROM {{a}}) SELECT * FROM t) x",
        "SELECT transform(xs, x -> x + 1) FROM {{a}}",
    ],
)
def test_supported_read_only_queries(query):
    checked = check_query(query)
    assert checked.inputs == ("a",)


@pytest.mark.parametrize(
    ("query", "reason"),
    [
        ("DROP TABLE x", "single SELECT or WITH"),
        ("INSERT INTO t SELECT * FROM {{a}}", "single SELECT or WITH"),
        ("SELECT 1 FROM {{a}}; DELETE FROM t", "exactly one statement"),
        ("SELECT * FROM {{a}} WHERE x IN (DELETE FROM t)", "DELETE is not allowed"),
        ("WITH t AS (SELECT * FROM {{a}}) MERGE INTO x USING t", "MERGE is not allowed"),
        ("SELECT * FROM prod.s.orders", "declared input placeholder"),
        ("SELECT * FROM orders", "not a declared input"),
        ("SELECT * FROM `orders`", "not a declared input"),
        ("SELECT * FROM {{a}}, other", "not a declared input"),
        ("SELECT * FROM {{a}} JOIN other ON true", "not a declared input"),
        ("SELECT * FROM {{a}} WHERE EXISTS (SELECT 1 FROM other)", "not a declared input"),
        ("WITH x AS (SELECT * FROM y), y AS (SELECT * FROM {{a}}) SELECT * FROM x", "y is not"),
        ("WITH t AS (SELECT * FROM t) SELECT * FROM t", "t is not"),
        ("SELECT * FROM (WITH t AS (SELECT * FROM {{a}}) SELECT * FROM t) x JOIN t", "t is not"),
        ("SELECT * FROM (SELECT * FROM {{a}}) x JOIN x", "x is not"),
        ("SELECT * FROM range(10)", "Table-valued function"),
        ("SELECT * FROM read_files('/Volumes/x')", "read_files() is not allowed"),
        ("SELECT secret('scope', 'key') FROM {{a}}", "secret() is not allowed"),
        ("SELECT * FROM {{a}} VERSION AS OF 3", "Time travel"),
        ("SELECT * FROM {{a}} TIMESTAMP AS OF '2026-01-01'", "Time travel"),
        ("SELECT * FROM {{a}}@v3", "'@' is not supported"),
        ("SELECT * FROM {{a}} WHERE d = '${day}' OR d = ${day}", "'$' is not supported"),
        ("SELECT * FROM {{a}} WHERE d = ?", "'?' is not supported"),
        ("SELECT * FROM {a}", "'{' is not supported"),
        ("SELECT rand() AS r FROM {{a}}", "rand() is not allowed"),
        ("SELECT current_timestamp FROM {{a}}", "CURRENT_TIMESTAMP is not allowed"),
        ("SELECT * FROM {{a}} TABLESAMPLE (10 PERCENT)", "TABLESAMPLE"),
        ("SELECT * FROM IDENTIFIER(:t)", "IDENTIFIER"),
        ("SELECT * FROM {{a}} WHERE x = : p", "must be :name"),
        ("SELECT 'unterminated FROM {{a}}", "unterminated"),
        ("SELECT (1 FROM {{a}}", "Unbalanced"),
        ("SELECT * FROM", "relation is expected"),
        ("", "requires a query"),
    ],
)
def test_unsafe_or_unsupported_queries_are_rejected(query, reason):
    with pytest.raises(UnsafeQuery, match=re.escape(reason)):
        check_query(query)


def test_parameter_markers_skip_strings_casts_and_paths():
    checked = check_query(
        "SELECT ':no', raw:path, x::int FROM {{a}} WHERE d BETWEEN :start AND :end OR e = :start"
    )
    assert checked.parameters == ("start", "end")


def test_render_replaces_only_placeholders_and_is_embeddable():
    query = "SELECT '{{a}}' AS literal FROM {{ a }} x; -- trailing"
    assert render(query, {"a": "`view`"}) == "SELECT '{{a}}' AS literal FROM `view` x -- trailing\n"


def test_sql_source_config_validates_and_expands_inputs_in_order():
    config = SuiteConfig.model_validate(suite())
    source = expand_source(config.pairs[0].baseline, config.templates)
    assert [(leaf.table, leaf.version) for leaf in leaf_tables(source)] == [
        ("prod.s.orders", None),
        ("prod.s.customers", 4),
    ]


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda s: s["inputs"].pop("customers"), "placeholders"),
        (lambda s: s["inputs"].update(extra="prod.s.x"), "placeholders"),
        (lambda s: s["parameters"].clear(), "parameter markers"),
        (lambda s: s["parameters"].update(other=1), "parameter markers"),
        (
            lambda s: (
                s.update(query=s["query"] + " AND :api_token = 1"),
                s["parameters"].update(api_token="x"),
            ),
            "credential",
        ),
        (lambda s: s.update(query="SELECT * FROM prod.s.orders"), "declared input"),
        (lambda s: s["parameters"].update(start=float("nan")), "finite"),
        (lambda s: s["parameters"].update(start=["2026"]), "valid"),
        (lambda s: s["inputs"].update({"bad-name": "prod.s.x"}), "pattern"),
        (lambda s: s.update(inputs={}, query="SELECT 1"), "at least 1"),
    ],
)
def test_sql_source_configuration_errors(change, message):
    data = suite()
    change(data["pairs"][0]["baseline"])
    with pytest.raises(ValidationError, match=message):
        SuiteConfig.model_validate(data)


def template_suite():
    return suite(
        source={
            "kind": "template",
            "name": "since",
            "inputs": {"table": "prod.s.orders"},
            "values": {"start": "2026-02-01"},
        },
        templates={
            "since": {
                "parameters": ["table"],
                "values": ["start"],
                "source": {
                    "kind": "sql",
                    "query": "SELECT * FROM {{t}} WHERE day >= :start",
                    "inputs": {"t": {"kind": "parameter", "name": "table"}},
                    "parameters": {"start": {"kind": "value", "name": "start"}},
                },
            }
        },
    )


def test_query_templates_bind_sources_and_scalar_values():
    config = SuiteConfig.model_validate(template_suite())
    source = expand_source(config.pairs[0].baseline, config.templates)
    assert source.kind == "sql"
    assert source.parameters == {"start": "2026-02-01"}
    assert source.inputs["t"].table == "prod.s.orders"


@pytest.mark.parametrize("case", ["missing", "extra", "unbound"])
def test_query_template_value_errors(case):
    data = template_suite()
    if case == "missing":
        data["pairs"][0]["baseline"]["values"] = {}
    elif case == "extra":
        data["pairs"][0]["baseline"]["values"]["other"] = 1
    else:
        data["pairs"][0]["baseline"] = copy.deepcopy(data["templates"]["since"]["source"])
        data["pairs"][0]["baseline"]["inputs"]["t"] = "prod.s.orders"
    with pytest.raises(ValidationError, match="values|Unbound template value"):
        SuiteConfig.model_validate(data)


@pytest.mark.parametrize("fmt", ["json", "yaml"])
def test_sql_sources_roundtrip_and_validate_against_schema(fmt):
    from jsonschema import Draft202012Validator

    from pairity.schema import configuration_schema
    from pairity.serialization import dump_config, load_config, load_document

    config = SuiteConfig.model_validate(template_suite())
    text = dump_config(config, fmt)
    assert load_config(text, fmt) == config
    Draft202012Validator(configuration_schema()).validate(load_document(text, fmt))


def test_app_authorization_covers_every_sql_input_table():
    from pairity.web.storage import source_tables

    config = SuiteConfig.model_validate(suite())
    assert source_tables(config) == ["dev.s.orders", "prod.s.customers", "prod.s.orders"]


def test_serverless_preparation_pins_sql_inputs_once():
    from pairity.serverless import prepare_config

    calls = []

    def latest(table):
        calls.append(table)
        return 11

    prepared = prepare_config(SuiteConfig.model_validate(suite()), latest)
    inputs = prepared.pairs[0].baseline.inputs
    assert (inputs["orders"].version, inputs["customers"].version) == (11, 4)
    assert sorted(calls) == ["dev.s.orders", "prod.s.orders"]


def incident_report(queries):
    from pairity.results import PairResult, RunResult
    from pairity.sql_guard import query_hash

    config = SuiteConfig.model_validate(suite())
    source = config.pairs[0].baseline
    recorded = [{"query_sha256": query_hash(source.query), "parameters": source.parameters}]
    pair = PairResult(
        "orders",
        "failed",
        counts={
            "baseline_rows": 2,
            "candidate_rows": 2,
            "matched_rows": 2,
            "changed_rows": 1,
            "missing_rows": 0,
            "extra_rows": 0,
        },
        inputs={
            "baseline": {
                "table": "sql",
                "version": None,
                "snapshot_pinned": True,
                "leaves": [
                    {"table": "prod.s.orders", "version": 9, "snapshot_pinned": True},
                    {"table": "prod.s.customers", "version": 4, "snapshot_pinned": True},
                ],
                "queries": queries(recorded),
            },
            "candidate": {"table": "dev.s.orders", "version": 2, "snapshot_pinned": True},
        },
        keys=[{"baseline": "id", "candidate": "id"}],
    )
    return RunResult(
        "run", "sql", config.fingerprint, config.model_dump(exclude_none=True), [pair], "s", "e"
    ).to_dict()


def test_incident_export_pins_sql_inputs_and_renders_reproducible_query():
    from pairity.incidents import IncidentExporter

    files = IncidentExporter().export(incident_report(lambda recorded: recorded))
    manifest = json.loads(files["manifest.json"])
    assert manifest["reproduction"]["ready"]
    resolved = json.loads(files["resolved.json"])["pairs"][0]["baseline"]
    assert resolved["inputs"]["orders"]["version"] == 9
    sql = files["reproduce.sql"].decode()
    assert "`pairity_input_orders` AS (SELECT * FROM `prod`.`s`.`orders` VERSION AS OF 9)" in sql
    assert "FROM `pairity_input_orders` o JOIN `pairity_input_customers` c" in sql
    assert '-- Bind named parameters: {"start": "2026-01-01"}' in sql
    assert "{{" not in sql


def test_incident_export_rejects_mismatched_recorded_query():
    from pairity.incidents import IncidentExporter

    files = IncidentExporter().export(
        incident_report(lambda recorded: [recorded[0] | {"query_sha256": "0" * 64}])
    )
    assert not json.loads(files["manifest.json"])["reproduction"]["ready"]
