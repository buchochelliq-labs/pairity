import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from pairity.schema import configuration_schema
from pairity.serialization import load_document


def test_schema_is_valid_current_and_accepts_both_examples():
    schema = configuration_schema()
    Draft202012Validator.check_schema(schema)
    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert json.loads(Path("schema/pairity.schema.json").read_text()) == schema
    for name in ("suite", "templates", "sql-sources"):
        for extension in ("json", "yaml"):
            data = load_document(Path(f"examples/{name}.{extension}").read_text(), extension)
            Draft202012Validator(schema).validate(data)


@pytest.mark.parametrize(
    "patch",
    [
        {"schema_version": 2},
        {"unexpected": True},
        {"execution": {"max_parallel_pairs": 0}},
        {"execution": {"max_parallel_pairs": "4"}},
        {
            "pairs": [
                {
                    "id": "x",
                    "baseline": "a.b.c",
                    "candidate": "d.e.f",
                    "compare": {"exclude_columns": None},
                }
            ]
        },
    ],
)
def test_schema_rejects_invalid_documents(patch):
    data = {
        "schema_version": 1,
        "suite": "test",
        "pairs": [{"id": "x", "baseline": "a.b.c", "candidate": "d.e.f"}],
    } | patch
    assert list(Draft202012Validator(configuration_schema()).iter_errors(data))
