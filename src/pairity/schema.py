"""Portable JSON Schema for decoded JSON and YAML suite documents."""

from pairity.config import SuiteConfig


def configuration_schema() -> dict:
    schema = SuiteConfig.model_json_schema()
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["$id"] = (
        "https://github.com/buchochelliq-labs/pairity/raw/main/schema/pairity.schema.json"
    )
    schema["title"] = "Pairity regression suite configuration v1"
    schema["description"] = (
        "Validates decoded JSON or YAML. Run pairity validate for cross-field uniqueness "
        "and semantic checks; table and PK availability are checked at execution time."
    )
    # None is an internal inheritance sentinel, never an accepted explicit override.
    for field in schema["$defs"]["ComparisonOverrides"]["properties"].values():
        variants = [item for item in field.pop("anyOf") if item.get("type") != "null"]
        field.update(variants[0])
        field.pop("default", None)
    return schema
