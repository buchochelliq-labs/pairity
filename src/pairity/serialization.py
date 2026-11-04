"""Strict JSON and JSON-scalar YAML codecs, with no unsafe object construction."""

import json
import math
import re

import yaml

from pairity.config import SuiteConfig


def _unique_mapping(pairs):
    result = {}
    for key, value in pairs:
        if not isinstance(key, str):
            raise ValueError("Configuration mapping keys must be strings")
        if key in result:
            raise ValueError(f"duplicate mapping key: {key}")
        result[key] = value
    return result


class _ConfigLoader(yaml.SafeLoader):
    # Do not modify PyYAML's global resolver tables.
    yaml_implicit_resolvers = {}

    def compose_node(self, parent, index):
        if self.check_event(yaml.AliasEvent):
            raise ValueError("YAML aliases are not supported; use suite defaults")
        return super().compose_node(parent, index)


class _ConfigDumper(yaml.SafeDumper):
    """Quote strings so neither YAML 1.1 nor JSON scalar resolution changes their type."""


_ConfigDumper.add_representer(
    str, lambda dumper, value: dumper.represent_scalar("tag:yaml.org,2002:str", value, style='"')
)


def _construct_mapping(loader, node):
    return _unique_mapping(
        (loader.construct_object(k), loader.construct_object(v)) for k, v in node.value
    )


_ConfigLoader.add_constructor("tag:yaml.org,2002:map", _construct_mapping)
for tag, pattern, first in [
    ("null", r"^(?:null)$", ["n"]),
    ("bool", r"^(?:true|false)$", ["t", "f"]),
    ("int", r"^-?(?:0|[1-9][0-9]*)$", list("-0123456789")),
    (
        "float",
        r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+(?:[eE][+-]?[0-9]+)?|[eE][+-]?[0-9]+)$",
        list("-0123456789"),
    ),
]:
    _ConfigLoader.add_implicit_resolver(f"tag:yaml.org,2002:{tag}", re.compile(pattern), first)


def _check_json_values(value):
    if value is None or type(value) in (str, bool, int):
        return
    if type(value) is float and math.isfinite(value):
        return
    if isinstance(value, list):
        for item in value:
            _check_json_values(item)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("Configuration mapping keys must be strings")
            _check_json_values(item)
        return
    raise ValueError("Configuration must contain only finite JSON-compatible values")


def load_document(text: str, format: str):
    """Load a document without yet assigning a suite or bulk-update schema."""
    try:
        if format.lower() == "json":

            def reject_constant(value):
                raise ValueError(f"Non-finite JSON number: {value}")

            value = json.loads(
                text, object_pairs_hook=_unique_mapping, parse_constant=reject_constant
            )
        elif format.lower() in ("yaml", "yml"):
            value = yaml.load(text, Loader=_ConfigLoader)
        else:
            raise ValueError(f"Unsupported configuration format: {format}")
        _check_json_values(value)
        return value
    except (yaml.YAMLError, RecursionError) as exc:
        raise ValueError("Invalid or excessively nested configuration document") from exc


def load_config(text: str, format: str) -> SuiteConfig:
    return SuiteConfig.model_validate(load_document(text, format))


def dump_config(config: SuiteConfig, format: str) -> str:
    data = config.model_dump(exclude_none=True)
    if format.lower() == "json":
        return json.dumps(data, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
    if format.lower() in ("yaml", "yml"):
        return yaml.dump(data, Dumper=_ConfigDumper, sort_keys=False, allow_unicode=True)
    raise ValueError(f"Unsupported configuration format: {format}")
