import json
from copy import deepcopy

import pytest
from fastapi.testclient import TestClient

from pairity.web.app import create_app
from pairity.workbench import FileSuiteCatalog, Workbench


def document(**pair):
    return {
        "schema_version": 1,
        "suite": "test",
        "pairs": [{"id": "p", "baseline": "cat.sch.left", "candidate": "cat.sch.right", **pair}],
    }


class Discovery:
    api_version = 1

    def __init__(self):
        self.sources = {
            "cat.sch.left": {
                "columns": [
                    {"name": "id", "type": "STRING", "nullable": False},
                    {"name": "version", "type": "INT", "nullable": False},
                ],
                "primary_key": ["id", "version"],
            },
            "cat.sch.right": {
                "columns": [
                    {"name": "id", "type": "BIGINT", "nullable": False},
                    {"name": "version", "type": "INT", "nullable": False},
                ],
                "primary_key": ["version", "id"],
            },
        }

    def describe(self, table):
        return {"table": table, **deepcopy(self.sources[table])}

    def catalogs(self, **kwargs):
        return {"items": [{"name": "cat"}], "next_after": None}


def inspect(tmp_path, config=None, discovery=None):
    service = Workbench(FileSuiteCatalog(tmp_path), discovery=discovery or Discovery())
    return service.inspect_pair(json.dumps(config or document()), "json", "p")


def test_declared_composite_keys_are_suggested_and_casts_resolve(tmp_path):
    result = inspect(tmp_path)
    assert result["valid"] and result["key_origin"] == "declared"
    assert result["keys"] == [
        {"baseline": "id", "candidate": "id"},
        {"baseline": "version", "candidate": "version"},
    ]
    assert result["casts"][0]["compare_as"] == "decimal(38,0)"
    assert "uniqueness" in " ".join(result["warnings"])


@pytest.mark.parametrize(
    "mapping",
    [
        {"baseline": "id", "candidate": "other_id"},
        {"baseline": "other_id", "candidate": "id"},
    ],
)
def test_declared_keys_cannot_overlap_explicit_value_mappings(tmp_path, mapping):
    config = document(columns=[mapping])
    result = inspect(tmp_path, config)
    assert not result["valid"]
    assert "overlap" in " ".join(result["issues"])
    assert config["pairs"][0]["columns"] == [mapping]
    assert "keys" not in config["pairs"][0]


def test_absent_declarations_never_guess_id(tmp_path):
    discovery = Discovery()
    discovery.sources["cat.sch.left"]["primary_key"] = []
    result = inspect(tmp_path, discovery=discovery)
    assert not result["valid"] and not result["keys"]
    assert result["baseline"]["columns"][0]["name"] == "id"


def test_explicit_renamed_keys_override_declarations(tmp_path):
    discovery = Discovery()
    discovery.sources["cat.sch.right"]["columns"][0]["name"] = "other_id"
    result = inspect(
        tmp_path, document(keys=[{"baseline": "id", "candidate": "other_id"}]), discovery
    )
    assert result["valid"] and result["key_origin"] == "explicit"


def test_missing_column_and_excluded_key_report_errors(tmp_path):
    result = inspect(tmp_path, document(keys=[{"baseline": "missing", "candidate": "id"}]))
    assert not result["valid"] and "missing" in " ".join(result["issues"])
    result = inspect(tmp_path, document(compare={"exclude_columns": ["id"]}))
    assert not result["valid"] and "excluded" in " ".join(result["issues"])


@pytest.mark.parametrize("override", [{}, {"column_rules": {"id": {"implicit_casts": False}}}])
def test_disabled_cast_inheritance(tmp_path, override):
    config = document(compare=override)
    config["defaults"] = {"implicit_casts": False}
    result = inspect(tmp_path, config)
    assert not result["valid"] and "incompatible" in " ".join(result["issues"])
    assert result["settings"]["implicit_casts"] is False


@pytest.mark.parametrize("rule", [{"compare_as": "long"}, {"implicit_casts": True}])
def test_column_cast_override_restores_alignment(tmp_path, rule):
    config = document(compare={"column_rules": {"id": rule}})
    config["defaults"] = {"implicit_casts": False}
    result = inspect(tmp_path, config)
    assert result["valid"] and result["casts"]


def test_unsupported_key_types_and_duplicate_mappings(tmp_path):
    discovery = Discovery()
    discovery.sources["cat.sch.right"]["columns"][0]["type"] = "ARRAY<INT>"
    assert not inspect(tmp_path, discovery=discovery)["valid"]
    with pytest.raises(ValueError):
        inspect(tmp_path, document(keys=[{"baseline": "id", "candidate": "id"}] * 2))


def test_derived_sources_are_not_flattened_and_errors_are_redacted(tmp_path):
    source = {"kind": "deduplicate", "source": "cat.sch.left"}
    result = inspect(tmp_path, document(baseline=source))
    assert not result["valid"] and "source editor" in " ".join(result["issues"])

    class Denied(Discovery):
        def describe(self, table):
            raise PermissionError("raw-token-and-private-details")

    result = inspect(tmp_path, discovery=Denied())
    assert not result["valid"]
    assert "raw-token" not in json.dumps(result)
    assert "permissions" in " ".join(result["issues"])


def test_versioned_tables_warn_about_current_metadata(tmp_path):
    result = inspect(
        tmp_path, document(baseline={"kind": "table", "table": "cat.sch.left", "version": 1})
    )
    assert result["valid"] and "current" in " ".join(result["warnings"])


def test_discovery_routes_authentication_validation_and_unavailable(tmp_path):
    service = Workbench(FileSuiteCatalog(tmp_path), discovery=Discovery())
    client = TestClient(create_app(lambda request: ("tester", service)))
    assert client.get("/api/v1/discovery/catalogs").status_code == 401
    headers = {"x-forwarded-access-token": "caller", "x-pairity-request": "1"}
    assert client.get("/api/v1/discovery/catalogs", headers=headers).json()["items"] == [
        {"name": "cat"}
    ]
    assert client.get("/api/v1/discovery/catalogs?limit=999", headers=headers).status_code == 422
    response = client.post(
        "/api/v1/discovery/inspect",
        json={"content": json.dumps(document()), "format": "json", "pair_id": "p"},
        headers=headers,
    )
    assert response.status_code == 200 and response.json()["valid"]
    service.discovery = None
    assert client.get("/api/v1/discovery/catalogs", headers=headers).status_code == 422
