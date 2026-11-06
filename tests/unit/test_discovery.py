import pytest

from pairity.discovery import UnityCatalogDiscovery
from pairity.type_alignment import canonical_type, implicit_target_name, scalar_type


class Query:
    def __init__(self, *results):
        self.results = iter(results)
        self.calls = []

    def __call__(self, sql, params):
        self.calls.append((sql, params))
        result = next(self.results)
        if isinstance(result, Exception):
            raise result
        return result


class Keys:
    api_version = 1

    def primary_key(self, table):
        return ["id", "version"]


def test_explorer_search_spans_visible_schemas_with_literal_filters_and_cursor():
    query = Query(
        [
            {"catalog": "cat", "schema": "a", "name": "member", "full_name": "cat.a.member"},
            {"catalog": "cat", "schema": "b", "name": "member", "full_name": "cat.b.member"},
        ]
    )
    result = UnityCatalogDiscovery(query, Keys()).search_tables(
        catalog="cat", search="%_'", limit=1
    )
    assert result["next_after"] == "cat.a.member"
    assert result["items"][0]["full_name"] == "cat.a.member"
    sql, params = query.calls[0]
    assert "system.information_schema.tables" in sql
    assert "data_source_format = 'DELTA'" in sql
    assert "%_'" not in sql and params["search"] == "%_'"
    assert params["catalog"] == "cat" and params["schema"] == ""
    assert "LIMIT 2" in sql


def test_explorer_search_schema_requires_catalog_and_validates_cursor():
    discovery = UnityCatalogDiscovery(Query(), Keys())
    for kwargs in [
        {"schema": "sch"},
        {"catalog": "bad`"},
        {"after": "not.qualified"},
        {"limit": 101},
    ]:
        with pytest.raises(ValueError):
            discovery.search_tables(**kwargs)


def test_catalog_search_is_bounded_literal_and_paginated():
    query = Query([{"name": "cat1"}, {"name": "cat2"}, {"name": "cat3"}])
    page = UnityCatalogDiscovery(query, Keys()).catalogs(search="%'_", limit=2)
    assert page == {"items": [{"name": "cat1"}, {"name": "cat2"}], "next_after": "cat2"}
    sql, params = query.calls[0]
    assert "%'_" not in sql
    assert params["search"] == "%'_"
    assert "LIMIT 3" in sql


def test_scoped_table_page_and_cursor():
    query = Query([{"name": "after"}])
    page = UnityCatalogDiscovery(query, Keys()).tables("cat", "sch", after="a")
    assert page["next_after"] is None
    assert page["items"] == [{"name": "after"}]
    sql, params = query.calls[0]
    assert "`cat`.information_schema.tables" in sql
    assert params["schema"] == "sch" and params["after"] == "a"
    assert "data_source_format = 'DELTA'" in sql


@pytest.mark.parametrize("kwargs", [{"limit": 0}, {"limit": 101}, {"search": "x" * 257}])
def test_reject_unbounded_discovery(kwargs):
    query = Query()
    with pytest.raises(ValueError):
        UnityCatalogDiscovery(query, Keys()).catalogs(**kwargs)
    assert not query.calls


def test_catalog_identifier_is_validated_before_query():
    query = Query()
    with pytest.raises(ValueError):
        UnityCatalogDiscovery(query, Keys()).schemas("cat`; DROP TABLE x; --")
    assert not query.calls


def test_describe_checks_select_access_and_returns_no_source_records():
    query = Query(
        [{"data_source_format": "DELTA"}],
        [],
        [
            {"column_name": "id", "full_data_type": "BIGINT", "is_nullable": "NO"},
            {"column_name": "version", "full_data_type": "INT", "is_nullable": "YES"},
        ],
    )
    info = UnityCatalogDiscovery(query, Keys()).describe("cat.sch.tbl")
    assert info["primary_key"] == ["id", "version"]
    assert info["columns"] == [
        {"name": "id", "type": "BIGINT", "nullable": False},
        {"name": "version", "type": "INT", "nullable": True},
    ]
    assert query.calls[1] == ("SELECT * FROM `cat`.`sch`.`tbl` LIMIT 0", {})


@pytest.mark.parametrize("rows", [[], [{"data_source_format": "PARQUET"}]])
def test_missing_or_unsupported_table_fails_without_source_read(rows):
    query = Query(rows)
    with pytest.raises(ValueError, match="not visible|Delta"):
        UnityCatalogDiscovery(query, Keys()).describe("cat.sch.tbl")
    assert len(query.calls) == 1


def test_denied_select_is_not_treated_as_valid_metadata():
    query = Query([{"data_source_format": "DELTA"}], PermissionError("denied"))
    with pytest.raises(PermissionError):
        UnityCatalogDiscovery(query, Keys()).describe("cat.sch.tbl")


def test_column_limit_fails_explicitly():
    query = Query([{"data_source_format": "DELTA"}], [], [{}] * 501)
    with pytest.raises(ValueError, match="500"):
        UnityCatalogDiscovery(query, Keys()).describe("cat.sch.tbl")


@pytest.mark.parametrize(
    "left,right,target",
    [
        ("STRING", "BIGINT", "decimal(38,0)"),
        ("string", "decimal(10,2)", "decimal(38,2)"),
        ("TINYINT", "SMALLINT", "long"),
        ("int", "decimal(12,3)", "decimal(13,3)"),
        ("decimal(38,20)", "long", None),
        ("float", "int", "double"),
        ("void", "date", "date"),
        ("void", "timestamp", None),
        ("date", "timestamp", None),
        ("bigint", "long", None),
        ("string", "boolean", None),
    ],
)
def test_shared_implicit_cast_policy(left, right, target):
    assert implicit_target_name(left, right) == target
    assert implicit_target_name(right, left) == target


def test_scalar_names_do_not_accept_complex_or_unrecognized_types():
    assert scalar_type("DECIMAL(12, 2)")
    assert scalar_type("TIMESTAMP_NTZ")
    assert not scalar_type("ARRAY<INT>")
    assert not scalar_type("DECIMAL(40,2)")
    assert canonical_type("INTEGER") == "int"
