import pytest

from pairity.config import PairConfig
from pairity.metadata import MissingKeys, UnityCatalogMetadata, resolve_keys


def pair(**kwargs):
    return PairConfig(
        id="orders", baseline="prod.sales.orders", candidate="dev.sales.orders", **kwargs
    )


class Catalog:
    api_version = 1

    def __init__(self, left, right):
        self.keys = {"prod.sales.orders": left, "dev.sales.orders": right}

    def primary_key(self, table):
        return self.keys[table]


def test_explicit_keys_do_not_request_catalog_metadata():
    class Unavailable:
        def primary_key(self, table):
            raise AssertionError("Explicit keys must not access catalog metadata")

    keys = resolve_keys(pair(keys=[{"baseline": "a", "candidate": "x"}]), Unavailable())
    assert [(k.baseline, k.candidate) for k in keys] == [("a", "x")]


def test_composite_metadata_matches_names_not_different_ordinal_positions():
    keys = resolve_keys(pair(), Catalog(["tenant", "id"], ["id", "tenant"]))
    assert [(k.baseline, k.candidate) for k in keys] == [("tenant", "tenant"), ("id", "id")]


@pytest.mark.parametrize(
    "left,right",
    [(None, ["id"]), (["id"], None), (["id"], ["renamed_id"]), (["id", "id"], ["id"]), ([], [])],
)
def test_missing_or_ambiguous_keys_require_explicit_user_input(left, right):
    with pytest.raises(MissingKeys):
        resolve_keys(pair(), Catalog(left, right))


def test_uc_lookup_scopes_parameters_and_orders_key_metadata():
    seen = []

    def query(sql, parameters):
        seen.append((sql, parameters))
        return [
            {"column_name": "tenant", "ordinal_position": 1, "constraint_name": "pk"},
            {"column_name": "id", "ordinal_position": 2, "constraint_name": "pk"},
        ]

    provider = UnityCatalogMetadata(query)
    assert provider.primary_key("prod.sales.orders") == ["tenant", "id"]
    sql, parameters = seen[0]
    assert "`prod`.information_schema.table_constraints" in sql
    assert "PRIMARY KEY" in sql
    assert parameters == {"catalog": "prod", "schema": "sales", "table": "orders"}
    assert "sales.orders" not in sql


def test_uc_permission_error_is_not_treated_as_absent_metadata():
    def denied(sql, parameters):
        raise PermissionError("denied")

    with pytest.raises(PermissionError):
        UnityCatalogMetadata(denied).primary_key("prod.sales.orders")


def test_uc_inconsistent_ordinals_are_rejected():
    provider = UnityCatalogMetadata(
        lambda *_: [{"column_name": "id", "ordinal_position": 2, "constraint_name": "pk"}]
    )
    with pytest.raises(MissingKeys):
        provider.primary_key("prod.sales.orders")
