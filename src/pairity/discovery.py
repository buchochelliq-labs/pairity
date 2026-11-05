"""Bounded Unity Catalog metadata discovery using the caller's SQL connection."""

from pairity.contracts import MetadataProvider
from pairity.metadata import quote_table, split_table


def _identifier(name):
    # Use exactly the identifier domain supported by the shared config schema.
    split_table(f"{name}.schema.table")
    return f"`{name}`"


class UnityCatalogDiscovery:
    api_version = 1

    def __init__(self, query, metadata: MetadataProvider):
        self.query, self.metadata = query, metadata

    def _page(self, relation, name, filters, params, *, search="", after="", limit=50):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("Discovery page size must be 1..100")
        if not isinstance(search, str) or len(search) > 256:
            raise ValueError("Search must be at most 256 characters")
        if not isinstance(after, str) or len(after) > 256:
            raise ValueError("Cursor must be at most 256 characters")
        rows = self.query(
            f"SELECT {name} AS name FROM {relation} WHERE {filters} "
            f"AND contains(lower({name}), lower(:search)) AND {name} > :after "
            f"ORDER BY {name} LIMIT {limit + 1}",
            params | {"search": search, "after": after},
        )
        items = [{"name": row["name"]} for row in rows[:limit]]
        return {"items": items, "next_after": items[-1]["name"] if len(rows) > limit else None}

    def catalogs(self, **paging):
        return self._page(
            "system.information_schema.catalogs", "catalog_name", "1 = 1", {}, **paging
        )

    def schemas(self, catalog, **paging):
        return self._page(
            f"{_identifier(catalog)}.information_schema.schemata",
            "schema_name",
            "catalog_name = :catalog AND schema_name <> 'information_schema'",
            {"catalog": catalog},
            **paging,
        )

    def tables(self, catalog, schema, **paging):
        _identifier(schema)
        return self._page(
            f"{_identifier(catalog)}.information_schema.tables",
            "table_name",
            "table_catalog = :catalog AND table_schema = :schema AND data_source_format = 'DELTA'",
            {"catalog": catalog, "schema": schema},
            **paging,
        )

    def describe(self, table):
        catalog, schema, name = split_table(table)
        params = {"catalog": catalog, "schema": schema, "table": name}
        scope = "table_catalog = :catalog AND table_schema = :schema AND table_name = :table"
        rows = self.query(
            f"SELECT data_source_format FROM `{catalog}`.information_schema.tables "
            f"WHERE {scope} LIMIT 2",
            params,
        )
        if len(rows) != 1:
            raise ValueError(f"{table}: table is not visible or no longer exists")
        if rows[0]["data_source_format"] != "DELTA":
            raise ValueError(f"{table}: only Delta tables are supported by the comparison engine")
        # Metadata visibility alone does not establish SELECT permission.
        self.query(f"SELECT * FROM {quote_table(table)} LIMIT 0", {})
        rows = self.query(
            f"SELECT column_name, full_data_type, is_nullable "
            f"FROM `{catalog}`.information_schema.columns WHERE {scope} "
            "ORDER BY ordinal_position LIMIT 501",
            params,
        )
        if not rows or len(rows) > 500:
            raise ValueError(f"{table}: discovery requires between 1 and 500 visible columns")
        columns = [
            {
                "name": r["column_name"],
                "type": r["full_data_type"],
                "nullable": r["is_nullable"] == "YES",
            }
            for r in rows
        ]
        if len({c["name"].casefold() for c in columns}) != len(columns):
            raise ValueError(f"{table}: ambiguous duplicate or case-colliding columns")
        return {
            "table": table,
            "columns": columns,
            "primary_key": self.metadata.primary_key(table) or [],
        }
