"""Primary-key discovery is metadata lookup, never data-based key guessing."""

from collections.abc import Callable, Iterable

from pydantic import TypeAdapter

from pairity.config import ColumnMapping, PairConfig, TableName, TableSource
from pairity.contracts import MetadataProvider
from pairity.results import BlockedComparison


class MissingKeys(BlockedComparison):
    """Supply explicit keys when the metadata cannot determine an unambiguous mapping."""


def resolve_keys(pair: PairConfig, metadata: MetadataProvider) -> list[ColumnMapping]:
    if pair.keys is not None:
        return [key.model_copy(deep=True) for key in pair.keys]

    def table_name(source):
        if isinstance(source, str):
            return source
        if isinstance(source, TableSource):
            return source.table
        raise MissingKeys("Derived sources require explicit key mappings")

    baseline = metadata.primary_key(table_name(pair.baseline))
    candidate = metadata.primary_key(table_name(pair.candidate))
    if (
        not baseline
        or not candidate
        or len(set(baseline)) != len(baseline)
        or len(set(candidate)) != len(candidate)
        or set(baseline) != set(candidate)
    ):
        raise MissingKeys("Declared PKs are missing or incompatible; supply explicit key mappings")
    return [ColumnMapping(baseline=key, candidate=key) for key in baseline]


def split_table(table: str) -> tuple[str, str, str]:
    valid = TypeAdapter(TableName).validate_python(table)
    return tuple(valid.split("."))


def quote_table(table: str) -> str:
    return ".".join(f"`{part}`" for part in split_table(table))


class UnityCatalogMetadata:
    api_version = 1

    def __init__(self, query: Callable[[str, dict], Iterable[dict]]):
        self._query = query

    def primary_key(self, table: str) -> list[str] | None:
        catalog, schema, name = split_table(table)
        sql = f"""SELECT k.column_name, k.ordinal_position, k.constraint_name
FROM `{catalog}`.information_schema.table_constraints c
JOIN `{catalog}`.information_schema.key_column_usage k
  ON c.constraint_catalog = k.constraint_catalog
 AND c.constraint_schema = k.constraint_schema
 AND c.constraint_name = k.constraint_name
 AND c.table_catalog = k.table_catalog
 AND c.table_schema = k.table_schema
 AND c.table_name = k.table_name
WHERE c.constraint_type = 'PRIMARY KEY'
  AND c.table_catalog = :catalog AND c.table_schema = :schema AND c.table_name = :table
ORDER BY k.ordinal_position"""
        rows = list(self._query(sql, {"catalog": catalog, "schema": schema, "table": name}))
        if not rows:
            return None
        ordinals = [row["ordinal_position"] for row in rows]
        names = [row["column_name"] for row in rows]
        if (
            ordinals != list(range(1, len(rows) + 1))
            or len(set(names)) != len(names)
            or len({row["constraint_name"] for row in rows}) != 1
        ):
            raise MissingKeys("Inconsistent catalog PK metadata; supply explicit key mappings")
        return names
