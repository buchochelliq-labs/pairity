"""Bounded SQL reads for a Spark-free web process, using the caller's identity."""

import hashlib
import json

from pairity.metadata import quote_table


class SqlQuery:
    def __init__(self, client, warehouse_id):
        if not warehouse_id:
            raise ValueError("A SQL warehouse is required for the hosted application")
        self.client, self.warehouse_id = client, warehouse_id

    def __call__(self, statement, parameters):
        from databricks.sdk.service.sql import (
            ExecuteStatementRequestOnWaitTimeout,
            StatementParameterListItem,
        )

        result = self.client.statement_execution.execute_statement(
            statement=statement,
            warehouse_id=self.warehouse_id,
            parameters=[
                StatementParameterListItem(name=k, value=str(v)) for k, v in parameters.items()
            ],
            wait_timeout="30s",
            on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CANCEL,
            row_limit=1000,
            byte_limit=4 * 1024 * 1024,
        )
        if (
            result.status is None
            or result.status.state is None
            or result.status.state.value != "SUCCEEDED"
        ):
            raise ValueError(
                "SQL read did not complete; check warehouse access and statement history"
            )
        if result.manifest is None or result.manifest.truncated:
            raise ValueError("SQL result exceeds the application response bound")
        chunk = result.result
        if chunk is None:
            return []
        if chunk.next_chunk_index is not None:
            raise ValueError("SQL result requires pagination; narrow the request")
        names = [c.name for c in result.manifest.schema.columns]
        return [dict(zip(names, row, strict=True)) for row in chunk.data_array or []]


class SqlResultReader:
    api_version = 1

    def __init__(self, query, table):
        self.query, self.table = query, quote_table(table)

    @staticmethod
    def decode(row):
        if hashlib.sha256(row["payload"].encode()).hexdigest() != row["report_hash"]:
            raise ValueError("Stored result checksum mismatch")
        value = json.loads(row["payload"])
        if value.get("storage_schema_version") != 1:
            raise ValueError("Unsupported result storage schema version")
        return value

    def load(self, run_id):
        rows = self.query(
            f"SELECT payload, report_hash FROM {self.table} "
            "WHERE record_type = 'report' AND run_id = :run_id LIMIT 2",
            {"run_id": run_id},
        )
        if not rows:
            raise FileNotFoundError("No completed result for this run")
        if len(rows) != 1:
            raise ValueError("Duplicate run completion records")
        return self.decode(rows[0])

    def history(self, *, suite=None, limit=50):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("History limit must be 1..100")
        clause, params = (" AND suite = :suite", {"suite": suite}) if suite else ("", {})
        rows = self.query(
            f"SELECT payload, report_hash FROM {self.table} WHERE record_type = 'report'"
            + clause
            + f" ORDER BY finished_at DESC, run_id LIMIT {limit}",
            params,
        )
        return [self.decode(row)["report"] for row in rows]


def source_tables(config):
    from pairity.config import DeduplicateSource, TableSource, UnionSource
    from pairity.sources import expand_source

    tables = set()

    def visit(source):
        if isinstance(source, str):
            tables.add(source)
        elif isinstance(source, TableSource):
            tables.add(source.table)
        elif isinstance(source, UnionSource):
            for child in source.sources:
                visit(child)
        elif isinstance(source, DeduplicateSource):
            visit(source.source)
        else:
            raise ValueError("Unsupported source authorization")

    for pair in config.pairs:
        if pair.enabled:
            visit(expand_source(pair.baseline, config.templates))
            visit(expand_source(pair.candidate, config.templates))
    return sorted(tables)


def source_authorizer(query):
    authorized = set()  # Request-scoped: never cache across users or requests.

    def authorize(config):
        for table in source_tables(config):
            if table not in authorized:
                query(f"SELECT * FROM {quote_table(table)} LIMIT 0", {})
                authorized.add(table)

    return authorize
