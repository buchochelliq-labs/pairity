"""Bounded SQL reads for a Spark-free web process, using the caller's identity."""

import hashlib
import json
import logging
import re
import time

from pairity.metadata import quote_table

logger = logging.getLogger(__name__)


def _sql_state(result):
    return result.status.state.value if result.status and result.status.state else "UNKNOWN"


def _sql_failure(result, warehouse_id):
    status = result.status
    error = status.error if status else None
    code = error.error_code.value if error and error.error_code else "UNKNOWN"
    sql_state = status.sql_state if status else None
    sql_state = sql_state if sql_state and re.fullmatch(r"[A-Z0-9]{5}", sql_state) else "unknown"
    # Provider messages can contain SQL, literals and source values. Return only
    # the structured error class, never the free-form message or submitted SQL.
    match = re.match(r"^\[([A-Z][A-Z0-9_.]{0,127})\]", error.message or "") if error else None
    error_class = match[1] if match else "unknown"
    state = _sql_state(result)
    statement_id = result.statement_id or "unavailable"
    logger.warning(
        "SQL check failed warehouse=%s statement=%s state=%s code=%s sql_state=%s class=%s",
        warehouse_id,
        statement_id,
        state,
        code,
        sql_state,
        error_class,
    )
    hint = "Open SQL query history for the statement's full error."
    if sql_state == "42501" or "PERMISSION" in error_class:
        hint = "Check your SELECT, USE CATALOG, USE SCHEMA and warehouse CAN_USE permissions."
    elif sql_state == "42P01" or error_class == "TABLE_OR_VIEW_NOT_FOUND":
        hint = "Check the configured table exists and its catalog/schema/table name is correct."
    elif state == "CANCELED":
        hint = "The SQL check was cancelled. Check warehouse availability, then retry."
    return ValueError(
        f"SQL check failed ({state}; {code}; SQLSTATE {sql_state}; {error_class}). "
        f"Statement ID: {statement_id}. {hint}"
    )


class SqlQuery:
    def __init__(self, client, warehouse_id, *, timeout_seconds=120):
        if not warehouse_id:
            raise ValueError("A SQL warehouse is required for the hosted application")
        self.client, self.warehouse_id = client, warehouse_id
        if type(timeout_seconds) is not int or not 30 <= timeout_seconds <= 300:
            raise ValueError("SQL wait budget must be between 30 and 300 seconds")
        self.timeout_seconds = timeout_seconds

    def __call__(self, statement, parameters):
        from databricks.sdk.service.sql import (
            ExecuteStatementRequestOnWaitTimeout,
            StatementParameterListItem,
        )

        deadline = time.monotonic() + self.timeout_seconds
        result = self.client.statement_execution.execute_statement(
            statement=statement,
            warehouse_id=self.warehouse_id,
            parameters=[
                StatementParameterListItem(name=k, value=str(v)) for k, v in parameters.items()
            ],
            wait_timeout="30s",
            on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
            row_limit=1000,
            byte_limit=4 * 1024 * 1024,
        )
        statement_id = result.statement_id
        while _sql_state(result) in {"PENDING", "RUNNING"}:
            if not statement_id:
                raise ValueError("SQL check is pending but returned no statement ID")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                cancellation = "Cancellation requested."
                try:
                    self.client.statement_execution.cancel_execution(statement_id=statement_id)
                except Exception:
                    cancellation = "Cancellation could not be confirmed; check SQL query history."
                logger.warning(
                    "SQL wait timed out warehouse=%s statement=%s", self.warehouse_id, statement_id
                )
                raise ValueError(
                    f"SQL check timed out after {self.timeout_seconds}s. "
                    f"Statement ID: {statement_id}. "
                    f"The warehouse may still be starting or busy. {cancellation}"
                )
            time.sleep(min(2, remaining))
            result = self.client.statement_execution.get_statement(statement_id=statement_id)
            if result.statement_id != statement_id:
                raise ValueError("SQL response belongs to a different statement")
        if _sql_state(result) != "SUCCEEDED":
            raise _sql_failure(result, self.warehouse_id)
        if result.manifest is None or result.manifest.truncated:
            raise ValueError("SQL result exceeds the application response bound")
        chunk = result.result
        if chunk is None:
            return []
        if chunk.next_chunk_index is not None:
            raise ValueError("SQL result requires pagination; narrow the request")
        if result.manifest.schema is None or result.manifest.schema.columns is None:
            raise ValueError("SQL result is missing its column schema")
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
    from pairity.sources import expand_source, leaf_tables

    # SQL sources can only read their declared inputs (pairity.sql_guard), so authorizing
    # every leaf table authorizes everything a suite can read.
    tables = set()
    for pair in config.pairs:
        if pair.enabled:
            for side in (pair.baseline, pair.candidate):
                tables.update(
                    leaf.table for leaf in leaf_tables(expand_source(side, config.templates))
                )
    return sorted(tables)


def source_authorizer(query):
    authorized = set()  # Request-scoped: never cache across users or requests.

    def authorize(config):
        for table in source_tables(config):
            if table not in authorized:
                query(f"SELECT * FROM {quote_table(table)} LIMIT 0", {})
                authorized.add(table)

    return authorize
