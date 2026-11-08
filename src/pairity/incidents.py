"""Offline, deterministic incident evidence from persisted result envelopes."""

import copy
import hashlib
import html
import io
import json
import re
import textwrap
import zipfile
from importlib.metadata import PackageNotFoundError, version

from pairity.config import SqlSource, SuiteConfig, TableSource, resolve_settings
from pairity.metadata import quote_table
from pairity.serialization import dump_config
from pairity.sources import child_sources, expand_source, leaf_tables, replace_children
from pairity.sql_guard import query_hash, render, tokenize


def _json(value):
    return (
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n"
    ).encode()


def _redact(value, columns=()):
    sensitive = {
        "password",
        "token",
        "secret",
        "access_token",
        "authorization",
        "client_secret",
        "api_key",
    } | {name.casefold() for name in columns}
    if isinstance(value, dict):
        return {
            key: "[REDACTED]" if key.casefold() in sensitive else _redact(item, columns)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact(item, columns) for item in value]
    if isinstance(value, str):
        value = re.sub(
            r"(?i)\b(password|token|secret|api_key|access_token)\s*[=:]\s*[^\s,;]+",
            r"\1=[REDACTED]",
            value,
        )
        value = re.sub(r"(?i)Bearer\s+\S+", "Bearer [REDACTED]", value)
        value = re.sub(r"://[^/\s:@]+:[^/\s@]+@", "://[REDACTED]@", value)
    return value


def _pin(source, metadata, explicit_version=None):
    """Match leaves by traversal order, not table name (one table may have two versions)."""
    leaves = iter(metadata.get("leaves") or [metadata])
    queries = []

    def visit(node):
        if isinstance(node, TableSource):
            leaf = next(leaves, None)
            if not leaf or leaf.get("table") != node.table:
                raise ValueError("Missing or mismatched recorded source leaf")
            resolved = leaf.get("version")
            expected = node.version if node.version is not None else explicit_version
            if leaf.get("as_of") != node.as_of or leaf.get("filters", []) != [
                f.model_dump(exclude_none=True) for f in node.where
            ]:
                raise ValueError("Recorded as_of or filters do not match configuration")
            if (
                leaf.get("snapshot_pinned") is not True
                or type(resolved) is not int
                or resolved < 0
                or (expected is not None and expected != resolved)
            ):
                raise ValueError("Missing, invalid, or conflicting recorded Delta version")
            return node.model_copy(update={"version": resolved})
        pinned = replace_children(node, [visit(child) for child in child_sources(node)])
        if isinstance(node, SqlSource):
            queries.append({"query_sha256": query_hash(node.query), "parameters": node.parameters})
        return pinned

    result = visit(source)
    if next(leaves, None) is not None:
        raise ValueError("Unexpected extra recorded source leaves")
    if queries != metadata.get("queries", []):
        raise ValueError("Recorded SQL source queries or parameters do not match configuration")
    return result


def _source_sql(node):
    if node.kind == "table":
        sql = f"SELECT * FROM {quote_table(node.table)} VERSION AS OF {node.version}"
        if node.where:
            sql += " WHERE " + " AND ".join(_filter_sql(item) for item in node.where)
        return sql
    if node.kind == "deduplicate":
        return f"SELECT DISTINCT * FROM ({_source_sql(node.source)}) AS source"
    if node.kind == "sql":
        return _sql_source_sql(node)
    # The engine unions by name; positional SQL would silently change semantics.
    raise ValueError("Union-by-name SQL needs recorded column order; use Python reproduction")


def _sql_literal(value):
    if type(value) is bool:
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    # Spark string literals treat backslash as an escape character.
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _filter_sql(item):
    column = "`" + item.column.replace("`", "``") + "`"
    if item.op == "is_null":
        return f"{column} IS NULL"
    if item.op == "is_not_null":
        return f"{column} IS NOT NULL"
    if item.op in {"in", "not_in"}:
        values = ", ".join(_sql_literal(v) for v in item.value)
        return f"{column} {'NOT IN' if item.op == 'not_in' else 'IN'} ({values})"
    if item.op == "between":
        low, high = (_sql_literal(v) for v in item.value)
        return f"{column} BETWEEN {low} AND {high}"
    return f"{column} {item.op} {_sql_literal(item.value)}"


def _sql_source_sql(node):
    """The guarded query with each pinned input inlined as a CTE; parameters stay markers."""
    names = {name: f"`pairity_input_{name}`" for name in node.inputs}
    ctes = ",\n".join(f"{names[name]} AS ({_source_sql(node.inputs[name])})" for name in names)
    tokens = tokenize(node.query)
    first = next(
        i for i, t in enumerate(tokens) if t.kind not in {"ws", "line_comment", "block_comment"}
    )
    head, rest = "".join(t.text for t in tokens[:first]), node.query[tokens[first].start :]
    body = render(rest, names)
    if tokens[first].lower == "with":
        body = f"WITH {ctes},\n{body[4:].lstrip()}"
    else:
        body = f"WITH {ctes}\n{body}"
    if node.parameters:
        head += f"-- Bind named parameters: {json.dumps(node.parameters, sort_keys=True)}\n"
    return head + body


def _sql(config):
    lines = [
        "-- Exact comparison SQL: UNSUPPORTED. Use reproduce.py for engine semantics.",
        "-- Queries below are diagnostic source reads and raw key checks only.",
        "-- Raw key checks do not include configured comparison casts.",
    ]
    for index, pair in enumerate(config.pairs, 1):
        for side in ("baseline", "candidate"):
            node = getattr(pair, side)
            try:
                source = _source_sql(node)
            except ValueError as exc:
                lines.append(f"-- Pair {index} {side}: UNSUPPORTED: {exc}")
                lines.extend(_source_sql(leaf) + ";" for leaf in leaf_tables(node))
                continue
            lines += [f"-- Pair {index} {side}: pinned source read", source + ";"]
            if pair.keys:
                keys = ["`" + getattr(key, side).replace("`", "``") + "`" for key in pair.keys]
                joined = ", ".join(keys)
                lines += [
                    f"SELECT * FROM ({source}) AS source WHERE "
                    + " OR ".join(f"{key} IS NULL" for key in keys)
                    + ";",
                    f"SELECT {joined}, COUNT(*) AS duplicate_count FROM ({source}) "
                    f"AS source GROUP BY {joined} HAVING COUNT(*) > 1;",
                ]
    return ("\n\n".join(lines) + "\n").encode()


_REPRODUCTION = '''"""Run on an authorized Classic Spark/Delta cluster.
Use matching Pairity/runtime versions.
No credentials are included. Historical Delta versions must still be retained.
The original Spark session settings were not persisted: verify timezone, ANSI and collation.
"""
import json
from contextlib import nullcontext
from pathlib import Path
from pyspark.sql import SparkSession
from pairity.config import SuiteConfig
from pairity.engine import ComparisonRunner, PairityEngine
from pairity.metadata import quote_table
from pairity.results import TableInput
from pairity.spark import SparkComparisonBackend
from pairity.spark_sources import SparkSourceResolver

class PinnedReader:
    api_version = 1
    def __init__(self, spark):
        self.spark = spark
    def scope(self, run_id, pair_id):
        return nullcontext()
    def read(self, table, version=None):
        if type(version) is not int or version < 0:
            raise ValueError("Refusing an unpinned read")
        frame = self.spark.read.option("versionAsOf", version).table(quote_table(table))
        return TableInput(frame, table, version=version, snapshot_pinned=True)

class RecordedKeysOnly:
    api_version = 1
    def primary_key(self, table):
        raise ValueError("No recorded key mapping; refusing current metadata lookup")

spark = SparkSession.builder.getOrCreate()
config = SuiteConfig.model_validate(json.loads(
    (Path(__file__).parent / "resolved.json").read_text()))
engine = PairityEngine(lambda: ComparisonRunner(
    PinnedReader(spark.newSession()), RecordedKeysOnly(), SparkComparisonBackend(),
    source_resolver=SparkSourceResolver()))
print(json.dumps(engine.run(config).to_dict(), indent=2))
'''


class IncidentExporter:
    """Export without readers, credentials, network access, or current metadata lookups.

    Sample input is ``{pair_id: [JSON row dictionaries]}``; it is ignored unless opted in.
    ``max_sample_rows`` is a total bundle limit and ``max_sample_bytes`` a serialized cap.
    """

    api_version = 1

    def __init__(
        self,
        *,
        include_samples=False,
        max_sample_rows=100,
        max_sample_bytes=1_000_000,
        mask_columns=(),
    ):
        if (
            type(max_sample_rows) is not int
            or not 0 <= max_sample_rows <= 10000
            or type(max_sample_bytes) is not int
            or not 0 <= max_sample_bytes <= 10_000_000
        ):
            raise ValueError("Sample limits must be bounded nonnegative integers")
        self.include_samples = include_samples
        self.max_sample_rows = max_sample_rows
        self.max_sample_bytes = max_sample_bytes
        self.mask_columns = tuple(mask_columns)

    def export(
        self, report: dict, *, pair_ids: list[str] | None = None, samples: dict | None = None
    ) -> dict[str, bytes]:
        data = copy.deepcopy(report)
        indexed = {pair["pair_id"]: pair for pair in data["pairs"]}
        selected = list(indexed) if pair_ids is None else pair_ids
        if not selected or len(set(selected)) != len(selected) or set(selected) - indexed.keys():
            raise ValueError("Select nonempty, unique, known pair IDs")
        config = SuiteConfig.model_validate(data["configuration"])
        config.pairs = [pair for pair in config.pairs if pair.id in selected]
        if {pair.id for pair in config.pairs} != set(selected):
            raise ValueError("Selected results do not match configuration")
        issues = []
        for pair in config.pairs:
            outcome = indexed[pair.id]
            pair.compare = type(pair.compare).model_validate(
                resolve_settings(config, pair).model_dump(exclude_none=True)
            )
            if outcome.get("keys"):
                from pairity.config import ColumnMapping

                pair.keys = [ColumnMapping.model_validate(key) for key in outcome["keys"]]
            if pair.keys is None:
                issues.append(f"{pair.id}: no recorded or configured keys")
            for side in ("baseline", "candidate"):
                expanded = expand_source(getattr(pair, side), config.templates)
                try:
                    pinned = _pin(
                        expanded,
                        outcome.get("inputs", {}).get(side, {}),
                        getattr(pair, f"{side}_version"),
                    )
                except ValueError as exc:
                    issues.append(f"{pair.id} {side}: {exc}")
                    pinned = expanded
                setattr(pair, side, pinned)
                setattr(pair, f"{side}_version", None)
        # Expanded sources and per-pair settings contain all needed template/default semantics.
        config.templates = {}
        config.defaults = type(config.defaults)()
        data["configuration"] = config.model_dump(exclude_none=True)
        data["pairs"] = [indexed[pair.id] for pair in config.pairs]
        for pair in data["pairs"]:
            if pair["status"] in ("blocked", "error", "skipped"):
                pair["counts"] = None
                pair["column_changes"] = {}
        statuses = {pair["status"] for pair in data["pairs"]}
        data["status"] = next(
            (s for s in ("error", "blocked", "failed", "passed") if s in statuses), "skipped"
        )
        # Configuration and structural maps use column names as keys: a column called
        # "token" is not a credential. Only free-text diagnostics receive text redaction;
        # sample row values are redacted separately when explicitly included below.
        for pair in data["pairs"]:
            if pair.get("reason") is not None:
                pair["reason"] = _redact(pair["reason"])
            pair["schema_differences"] = [
                _redact(message) for message in pair.get("schema_differences", [])
            ]
        lines = [
            "Pairity incident",
            f"Run: {data['run_id']}",
            f"Suite: {data['suite']}",
            f"Status: {data['status']}",
            f"Original config hash: {data['config_hash']}",
            "Scope: selected persisted outcomes; no current table reads.",
            "",
        ]
        for pair in data["pairs"]:
            lines.append(f"{pair['pair_id']}: {pair['status']}")
            counts = pair.get("counts")
            lines.append(
                "Counts unavailable"
                if counts is None
                else ", ".join(f"{key}: {value}" for key, value in sorted(counts.items()))
            )
            if pair.get("reason"):
                lines.append(pair["reason"])
            lines.extend(pair.get("schema_differences", []))
            if pair.get("column_changes"):
                lines.append(
                    "Column changes: " + json.dumps(pair["column_changes"], sort_keys=True)
                )
            lines.append("")
        lines += [
            "Exact comparison SQL: UNSUPPORTED; use the Python engine reproduction.",
            "Runtime settings/version were not recorded; identical rerun is not guaranteed.",
            "Delta versions require retention and access privileges.",
        ]
        lines += ["Reproduction blocked: " + issue for issue in issues]
        summary = "\n".join(lines) + "\n"
        files = {
            "summary.txt": summary.encode(),
            "ticket.md": summary.encode(),
            "summary.html": (
                '<!doctype html><meta charset="utf-8"><title>Pairity incident</title>'
                "<pre>" + html.escape(summary) + "</pre>"
            ).encode(),
            "result.json": _json(data),
            "resolved.json": dump_config(config, "json").encode(),
            "resolved.yaml": dump_config(config, "yaml").encode(),
        }
        try:
            from PIL import Image, ImageDraw, ImageFont
        except ImportError as exc:
            raise ImportError("Install pairity-data[artifacts] to export incident PNGs") from exc
        wrapped = [part for line in lines for part in (textwrap.wrap(line, 110) or [""])]
        # Bound image size; complete details remain in HTML/text/JSON.
        if len(wrapped) > 180:
            wrapped = wrapped[:179] + ["More details: see summary.html and result.json"]
        picture = Image.new("RGB", (1000, 40 + len(wrapped) * 17), "#101827")
        draw = ImageDraw.Draw(picture)
        font = ImageFont.load_default()
        for i, line in enumerate(wrapped):
            draw.text((20, 20 + i * 17), line, font=font, fill="#e5edf8")
        output = io.BytesIO()
        picture.save(output, format="PNG", optimize=False)
        files["summary.png"] = output.getvalue()
        if issues:
            files["reproduce.py"] = ("raise SystemExit(" + repr("; ".join(issues)) + ")\n").encode()
            files["reproduce.sql"] = b"-- UNSUPPORTED: missing validated pins; see manifest.json\n"
        else:
            files["reproduce.py"] = _REPRODUCTION.encode()
            files["reproduce.sql"] = _sql(config)
        if self.include_samples and samples is not None:
            bounded = {}
            remaining = self.max_sample_rows
            for pair in data["pairs"]:
                rows = samples.get(pair["pair_id"], [])
                if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                    raise ValueError("Samples must be lists of JSON row dictionaries")
                bounded[pair["pair_id"]] = _redact(rows[:remaining], self.mask_columns)
                remaining -= len(bounded[pair["pair_id"]])
            payload = _json(bounded)
            if len(payload) > self.max_sample_bytes:
                raise ValueError("Masked samples exceed the configured byte limit")
            files["samples.json"] = payload
        try:
            exporter_version = version("pairity-data")
        except PackageNotFoundError:
            exporter_version = "unknown"
        manifest = {
            "manifest_schema_version": 1,
            "exporter_version": exporter_version,
            "run_id": data["run_id"],
            "pair_ids": [p.id for p in config.pairs],
            "original_config_hash": data["config_hash"],
            "resolved_config_hash": config.fingerprint,
            "reproduction": {
                "ready": not issues,
                "issues": issues,
                "exact_comparison_sql": False,
                "runtime_settings_recorded": False,
            },
            "samples": {
                "included": "samples.json" in files,
                "max_rows": self.max_sample_rows,
                "masked_columns": sorted(self.mask_columns),
            },
            "files": {
                name: {"sha256": hashlib.sha256(content).hexdigest(), "size_bytes": len(content)}
                for name, content in sorted(files.items())
            },
        }
        files["manifest.json"] = _json(manifest)
        return files

    @staticmethod
    def zip_bytes(files: dict[str, bytes]) -> bytes:
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, content in sorted(files.items()):
                if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", name) or ".." in name:
                    raise ValueError("Unsafe artifact filename")
                info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = 0o600 << 16
                archive.writestr(info, content)
        return output.getvalue()
