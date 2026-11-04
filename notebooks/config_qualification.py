# Databricks notebook source
"""Synthetic serverless qualification: real core, read-only in-memory source adapter."""

import io
import json
import re
from contextlib import nullcontext

from pairity.config import SuiteConfig
from pairity.engine import ComparisonRunner, PairityEngine
from pairity.results import TableInput
from pairity.serialization import dump_config, load_config
from pairity.serverless import InlineExecution


def suite(case, baseline="text", candidate="number", *, defaults=None, compare=None, keys=True):
    pair = {
        "id": case,
        "baseline": f"qualification.memory.{baseline}",
        "candidate": f"qualification.memory.{candidate}",
        "compare": compare or {},
    }
    if keys:
        pair["keys"] = [{"baseline": "id", "candidate": "id"}]
    return SuiteConfig.model_validate(
        {
            "schema_version": 1,
            "suite": "configuration-qualification",
            "defaults": defaults or {},
            "pairs": [pair],
        }
    )


def counts(rows, changed=0):
    return dict(
        baseline_rows=rows,
        candidate_rows=rows,
        matched_rows=rows,
        changed_rows=changed,
        missing_rows=0,
        extra_rows=0,
    )


def qualification_cases():
    cases = [
        (suite("implicit-default"), "passed", counts(2)),
        (suite("global-casts-disabled", defaults={"implicit_casts": False}), "failed", None),
        (
            suite(
                "column-casts-disabled",
                compare={"column_rules": {"value": {"implicit_casts": False}}},
            ),
            "failed",
            None,
        ),
        (
            suite(
                "explicit-cast",
                defaults={"implicit_casts": False},
                compare={"column_rules": {"value": {"compare_as": "long"}}},
            ),
            "passed",
            counts(2),
        ),
        (
            suite(
                "column-casts-enabled",
                defaults={"implicit_casts": False},
                compare={"column_rules": {"value": {"implicit_casts": True}}},
            ),
            "passed",
            counts(2),
        ),
        (
            suite(
                "common-excluded",
                "old",
                "new",
                compare={"column_policy": "common", "exclude_columns": ["drop"]},
            ),
            "passed",
            counts(1),
        ),
        (suite("strict-schema", "old", "new"), "failed", None),
        (suite("null-safe-default", "nulls", "nulls"), "passed", counts(2)),
        (
            suite("null-safe-disabled", "nulls", "nulls", defaults={"null_safe": False}),
            "failed",
            counts(2, 1),
        ),
        (
            suite(
                "column-null-safe",
                "nulls",
                "nulls",
                defaults={"null_safe": False},
                compare={"column_rules": {"value": {"null_safe": True}}},
            ),
            "passed",
            counts(2),
        ),
        (suite("missing-pk", "nulls", "nulls", keys=False), "blocked", None),
        (suite("null-pk", "null_key", "nulls"), "blocked", None),
        (suite("duplicate-pk", "duplicate_key", "nulls"), "blocked", None),
        (suite("invalid-cast", "bad_number", "number"), "blocked", None),
    ]
    data = suite("template-union-dedup", "current", "combined").model_dump(exclude_none=True)
    data["templates"] = {
        "unique": {
            "parameters": ["rows"],
            "source": {"kind": "deduplicate", "source": {"kind": "parameter", "name": "rows"}},
        }
    }
    data["pairs"][0]["baseline"] = {
        "kind": "template",
        "name": "unique",
        "inputs": {
            "rows": {
                "kind": "union",
                "sources": ["qualification.memory.current", "qualification.memory.archive"],
            }
        },
    }
    cases.append((SuiteConfig.model_validate(data), "passed", counts(2)))
    return cases


def schema_validation_evidence():
    config = qualification_cases()[0][0]
    invalid = [
        config.model_dump(exclude_none=True) | {"unknown_option": True},
        config.model_dump(exclude_none=True) | {"defaults": {"implicit_casts": "false"}},
        config.model_dump(exclude_none=True)
        | {"defaults": {"column_rules": {"value": {"compare_as": "decimal(39,2)"}}}},
    ]
    for data in invalid:
        for codec in ("json", "yaml"):
            # JSON is valid YAML, so both parsers receive exactly the same invalid document.
            try:
                load_config(json.dumps(data), codec)
            except ValueError:
                continue
            raise AssertionError(f"{codec} accepted invalid configuration")
    return {"rejected_documents": len(invalid) * 2, "codecs": ["json", "yaml"]}


def run_qualification(spark):
    from pairity.spark import SparkComparisonBackend
    from pairity.spark_sources import SparkSourceResolver

    specifications = {
        "text": ([(1, "00042"), (2, None)], "id long, value string"),
        "number": ([(1, 42), (2, None)], "id long, value long"),
        "old": ([(1, "ok", 3, 8)], "id long, value string, drop long, old long"),
        "new": ([(1, "ok", 4, 9)], "id long, value string, drop long, new long"),
        "nulls": ([(1, None), (2, "ok")], "id long, value string"),
        "null_key": ([(None, "ok")], "id long, value string"),
        "duplicate_key": ([(1, "a"), (1, "b")], "id long, value string"),
        "bad_number": ([(1, "nope"), (2, None)], "id long, value string"),
        "current": ([(1, "a"), (2, "b")], "id long, value string"),
        "archive": ([(1, "a")], "id long, value string"),
        "combined": ([(2, "b"), (1, "a")], "id long, value string"),
    }
    frames = {
        f"qualification.memory.{name}": spark.createDataFrame(rows, schema)
        for name, (rows, schema) in specifications.items()
    }

    class Reader:
        api_version = 1

        def scope(self, *args):
            return nullcontext()

        def read(self, table, version=None):
            if version is not None:
                raise ValueError("Synthetic frames do not have Delta versions")
            return TableInput(frames[table], table)

    class Metadata:
        api_version = 1

        def primary_key(self, table):
            return None

    def factory():
        return ComparisonRunner(
            Reader(), Metadata(), SparkComparisonBackend(), source_resolver=SparkSourceResolver()
        )

    engine = PairityEngine(factory, execution=InlineExecution())
    evidence = {"schema_validation": schema_validation_evidence(), "cases": []}
    for config, expected_status, expected_counts in qualification_cases():
        outcomes = []
        for codec in ("json", "yaml"):
            loaded = load_config(dump_config(config, codec), codec)
            if loaded.fingerprint != config.fingerprint:
                raise AssertionError("Codec changed configuration semantics")
            run = engine.run(loaded)
            result = run.pairs[0]
            if (
                run.status != expected_status
                or result.status != expected_status
                or result.counts != expected_counts
            ):
                raise AssertionError(f"Unexpected {codec} result: {result.to_dict()}")
            outcomes.append(result.to_dict())
        if outcomes[0] != outcomes[1]:
            raise AssertionError("JSON and YAML results differ")
        result = outcomes[0]
        case = config.pairs[0].id
        if case == "common-excluded" and result["ignored_columns"] != {
            "baseline": ["drop", "old"],
            "candidate": ["drop", "new"],
        }:
            raise AssertionError("Ignored columns audit differs")
        if case in {"global-casts-disabled", "column-casts-disabled", "strict-schema"}:
            if not result["schema_differences"]:
                raise AssertionError("Expected schema mismatch evidence")
        if case in {"implicit-default", "explicit-cast"}:
            mode = "explicit" if case == "explicit-cast" else "implicit"
            if not any(c["mode"] == mode for c in result["casts"]):
                raise AssertionError("Missing cast audit")
        evidence["cases"].append(
            {
                "case": case,
                "codecs": ["json", "yaml"],
                "config_hash": config.fingerprint,
                "result": result,
            }
        )

    class DeniedSink:
        api_version = 1
        calls = 0

        def write(self, result):
            self.calls += 1
            if result.status != "passed":
                raise AssertionError("Denied sink must receive a successful comparison")
            raise PermissionError("Controlled qualification persistence denial")

    sink = DeniedSink()
    try:
        PairityEngine(factory, execution=InlineExecution(), sink=sink).run(
            suite("denied-persistence", "nulls", "nulls")
        )
    except PermissionError as exc:
        if str(exc) != "Controlled qualification persistence denial" or sink.calls != 1:
            raise AssertionError("Unexpected persistence failure") from exc
        evidence["denied_persistence"] = {"adapter": "controlled", "propagated": True}
    else:
        raise AssertionError("Persistence denial was swallowed")
    return evidence


def artifact_path(artifact_root, test_id):
    if not re.fullmatch(r"gha_[0-9]+_[0-9]+", test_id):
        raise ValueError("Invalid test identity")
    root = artifact_root.rstrip("/")
    if not re.fullmatch(r"/Volumes/[^/]+/[^/]+/[^/]+(?:/[^/]+)*", root) or any(
        part in {".", ".."} for part in root.split("/")
    ):
        raise ValueError("artifact_root must be an absolute Unity Catalog Volume path")
    return root + "/smoke/" + test_id + "/config-qualification.json"


def qualify(spark, dbutils):
    from databricks.sdk import WorkspaceClient

    test_id = dbutils.widgets.get("test_id")
    path = artifact_path(dbutils.widgets.get("artifact_root"), test_id)
    evidence = {
        "schema_version": 1,
        "test_id": test_id,
        "status": "passed",
        **run_qualification(spark),
    }
    payload = json.dumps(evidence, sort_keys=True, allow_nan=False).encode()
    if len(payload) > 128 * 1024:
        raise ValueError("Qualification evidence exceeded 128 KiB")
    WorkspaceClient().files.upload(path, io.BytesIO(payload), overwrite=False)
    print(f"Verified {len(evidence['cases'])} cases in JSON and YAML: {path}")


if __name__ == "__main__":
    qualify(spark, dbutils)  # noqa: F821
