import hashlib
import io
import json
import zipfile

import pytest

from pairity.config import SuiteConfig
from pairity.results import PairResult, RunResult


def report():
    config = SuiteConfig.model_validate(
        {
            "schema_version": 1,
            "suite": "example",
            "pairs": [
                {
                    "id": name,
                    "baseline": f"cat.sch.{name}",
                    "candidate": f"cat.new.{name}",
                    "keys": [{"baseline": "id", "candidate": "id"}],
                }
                for name in ("a", "private_pair")
            ],
        }
    )
    pairs = [
        PairResult(
            p.id,
            "failed",
            counts={
                "baseline_rows": 3,
                "candidate_rows": 3,
                "matched_rows": 3,
                "changed_rows": 1,
                "missing_rows": 0,
                "extra_rows": 0,
            },
            inputs={
                side: {"table": getattr(p, side), "version": 7, "snapshot_pinned": True}
                for side in ("baseline", "candidate")
            },
            keys=[{"baseline": "id", "candidate": "id"}],
        )
        for p in config.pairs
    ]
    return RunResult(
        "run",
        "example",
        config.fingerprint,
        config.model_dump(exclude_none=True),
        pairs,
        "start",
        "end",
    ).to_dict()


def exporter(**kwargs):
    from pairity.incidents import IncidentExporter

    return IncidentExporter(**kwargs)


def test_selected_export_manifest_and_deterministic_zip():
    files = exporter().export(report(), pair_ids=["a"])
    assert {
        "ticket.md",
        "summary.png",
        "summary.html",
        "summary.txt",
        "result.json",
        "resolved.json",
        "resolved.yaml",
        "manifest.json",
        "reproduce.py",
        "reproduce.sql",
    } <= files.keys()
    assert files["summary.png"].startswith(b"\x89PNG")
    assert all(b"private_pair" not in value for value in files.values())
    manifest = json.loads(files["manifest.json"])
    assert manifest["manifest_schema_version"] == 1
    for name, metadata in manifest["files"].items():
        assert hashlib.sha256(files[name]).hexdigest() == metadata["sha256"]
        assert len(files[name]) == metadata["size_bytes"]
    archive = exporter().zip_bytes(files)
    assert archive == exporter().zip_bytes(dict(reversed(list(files.items()))))
    with zipfile.ZipFile(io.BytesIO(archive)) as zipped:
        assert zipped.read("ticket.md") == files["ticket.md"]
    resolved = json.loads(files["resolved.json"])
    assert resolved["pairs"][0]["baseline"]["version"] == 7
    assert b"UNSUPPORTED" in files["reproduce.sql"]
    compile(files["reproduce.py"], "reproduce.py", "exec")


@pytest.mark.parametrize("status", ["blocked", "error", "skipped"])
def test_incomplete_status_never_claims_counts(status):
    data = report()
    data["pairs"][0]["status"] = status
    files = exporter().export(data, pair_ids=["a"])
    assert json.loads(files["result.json"])["pairs"][0]["counts"] is None
    assert b"Counts unavailable" in files["ticket.md"]


def test_missing_pin_blocks_reproduction_without_resolving_latest():
    data = report()
    data["pairs"][0]["inputs"]["baseline"]["version"] = None
    files = exporter().export(data, pair_ids=["a"])
    assert not json.loads(files["manifest.json"])["reproduction"]["ready"]
    assert b"raise SystemExit" in files["reproduce.py"]


def test_samples_opt_in_bounded_masked_and_secrets_removed():
    data = report()
    data["pairs"][0]["reason"] = "failure token=abc123"
    samples = {
        "a": [
            {"id": i, "email": "secret@example.org", "nested": {"token": "secret"}}
            for i in range(5)
        ],
        "private_pair": [{"id": "unrelated"}],
    }
    assert "samples.json" not in exporter().export(data, samples=samples)
    files = exporter(include_samples=True, max_sample_rows=2, mask_columns=["email"]).export(
        data, pair_ids=["a"], samples=samples
    )
    rows = json.loads(files["samples.json"])["a"]
    assert len(rows) == 2
    assert rows[0]["email"] == "[REDACTED]"
    assert all(
        b"secret@example.org" not in value and b"abc123" not in value for value in files.values()
    )


def test_reject_unknown_pair_and_unsafe_archive_paths():
    with pytest.raises(ValueError):
        exporter().export(report(), pair_ids=["unknown"])
    with pytest.raises(ValueError):
        exporter().zip_bytes({"../escape": b"bad"})


def test_templates_union_and_dedup_pin_each_leaf_and_remove_unused_templates():
    data = report()
    data["configuration"]["templates"] = {
        "used": {
            "parameters": [],
            "source": {
                "kind": "deduplicate",
                "source": {"kind": "union", "sources": ["cat.sch.a", "cat.sch.a"]},
            },
        },
        "unused": {"parameters": [], "source": "cat.secret.unused"},
    }
    data["configuration"]["pairs"][0]["baseline"] = {
        "kind": "template",
        "name": "used",
        "inputs": {},
    }
    data["pairs"][0]["inputs"]["baseline"] = {
        "table": "deduplicate",
        "snapshot_pinned": True,
        "leaves": [
            {"table": "cat.sch.a", "snapshot_pinned": True, "version": value} for value in (2, 3)
        ],
    }
    files = exporter().export(data, pair_ids=["a"])
    config = json.loads(files["resolved.json"])
    children = config["pairs"][0]["baseline"]["source"]["sources"]
    assert [child["version"] for child in children] == [2, 3]
    assert config["templates"] == {}
    assert all(b"cat.secret.unused" not in value for value in files.values())
    assert b"VERSION AS OF 2" in files["reproduce.sql"]
    assert b"VERSION AS OF 3" in files["reproduce.sql"]
    assert b"Union-by-name SQL needs recorded column order" in files["reproduce.sql"]
    assert json.loads(files["manifest.json"])["reproduction"]["ready"]


def test_source_pin_conflict_blocks_reproduction():
    data = report()
    data["configuration"]["pairs"][0]["baseline_version"] = 8
    files = exporter().export(data, pair_ids=["a"])
    assert not json.loads(files["manifest.json"])["reproduction"]["ready"]
    assert b"conflicting" in files["reproduce.py"]


def test_export_is_repeatable_and_does_not_mutate_report():
    data = report()
    before = json.dumps(data, sort_keys=True)
    first = exporter().export(data)
    second = exporter().export(data)
    assert first == second
    assert json.dumps(data, sort_keys=True) == before


@pytest.mark.parametrize("column", ["token", "password", "secret"])
def test_sensitive_column_names_preserve_rules_counts_and_reproduction(column):
    data = report()
    rules = {column: {"compare_as": "string", "null_safe": True}}
    data["configuration"]["pairs"][0]["compare"] = {"column_rules": rules}
    data["pairs"][0]["column_changes"] = {column: 1}
    data["pairs"][0]["effective_settings"] = {"column_rules": rules}
    data["pairs"][0]["reason"] = "failure token=hidden-diagnostic-value"
    data["pairs"][0]["schema_differences"] = ["failure password=hidden-schema-value"]
    files = exporter(include_samples=True).export(
        data, pair_ids=["a"], samples={"a": [{column: "hidden-sample-value"}]}
    )
    resolved = json.loads(files["resolved.json"])
    SuiteConfig.model_validate(resolved)
    assert resolved["pairs"][0]["compare"]["column_rules"] == rules
    result = json.loads(files["result.json"])["pairs"][0]
    assert result["column_changes"] == {column: 1}
    assert result["effective_settings"]["column_rules"] == rules
    assert json.loads(files["manifest.json"])["reproduction"]["ready"]
    assert json.loads(files["samples.json"])["a"][0][column] == "[REDACTED]"
    assert all(
        b"hidden-diagnostic-value" not in value
        and b"hidden-schema-value" not in value
        and b"hidden-sample-value" not in value
        for value in files.values()
    )
