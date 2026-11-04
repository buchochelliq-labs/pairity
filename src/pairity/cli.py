"""Config management and Databricks Job entry point over the same library API."""

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

from pairity.bulk import patch_pairs, preview_update
from pairity.config import SuiteConfig
from pairity.config_storage import FileConfigStore, RevisionConflict
from pairity.serialization import dump_config, load_config, load_document


def _postgres_documents():
    from psycopg import connect

    from pairity.postgres import PostgresDocumentStore

    dsn = os.environ.get("PAIRITY_POSTGRES_DSN")
    if not dsn:
        raise ValueError(
            "Set PAIRITY_POSTGRES_DSN or supply a connection factory through the library"
        )
    return PostgresDocumentStore(
        lambda: connect(dsn), schema=os.environ.get("PAIRITY_POSTGRES_SCHEMA", "pairity")
    )


def _config_store(location):
    if location.startswith("postgres:"):
        from pairity.postgres import PostgresConfigStore

        suite = location.removeprefix("postgres:")
        if not suite:
            raise ValueError("Use postgres:SUITE_NAME")
        return PostgresConfigStore(_postgres_documents(), suite)
    return FileConfigStore(location)


def _read(path, *, document=False):
    file = Path(path)
    loader = load_document if document else load_config
    return loader(file.read_text(encoding="utf-8"), file.suffix.lstrip("."))


def _output(text, destination=None):
    if destination is None:
        print(text, end="" if text.endswith("\n") else "\n")
        return
    path = Path(destination)
    pending = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, prefix=".pairity-", delete=False
        ) as stream:
            pending = Path(stream.name)
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(pending, path)
    finally:
        if pending is not None:
            pending.unlink(missing_ok=True)


def _json(value, destination=None):
    _output(json.dumps(value, indent=2, allow_nan=False) + "\n", destination)


def _parser():
    parser = argparse.ArgumentParser(prog="pairity", description="Data regression testing")
    parser.add_argument("--json", action="store_true", help="Machine-readable JSON output")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("validate", help="Validate a JSON/YAML suite").add_argument("source")
    commands.add_parser("init-postgres", help="Provision Pairity application-state tables")
    commands.add_parser("init-results", help="Provision a Delta result table").add_argument("table")
    commands.add_parser("schema", help="Print the configuration JSON Schema")
    commands.add_parser("inspect", help="Explore a saved run with intuiTUIve").add_argument(
        "source"
    )
    convert = commands.add_parser("convert", help="Convert a suite between JSON and YAML")
    convert.add_argument("source")
    export = commands.add_parser("export", help="Export the latest or a historical config revision")
    export.add_argument("--store", required=True)
    export.add_argument("--revision", type=int)
    for command in (convert, export):
        command.add_argument("--format", choices=["json", "yaml", "yml"], required=True)
        command.add_argument("--output")
    for name in ("import", "patch"):
        command = commands.add_parser(name, help=f"Preview or apply a bulk {name}")
        command.add_argument("source")
        command.add_argument("--store", required=True)
        command.add_argument("--expected-revision", type=int, required=True)
        command.add_argument("--apply", action="store_true")
        if name == "patch":
            selection = command.add_mutually_exclusive_group(required=True)
            selection.add_argument("--pair-id", action="append")
            selection.add_argument("--tag", action="append")
    submit = commands.add_parser("submit", help="Submit a suite to an existing Databricks Job")
    submit.add_argument("source")
    submit.add_argument("--job-id", type=int, required=True)
    submit.add_argument("--artifact-root", required=True)
    submit.add_argument("--request-id", required=True, help="Stable unique submission token")
    for name in ("job-status", "job-cancel"):
        command = commands.add_parser(name)
        command.add_argument("--job-id", type=int, required=True)
        command.add_argument("--run-id", required=True)
    run = commands.add_parser("run", help="Compare Delta tables on the active Spark runtime")
    run.add_argument("source")
    run.add_argument("--max-parallel-pairs", type=int)
    selection = run.add_mutually_exclusive_group()
    selection.add_argument("--pair-id", action="append")
    selection.add_argument("--tag", action="append")
    run.add_argument("--output", help="JSON run report; difference DataFrames stay distributed")
    run.add_argument("--result-table", help="Existing Pairity Delta result table")
    run.add_argument(
        "--retain-differences",
        action="store_true",
        help="Persist row-level differences with results",
    )
    return parser


def _bulk(args):
    store = _config_store(args.store)
    try:
        previous = store.load()
    except FileNotFoundError:
        previous = None
    revision = previous.revision if previous else 0
    if args.expected_revision != revision:
        raise RevisionConflict(f"Expected revision {args.expected_revision}, found {revision}")
    if args.command == "import":
        imported = _read(args.source)
        updated = imported
        if previous:
            if imported.suite != previous.config.suite:
                raise ValueError("Cannot import a different suite into an existing store")
            merged = {p.id: p for p in previous.config.pairs}
            merged.update({p.id: p for p in imported.pairs})
            updated = SuiteConfig.model_validate(
                imported.model_dump(exclude_none=True)
                | {"pairs": [p.model_dump(exclude_none=True) for p in merged.values()]}
            )
    else:
        if previous is None:
            raise ValueError("Import a suite before patching it")
        updated = patch_pairs(
            previous.config, _read(args.source, document=True), ids=args.pair_id, tags=args.tag
        )
    preview = (
        preview_update(previous.config, updated)
        if previous
        else {"added": [p.id for p in updated.pairs], "changed": [], "unchanged": [], "removed": []}
    )
    settings_changed = bool(
        previous
        and (
            previous.config.defaults != updated.defaults
            or previous.config.execution != updated.execution
            or previous.config.templates != updated.templates
        )
    )
    if args.apply:
        revision = store.save(updated, expected_revision=args.expected_revision).revision
    _display(
        {
            "applied": args.apply,
            "revision": revision,
            "preview": preview,
            "suite_settings_changed": settings_changed,
            "config_hash": updated.fingerprint,
        },
        args.json,
    )


def _run(args):
    if args.retain_differences and not args.result_table:
        raise ValueError("--retain-differences requires --result-table")
    config = _read(args.source)
    if args.max_parallel_pairs is not None:
        data = config.model_dump(exclude_none=True)
        data["execution"]["max_parallel_pairs"] = args.max_parallel_pairs
        config = SuiteConfig.model_validate(data)
    # Optional runtime dependencies are loaded only for actual comparisons.
    from pyspark.sql import SparkSession

    from pairity.spark import create_spark_engine

    spark = SparkSession.builder.getOrCreate()
    sink = None
    if args.result_table:
        from pairity.delta_storage import DeltaResultStore

        sink = DeltaResultStore(
            spark, args.result_table, retain_differences=args.retain_differences
        )
    result = create_spark_engine(spark, sink=sink).run(config, pair_ids=args.pair_id, tags=args.tag)
    if args.output:
        _json(result.to_dict(), args.output)
    _display(result.to_dict(), args.json)
    return {"passed": 0, "failed": 1, "blocked": 2, "error": 2, "skipped": 3}[result.status]


def main(argv=None):
    args = _parser().parse_args(argv)
    try:
        if args.command in {"submit", "job-status", "job-cancel"}:
            from dataclasses import asdict

            from databricks.sdk import WorkspaceClient

            from pairity.artifacts import VolumeArtifactStore
            from pairity.submission import DatabricksJobSubmitter, RunHandle, VolumeConfigPublisher

            client = WorkspaceClient()
            publisher = (
                VolumeConfigPublisher(VolumeArtifactStore(args.artifact_root, client))
                if args.command == "submit"
                else None
            )
            submitter = DatabricksJobSubmitter(client, args.job_id, publisher)
            if args.command == "submit":
                handle = submitter.submit(_read(args.source), request_id=args.request_id)
                _display(asdict(handle), args.json)
            else:
                handle = RunHandle("databricks-jobs", args.run_id)
                if args.command == "job-cancel":
                    submitter.cancel(handle)
                    _display({"cancel_requested": args.run_id}, args.json)
                else:
                    _display(submitter.status(handle), args.json)
        elif args.command == "init-postgres":
            _postgres_documents().initialize()
            _display({"initialized": "postgres"}, args.json)
        elif args.command == "init-results":
            from pyspark.sql import SparkSession

            from pairity.delta_storage import DeltaResultStore

            DeltaResultStore(SparkSession.builder.getOrCreate(), args.table).initialize()
            _display({"initialized": args.table}, args.json)
        elif args.command == "validate":
            config = _read(args.source)
            _display(
                {
                    "valid": True,
                    "suite": config.suite,
                    "pairs": len(config.pairs),
                    "config_hash": config.fingerprint,
                },
                args.json,
            )
        elif args.command == "schema":
            from pairity.schema import configuration_schema

            _json(configuration_schema())
        elif args.command == "convert":
            _output(dump_config(_read(args.source), args.format), args.output)
        elif args.command == "export":
            config = FileConfigStore(args.store).load(args.revision).config
            _output(dump_config(config, args.format), args.output)
        elif args.command in ("import", "patch"):
            _bulk(args)
        elif args.command == "run":
            return _run(args)
        elif args.command == "inspect":
            from pairity.terminal import create_report_app

            create_report_app(_read(args.source, document=True)).run()
        return 0
    except Exception as exc:
        if args.json:
            print(
                json.dumps({"error_type": type(exc).__name__, "error": str(exc)}), file=sys.stderr
            )
        else:
            from pairity.terminal import render_error

            render_error(exc)
        return 2


def _display(report, machine_json):
    if machine_json:
        _json(report)
    else:
        from pairity.terminal import render_report

        render_report(report)


def job_main(argv=None):
    """Wheel tasks invoke functions directly, so nonzero results must raise."""
    code = main(argv)
    if code:
        raise RuntimeError(f"Pairity finished with exit code {code}; see the run report")
