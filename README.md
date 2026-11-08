# Pairity

Configurable data regression testing, starting with Databricks.

Pairity follows the DQX library and workspace-tool architecture, with independent, pluggable implementations and interchangeable JSON/YAML configuration.

## What works now

- Compare multiple table pairs **in parallel**, with `execution.max_parallel_pairs` (default 4, range 1–64).
- Parse, validate, convert, import, export and bulk-patch one shared JSON/YAML configuration model.
- Resolve explicit single/composite PK mappings or declared Unity Catalog PKs. Missing, null or duplicate keys never produce a passing comparison.
- Compare schemas and scalar column values with distributed Spark, reporting missing, extra and changed rows and per-column counts.
- Compose union/deduplication sources, guarded read-only SQL queries over pinned inputs with bound parameters, and reusable parameterized JSON/YAML templates.
- Compare nested structs (by field name), maps (by content) and arrays (ordered or as multisets), apply exact decimal/integral tolerances, and extend comparisons with registered column comparators.
- Compare common columns or exclude named columns; apply audited implicit casts by default, with suite/pair/column opt-outs and explicit `compare_as` directives. Configure null equality at the same levels.
- Persist application config revisions in Postgres/Lakebase, results in Delta, and downloadable evidence files in UC Volumes through replaceable [storage adapters](docs/storage.md).
- Pin Delta inputs to recorded table versions, or to the version at an explicit `as_of` instant, and scope rows with typed, validated `where` filters. Compared tables are read-only.
- Compose metadata providers, readers, comparison backends, execution backends, configuration stores and result sinks through versioned interfaces.
- Render CLI reports with **rs-rich's Rust-backed Python bindings**, and inspect saved runs with **intuiTUIve**.
- Use the Databricks workbench to import/edit configurations, preview revisions, submit Jobs, review results and download incident evidence. Browse Unity Catalog Delta tables and validate primary-key mappings in the guided editor.

The serverless Job smoke and hosted App startup have succeeded on Databricks Free Edition. End-to-end App permission qualification, automated retention, retry policies and optional DQX integration remain tracked in the [issues](https://github.com/buchochelliq-labs/pairity/issues). No DQX runtime dependency is installed.

## Install and validate

From a checkout:

```bash
python -m pip install -e '.[dev,spark]'
pairity validate examples/suite.yaml
pairity convert examples/suite.yaml --format json --output suite.json
pairity --json validate suite.json
```

On Databricks, install the built wheel **without the `spark` extra** so the platform's PySpark is retained. Local Spark tests use PySpark 3.5.8 and Java 17. Classic Spark uses pinned threads. The serverless bundle uses isolated Connect-compatible pair tasks and Job-level parallelism. See [deployment and submission methods](docs/deployment.md).

## Run comparisons

See [reusable sources and comparison rules](docs/sources-and-comparison.md) and [the template example](examples/templates.yaml) for composed inputs.

Edit the table names and PKs in [examples/suite.yaml](examples/suite.yaml), then on a Databricks Spark runtime:

```bash
pairity run suite.yaml --max-parallel-pairs 4 --output run.json
pairity inspect run.json
```

Use `--json` before the command for machine-readable output. `--pair-id` or `--tag` selects a subset and can be repeated. Results retain configuration order regardless of completion order. The JSON report contains summary metadata, not source row values. In the library, `pair_result.differences` is a distributed DataFrame for drill-down or an explicitly configured result sink.

```python
from pathlib import Path
from pairity import load_config
from pairity.spark import create_spark_engine

config = load_config(Path("suite.yaml").read_text(), "yaml")
result = create_spark_engine(spark).run(config)
print(result.status)
```

Set `spark.scheduler.mode=FAIR` in the compute configuration before Spark starts when pairs should share cluster capacity. Pairity submits bounded concurrent work, assigns per-pair scheduler pools/job groups and creates separate Spark sessions. Available cluster resources still govern throughput. It does not silently run a requested parallel suite through a serial executor.

CLI exit codes: **0** passed, **1** regression failed, **2** blocked/configuration/runtime/persistence error, **3** no comparison executed. The `pairity-job` wheel entry point raises for nonzero results so Databricks Jobs fail correctly.

## Bulk configuration changes

```bash
# Preview first; --apply commits a validated, atomic revision.
pairity import examples/suite.yaml --store .pairity-config --expected-revision 0
pairity import examples/suite.yaml --store .pairity-config --expected-revision 0 --apply
pairity patch examples/disable-sales.yaml --store .pairity-config --expected-revision 1 --tag sales
pairity export --store .pairity-config --revision 1 --format yaml --output historical.yaml
```

Imports upsert by stable pair ID and preserve omitted pairs. The local store rejects stale revisions and concurrent writers. It is a local-filesystem adapter, not a distributed lock for UC Volumes. See [configuration](docs/configuration.md) for inheritance and update semantics.

## Databricks deployment

[databricks.yml](databricks.yml) defines an unscheduled serverless Job with preparation, parallel pair tasks and aggregation; [the App bundle](app/databricks.yml) deploys the workbench and Lakebase state. The Free Edition [Job smoke](https://github.com/buchochelliq-labs/pairity/actions/runs/41833551263) and [App deployment/startup](https://github.com/buchochelliq-labs/pairity/actions/runs/42100391510) succeeded. See [deployment](docs/deployment.md) and [workbench qualification](docs/workbench.md) for setup and remaining live checks.

## Development

```bash
python -m pytest
ruff check src tests
ruff format --check src tests
python -m build
```

The tests include real Spark comparisons, barrier-proven concurrent execution, per-worker Spark context isolation, JSON/YAML equivalence, revision conflicts and an intuiTUIve navigation/quit test.

See [architecture and plugins](docs/architecture.md) and [configuration schema](schema/pairity.schema.json).

## Workbench and evidence

The [Databricks workbench](docs/workbench.md) adds configuration upload/editing, parallel Job submission, run history and incident downloads over the same core. Incident bundles include exact comparison SQL that reproduces the recorded counts for every pair it can render faithfully. See [incident exports](docs/incidents.md) and the [expanded qualification matrix](docs/config-qualification.md). Live deployment of these additions is a separate release gate.
