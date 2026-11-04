# Databricks deployment and submission

The default bundle implements serverless preparation, parallel pair tasks and aggregation. It is locally tested and matches the Databricks CLI 1.23.0 bundle JSON Schema. Full `bundle validate` stops at missing workspace credentials here; the bundle has not been deployed or qualified in a live workspace. See [Free Edition acceptance tests](free-edition.md).

See [test access setup](databricks-test-access.md) and the [Tuva synthetic datasets](../examples/tuva/README.md) for the CI sandbox.

## Prerequisites

- A workspace with serverless Jobs and environment version 5 available.
- Read access to input Delta tables, history and (for inferred PKs) UC constraint metadata.
- An existing UC Volume for immutable configurations/manifests and an initialized Delta results table. Provision the latter explicitly using `DeltaResultStore(spark, "catalog.schema.results").initialize()`.
- Databricks CLI authentication, permission to deploy/run Jobs, and read/write privileges on the chosen result table and Volume.
- Build tooling installed locally. Install Pairity on Databricks without its local `spark` extra.

```bash
databricks bundle validate \
  --var config_path=/Volumes/CATALOG/SCHEMA/VOLUME/suite.yaml \
  --var artifact_root=/Volumes/CATALOG/SCHEMA/VOLUME/pairity \
  --var result_table=CATALOG.SCHEMA.pairity_results
# Use the same variables with bundle deploy, then bundle run regression.
```

The Job is unscheduled. `parallel_pairs` defaults to 2 and must be no larger than the suite's `execution.max_parallel_pairs`. Increase it only within available account capacity. The preparation task rejects an inconsistent limit instead of exceeding the suite's setting.

Preparation reuses the first published manifest for the Job run. On first execution it validates the JSON/YAML config, expands reusable sources, pins unversioned leaf tables once across the suite, and publishes an immutable Volume manifest. `For each` schedules one pair per task, without nested threading or SparkContext. Aggregation runs with `ALL_DONE`; absent outcomes become errors and invalid/duplicate child identities fail validation. A suite with only disabled pairs also fails its final task.

Delta child run IDs are `job:<job-run-id>:pair:<zero-based-index>`; the suite summary uses `job:<job-run-id>`. Each child holds the full pinned configuration and its pair result. The parent holds all pair summaries. Optional row evidence remains under the child IDs. No Postgres credentials are needed by Spark tasks.

By default, only summaries are persisted, and no DataFrame caching preview is required. Set `retain_differences=true` only where serverless DataFrame caching is enabled (currently preview, environment 5+); publication needs that snapshot guarantee. A missing capability fails publication rather than weakening evidence checks.

Automatic task retries are disabled until attempt lifecycle support (#7). Explicit worker replays reuse existing completed evidence after checking its config and pair identity. Failures before publication can be retried manually; a published blocked/error result is immutable. Start a **new Job run** to rerun or correct a completed suite. Repairing an already aggregated Job in place is not supported yet (#7); aggregation records cannot be rewritten. Cancellation is available through Jobs, but a cancelled run may have no parent summary; consult the Job lifecycle and published children.

## Submission methods

Submission is separate from pair execution. All adapters implement the versioned `SubmissionBackend` protocol (`submit`, `status`, `cancel`).

| Method | Implementation | Execution location |
| --- | --- | --- |
| Python / notebook | `DirectSubmitter(engine)` or `engine.run(config)` | Supplied Spark runtime |
| CLI direct | `pairity run suite.yaml` | Active classic Spark runtime |
| CLI remote | `pairity submit suite.yaml --job-id ...` | Existing serverless Job |
| Python SDK / external orchestrator | `DatabricksJobSubmitter` | Existing serverless Job |
| Custom scheduler | Implement `SubmissionBackend` | Adapter-defined |

```bash
# Install .[databricks] and configure normal SDK authentication first.
pairity --json submit suite.yaml --job-id 123 \
  --artifact-root /Volumes/CATALOG/SCHEMA/VOLUME/configs \
  --request-id unique-logical-submission-1
pairity --json job-status --job-id 123 --run-id 456
pairity --json job-cancel --job-id 123 --run-id 456
```

Use a unique request ID per logical submission; repeat the same ID only for an identical retry. Jobs enforces its idempotency-token semantics. Remote submission publishes a validated config snapshot and sends its hash; preparation verifies the hash before pinning. SDK authentication stays with the caller, outside YAML/JSON configs.

```python
from databricks.sdk import WorkspaceClient
from pairity.artifacts import VolumeArtifactStore
from pairity.submission import DatabricksJobSubmitter, VolumeConfigPublisher

client = WorkspaceClient()
submitter = DatabricksJobSubmitter(
    client, 123,
    VolumeConfigPublisher(VolumeArtifactStore('/Volumes/catalog/schema/volume/configs', client)),
)
handle = submitter.submit(config, request_id='pipeline-2026-11-04-1')
print(submitter.status(handle))
```

The direct adapter is synchronous and its handles are process-local; use a durable engine sink for history. Classic Spark uses the existing parallel engine. `create_task_engine(spark)` is the single-pair Connect composition root for notebook/Job wrappers; it deliberately rejects multi-pair execution. Use Job fan-out for a parallel serverless suite.

REST/API hosting, Airflow-specific operators, SQL warehouse execution and standalone Spark-submit packaging are not included; callers can use the SDK adapter or supply another submission plugin. No deployment mode silently falls back from requested parallelism to serial execution.

## Live qualification

Before relying on results, validate and deploy in a sandbox, run the two-pair [smoke fixture](../examples/serverless-smoke.sql), then follow the failure, permission, source-version and task-overlap checks in [free-edition.md](free-edition.md). Local Spark tests cannot establish serverless dependency installation, current bundle/API acceptance or workspace permissions.
