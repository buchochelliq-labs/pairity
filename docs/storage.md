# Hybrid storage

Pairity separates application state, regression evidence and downloadable files, following DQX Studio's storage boundaries. The adapters are implemented here; live Lakebase, Unity Catalog permissions and deployment still require workspace verification.

| Data | Hosted storage | Adapter |
|---|---|---|
| Suites/templates/config revisions; application settings, schedule definitions, incident and artifact references | PostgreSQL / Lakebase | `PostgresDocumentStore`, `PostgresConfigStore` |
| Completed run reports, exact counts, provenance, optional distributed row differences | Managed Unity Catalog Delta table | `DeltaResultStore` |
| SQL, images, instructions, ZIPs and other evidence files | One or more Unity Catalog Volumes | `VolumeArtifactStore` |

Local configuration and artifact adapters remain available for development. Schedule and incident document namespaces are storage primitives, not an implemented scheduler or ticketing integration. Artifact content generation is tracked in #19; this change supplies storage and verified download.

## Postgres / Lakebase

Install `pairity-data[postgres]`. Supply a connection factory returning a fresh psycopg 3 connection per operation. The host owns authentication, TLS and token refresh; a Lakebase integration can obtain a fresh OAuth credential inside that factory. Do not put credentials in suite YAML/JSON.

```python
import psycopg
from pairity.postgres import PostgresDocumentStore, PostgresConfigStore

# connection_parameters is supplied by the deployment's credential provider.
documents = PostgresDocumentStore(lambda: psycopg.connect(**connection_parameters), schema="pairity")
documents.initialize()  # explicit provisioning, normally a separate deployment identity
store = PostgresConfigStore(documents, "daily-regression")
# store.save(config, expected_revision=0)
# pinned = store.load(revision=1)
```

The schema contains `schema_versions`, `document_heads` and `document_revisions`. Document kinds are `suite`, `settings`, `schedule`, `incident` and `artifact`. Each revision retains timestamp, exact canonical JSON, queryable JSONB and SHA-256. Keeping canonical JSON separately preserves numeric representations through JSONB normalization. Version 1 is the initial schema; unknown schema versions are rejected during provisioning.

A save locks the document head in a transaction, checks `expected_revision`, inserts the immutable revision and advances the head. Concurrent creation is serialized by the primary key and row lock. A stale revision rolls back the whole transaction. The application never updates or deletes revision rows. Deployment should grant runtime SELECT/INSERT on revisions, SELECT/INSERT/UPDATE on heads and schema usage, keeping DDL and retention privileges separate.

CLI imports, patches and exports accept `--store postgres:SUITE_NAME` in addition to local directories. Set `PAIRITY_POSTGRES_DSN` and optionally `PAIRITY_POSTGRES_SCHEMA` in the calling environment. `pairity init-postgres` is the explicit provisioning command. Use the library connection factory when rotating credentials dynamically. Export a fixed revision for a Job; workers do not need Postgres access.

## Delta results

On a Databricks Spark runtime:

```bash
pairity init-results catalog.pairity.results
pairity run suite.yaml --result-table catalog.pairity.results --retain-differences
```

`--retain-differences` is explicit because it stores row values; omit it to persist summaries only. `--output run.json` remains available independently. Result persistence failure fails the run rather than reporting success.

The result table uses a versioned envelope with `record_type`, `run_id`, `suite`, `pair_id`, `finished_at`, `report_hash` and `payload`. A `report` record contains the full resolved config/hash, pair counts/statuses, keys, pinned input versions, cast decisions, ignored columns and runtime versions. `difference` records contain JSON with baseline/candidate rows, kind and changed columns. Heterogeneous source schemas therefore fit the same table without altering its schema.

The report and differences are committed together in **one Delta transaction**. This avoids pretending that writes to separate run, pair and evidence tables are atomic. Read views can expose those logical entities later. Table provisioning is explicit and checks the expected column schema; normal writes require the table to exist.

Stable Delta transaction IDs make retry appends idempotent. Stored report hashes and exact distributed multiset checks reject conflicting retries. Before publication, retained evidence counts must agree with the reported missing/extra/changed counts. The candidate is persisted during validation/write and unpersisted afterward. These correctness checks add Spark work, particularly when retaining large difference sets.

```python
from pairity.delta_storage import DeltaResultStore

results = DeltaResultStore(spark, "catalog.pairity.results", retain_differences=True)
# Inject as create_spark_engine(spark, sink=results).
report = results.load(run_id)  # bounded report lookup; does not collect row evidence
rows = results.differences(run_id, pair_id)  # distributed DataFrame with JSON payload
history = results.history(suite="daily-regression", limit=100)
```

Requesting differences from a summary-only or unexecuted comparison raises; an empty response must not imply that missing evidence proved equality. This sink stores completed runs, not live scheduling/attempt state. A retry must retain the run ID and exact evidence; rerunning the engine creates a new run. Automated retention/cleanup and dashboard views remain follow-up work in #8. Any external retention policy must remove a whole run's records consistently; never remove just the evidence while leaving a report claiming it was retained.

## Artifacts in one or more UC Volumes

Install `pairity-data[databricks]` for the workspace client. Configure separate store instances for evidence classes, environments or access policies:

```python
from dataclasses import asdict
from databricks.sdk import WorkspaceClient
from pairity.artifacts import VolumeArtifactStore

workspace = WorkspaceClient()
artifacts = VolumeArtifactStore("/Volumes/catalog/pairity/incident_evidence", workspace)
reference = artifacts.publish(run_id, {
    "summary.json": summary_bytes,
    "reproduce.sql": sql_bytes,
    "steps.md": instructions_bytes,
})
# Persist asdict(reference) as application-state metadata, never the file bytes.
files = artifacts.download(reference)  # verified dict[str, bytes] for a download response
```

The existing volume must be provisioned with the required UC catalog/schema usage and volume read/write privileges. The Files API adapter uses absolute `/Volumes/catalog/schema/volume[/prefix]` paths, not DBFS root or workspace-local ephemeral storage. It does not require a local FUSE mount or assume rename/locking is atomic on cloud storage.

Each publication receives a unique directory, uploads files without overwriting, then uploads a manifest last. The returned reference pins the manifest hash. Downloads validate the manifest, file sizes and SHA-256 checksums; missing/corrupt files fail visibly. Limits are 128 files, 32 MiB per file and 128 MiB per bundle. Images, SQL and ticket instructions can be separate artifacts or contained in a ZIP generated by the exporter. Interrupted uploads may leave unpublished directories; retention/orphan cleanup must be scheduled separately. No ticket is submitted by these stores.

For offline development, `LocalArtifactStore(path)` implements the same publish/download contract. It is not the hosted default and is not used for distributed locking.

## Verification

Real local Delta tests require `PAIRITY_TEST_DELTA=1`; install `delta-spark==3.3.2` with the supported local Spark version. `PAIRITY_TEST_DELTA_JARS` optionally supplies comma-separated absolute JAR paths for offline testing. Real PostgreSQL tests require `PAIRITY_TEST_POSTGRES_DSN`. CI provisions PostgreSQL 16 and runs both adapters. The Files API transport is tested with an in-memory service double; real UC Volume permissions and Lakebase authentication must be checked in a Databricks workspace.
