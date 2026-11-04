# Testing and deploying on Databricks Free Edition

Verified against official documentation on 4 November 2026. This is a compatibility assessment and deployment plan; no Free Edition workspace was accessed or deployed to.

## Current compatibility

Free Edition provides serverless compute only, without custom classic clusters. Its documented account limit is five concurrent Job tasks. It supports up to three Apps, which automatically stop after 24 hours and can be restarted. Usage quotas and restricted outbound internet access also apply.

The current `databricks.yml` requires an existing classic cluster. `SparkExecutionBackend` uses SparkContext/JVM thread properties, and `SparkTableReader.scope` sets classic job groups/pools. Serverless exposes Spark Connect APIs and does not expose SparkContext. Therefore the current engine composition and bundle are **not Free Edition compatible**. Schema validation and config management can be tested independently; the DataFrame comparison core is a candidate for reuse, subject to live Connect tests.

## Target architecture: DQX-style serverless Jobs

DQX Studio separates its App/API from a serverless Databricks task-runner Job; Spark does not run in the App process. Pairity should follow this execution boundary. The CLI or hosted App submits a Job, thin notebook/Python wheel tasks call the library, and the Job orchestrates parallel comparisons and aggregation. Task wrappers stay separate from comparison logic, allowing other task types where appropriate. This is the intended hosted deployment architecture, rather than a Free Edition workaround. The existing classic adapter remains an optional execution implementation.

## Deployment implementation route

1. Build the wheel locally. Upload it and small JSON/YAML fixture configs to workspace files or a Unity Catalog Volume. Install it using the serverless notebook environment or Job environment dependencies. Retain Databricks' PySpark; do not install Pairity's local `spark` extra. Verify the native rs-rich wheel installs on that environment; use JSON output in Jobs and run the full-screen intuiTUIve inspector locally.
2. Implement a Connect-compatible reader/scope adapter using public SparkSession APIs, without SparkContext, JVM properties or FAIR pool configuration. Preserve version-pinned Delta reads and UC PK discovery. Audit each used DataFrame API on the actual serverless runtime.
3. Add a serverless Job bundle without `existing_cluster_id`. Use a `For each` task to fan out pair IDs to a comparison task, initially with concurrency **2**, configurable up to the available account quota (at most five concurrent tasks across the account). Each task compares one pair; do not nest the classic thread executor inside it.
4. Publish validated configuration once per run and pass its immutable path/hash and each pair ID. Write unique per-pair result files or Delta result records; do not share a latest-report filename or the local file-store lock between tasks.
5. Run a final aggregation task after all comparison tasks finish, including failure paths. Verify every selected pair has exactly one terminal outcome. Missing task results, failed persistence and inaccessible inputs must fail the run. Preserve row-regression outcomes as results for aggregation; infrastructure failures must also remain visible to Jobs.
6. Add the hosted App after the execution route works. It submits Jobs and displays results; large comparisons stay on serverless compute. App limits do not provide an always-on production service.

Job fan-out is the recommended initial Free Edition route because it provides parallel comparisons with task isolation and observable outcomes, without depending on classic JVM threading. The five-task quota is an account-wide task limit, not a promise of five simultaneous Spark queries inside one notebook.

## Acceptance tests in a sandbox workspace

- Use small synthetic Delta fixtures in a dedicated schema; begin with two pairs and a few dozen rows each.
- Pair A: identical rows in different order. Expected pass, no differences.
- Pair B: three rows on each side, two matching keys; one matched row changed, one baseline-only key, one candidate-only key. Expected matched=2, changed=1, missing=1, extra=1.
- Run the same suite from JSON and YAML and require identical normalized config hashes and outcomes.
- Verify composite/renamed explicit keys, declared UC keys, absent keys, null/duplicate keys and schema drift. Unresolved/invalid keys cannot pass.
- Change inputs after pinning; compare recorded versions and confirm result consistency. Exercise finite extremes, nulls and NaN under serverless's default ANSI behavior.
- Verify task overlap in the Job timeline, configured concurrency, and complete aggregation when one pair fails or its output cannot be written.
- Verify table read/history permissions, metadata visibility, dependency installation and report access. Treat quota exhaustion as incomplete/error, never a successful empty run.

Free Edition is documented for learning and non-commercial exploration, with limited usage. Use synthetic fixtures for this evaluation. A commercial deployment needs an appropriate paid workspace or trial.

## Sources

- [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)
- [Serverless compute limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations)
- [SparkContext serverless compatibility](https://docs.databricks.com/aws/en/pyspark/reference/classes/sparksession/sparkContext)
- [For each Job task](https://docs.databricks.com/aws/en/jobs/for-each)
- [Free Edition versus trial](https://docs.databricks.com/aws/en/getting-started/free-trial-vs-free-edition)

- [DQX Studio architecture](https://github.com/databrickslabs/dqx/blob/main/app/README.md)
- [DQX installation and serverless wheel runner](https://databrickslabs.github.io/dqx/docs/installation/)
