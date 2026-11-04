# Databricks deployment

The bundle is an initial classic-compute deployment definition, not a claim of live workspace verification. The intended hosted architecture uses serverless Jobs, following DQX. See [Free Edition and serverless deployment](free-edition.md) for the required adapter changes and acceptance tests.

## Prerequisites

- A Databricks workspace with classic Spark compute compatible with the initial Spark 3.5 adapter.
- Compute with pinned Python/JVM threads and `spark.scheduler.mode=FAIR` configured before Spark starts for fair sharing between pair jobs.
- Read access to the compared tables and their Delta history. Automatic PK discovery also needs visible Unity Catalog constraint metadata; explicit PK mappings avoid that lookup.
- A readable JSON/YAML configuration file and an existing writable report directory, for example in a UC Volume.
- Databricks CLI authentication and permission to create/update a Job and attach to the chosen compute.
- `python -m build` available in the local deployment environment.

```bash
databricks bundle validate \
  --var existing_cluster_id=CLUSTER_ID \
  --var config_path=/Volumes/CATALOG/SCHEMA/VOLUME/suite.yaml \
  --var report_directory=/Volumes/CATALOG/SCHEMA/VOLUME/reports

databricks bundle deploy \
  --var existing_cluster_id=CLUSTER_ID \
  --var config_path=/Volumes/CATALOG/SCHEMA/VOLUME/suite.yaml \
  --var report_directory=/Volumes/CATALOG/SCHEMA/VOLUME/reports
```

The bundle creates an **unscheduled** wheel task on the explicitly selected existing cluster. Pair concurrency comes from the suite configuration. Each report uses the Databricks job run ID as its filename. It does not create a cluster, provision a database, change source-table permissions or start scheduled work.

The wheel includes the Rust-backed `rs-rich` binding for terminal reports. It does not install a replacement PySpark over Databricks' bundled runtime. `pairity-job` raises on failed, blocked, erroneous or skipped runs so a Job cannot treat an integer return value as success.

## Workspace smoke test before relying on results

1. Create two small Delta table pairs in a sandbox schema, with explicit PK mappings and `max_parallel_pairs: 2`.
2. Include one identical pair and one pair with one changed, one missing and one extra row. Confirm the exact counts and nonzero Job outcome.
3. Inspect the report's pinned versions and per-pair Spark job groups. Run the intuiTUIve inspector against the downloaded report.
4. Repeat with declared UC PKs, missing PKs, duplicate keys and a denied input table; these must never produce a passing suite.
5. Confirm the report exists and a denied report destination fails the Job.

Local verification covers the Spark core, parallel executor and adapter interfaces. Unity Catalog metadata access, Delta time-travel permissions, report writes through the workspace filesystem and deployment permissions require this live check. Spark Connect/serverless, remote configuration storage, Delta result history and the hosted App remain tracked follow-up work.
