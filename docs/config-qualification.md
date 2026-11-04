# Configuration flexibility qualification

The opt-in `free_test` Job resource `config_qualification` (display name
`pairity-config-qualification`) runs `notebooks/config_qualification.py` on
serverless environment 5 with the built wheel and pinned Databricks SDK.
Pass `test_id=gha_<run-number>_<attempt>`; `artifact_root` comes from the bundle
variable and must be a writable Unity Catalog Volume path.

The notebook executes 15 scenarios through both shared JSON/YAML codecs and
validates identical fingerprints and results. Coverage includes reusable
parameterized union/dedup templates, strict/common/excluded columns, default
implicit casts, global and column cast disabling, column re-enabling, explicit
`compare_as`, global and column null-safe settings, missing PK metadata,
null/duplicate PKs, and invalid casts. Six invalid configuration documents must
be rejected by schema validation. Expected statuses and complete row counts
(or absence of counts for schema failures/blocked prerequisites) are checked.

Every comparison uses the production `PairityEngine`, `ComparisonRunner`,
`SparkComparisonBackend`, `SparkSourceResolver`, and serverless `InlineExecution`.
The source reader and metadata adapter are deliberately synthetic: tiny
in-memory DataFrames, no source table/view writes, no caching, JVM access, or
SparkContext use. The reader does not claim Delta snapshot provenance.
`create_task_engine` is not used because it binds physical table/UC adapters;
the harness composes the same core with controlled read-only adapters.

A controlled sink raises `PermissionError` after a successful comparison. The
harness requires propagation, proving fail-closed persistence behavior without
changing permissions or writing a denied target. This does **not** qualify
actual workspace grants, Unity Catalog PK discovery, or Delta persistence.

Only after every assertion succeeds does the SDK Files API publish bounded
JSON evidence (128 KiB maximum) to
`<artifact_root>/smoke/<test_id>/config-qualification.json`, without overwrite.
Upload failures fail the Job. Output contains synthetic result metadata and
counts, never collected difference rows. A local integration test runs the
same harness; local success is not Databricks serverless qualification.
Record the actual Job run and downloaded evidence before claiming live
qualification. Until that run completes, serverless qualification is pending.
