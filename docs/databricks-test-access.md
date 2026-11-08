# Databricks access for Pairity integration tests

The GitHub Actions deployment identity and the Job's **Run as** identity need the
permissions below. They may be the same identity for the sandbox. Tests use only
synthetic fixtures, a dedicated schema and a managed Volume.

## Authentication

The `Databricks-free` environment uses a service principal with an OAuth client
ID and secret. The workflow defaults to **`DATABRICKS_AUTH_TYPE=oauth-m2m`** and
passes these settings to the Databricks CLI and SDK:

- `DATABRICKS_HOST`: workspace URL, from an environment secret.
- `DATABRICKS_CLIENT_ID`: environment secret.
- `DATABRICKS_CLIENT_SECRET`: environment secret.

No personal access token is required. The CLI/SDK obtain and refresh OAuth access
tokens using the client credentials. The principal needs workspace assignment and
the resource permissions below. The workflow does not create or change identities.

Other setups can explicitly select `github-oidc` with a configured federation
policy and client ID, or `pat` with the `DATABRICKS_TOKEN` secret. A configured PAT
is only passed when PAT authentication is explicitly selected, avoiding conflicts
with the default OAuth mode. Never paste credentials into issues, configs or chat.

## OAuth scopes

The workflow selects the `pairity-ci` profile in `.github/databrickscfg` for both
bundle CLI commands and the Python smoke runner. It explicitly requests these
eight scopes: `apps`, `bundle-deployments`, `databricks-connect`, `files`, `jobs`, `sql`,
`unity-catalog`, and `workspace`. Update that profile to change the scope policy. It contains no
credentials; the existing GitHub environment secrets remain the credential source.
An explicit profile is necessary because the pinned SDKs do not read scopes from
an environment variable. Without configured scopes they default to `all-apis`.

The service principal's OAuth credential must allow all eight requested scopes.
The workflow does not broaden its request or fall back to `all-apis` on denial.
These scopes limit API access; the workspace, Job and Unity Catalog grants below
are still required. Job notebooks retain their Databricks-provided authentication
and Run as identity. Successful token issuance alone does not prove every bundle
or Volume operation is permitted; the live smoke run validates those operations.

## Permissions

| Resource | Deployment / fixture setup | Comparison Job |
| --- | --- | --- |
| Workspace | Workspace access; create/manage a dedicated bundle folder and upload notebooks/wheel | Read/access deployed task files and serverless Jobs execution |
| Pairity Job | Create Job; CAN MANAGE or ownership to redeploy; CAN MANAGE RUN to start/cancel; Service Principal User role if assigning a separate service principal | Executes with the configured Run as identity |
| Existing catalog | USE CATALOG | USE CATALOG |
| Dedicated schema | USE SCHEMA, CREATE TABLE; CREATE VOLUME only if provisioning a Volume | USE SCHEMA |
| Synthetic inputs | Own newly created fixture tables; write only fixture resources | SELECT on all eight fixture tables, including their retained Delta history |
| Result table | CREATE TABLE for explicit initialization | SELECT and MODIFY on the Pairity result table |
| Managed artifact Volume | READ VOLUME and WRITE VOLUME | READ VOLUME and WRITE VOLUME for configs and immutable manifests/artifacts |

An administrator/owner can pre-create the schema and Volume. Then CI does not
need CREATE SCHEMA or CREATE VOLUME. No account-admin role, broad catalog ownership,
external storage credentials, external locations or classic cluster is required
for these tests. The fixture loader owns its newly created tables; cleanup must
be restricted to those fixture resources. Explicit PK mappings avoid requiring
constraint discovery for this first fixture.

GitHub environment secrets (use your actual resource names/paths):

```text
DATABRICKS_HOST=https://<your-workspace-host>
DATABRICKS_AUTH_TYPE=oauth-m2m    # default; optional secret override
PAIRITY_CATALOG=workspace
PAIRITY_SCHEMA=pairity_test
PAIRITY_ARTIFACT_ROOT=/Volumes/workspace/pairity_test/artifacts
PAIRITY_RESULT_TABLE=workspace.pairity_test.results
```

The configured GitHub environment is **`Databricks-free`**. Once the workflow is
merged to main, open **Actions → Databricks Free Edition smoke tests → Run workflow**
and choose main. It is manual and main-only. Protect this environment to allow
only main deployments; the workflow does not inspect or print secret values.

The workflow checks the required environment secrets, builds the checked-out wheel,
deploys the `free_test` bundle target, then runs fixture setup, the identical
suite, the changed suite, the features suite and a Delta-result verifier. Expected
regression Job failure alone is insufficient: all counts, statuses and source
snapshot identities must match, and the Jobs timeline must show peak pair-task
overlap of two for the four-pair suites.

The features suite (`examples/tuva/tuva-features.*`, expectations in
`expected-features.json`) qualifies on serverless:
- `as_of` snapshot selection, with the instant captured after setup creates the tables, so it must resolve to version 0;
- typed `where` filters and exact decimal tolerances;
- a SQL source with a bound parameter that builds struct, map and array columns, compared with the nested comparator and `array_order: unordered`.

The verifier checks the recorded comparator per column, `as_of` and filters on scoped inputs, and each SQL source's leaf snapshot, query hash and bound parameters.
Reports and Job links are uploaded as a 14-day Actions artifact.

Each invocation uses `gha_<run_id>_<attempt>` table prefixes and a separate Volume
subdirectory. Synthetic tables, configs and results are retained for investigation;
cleanup is deliberately not automatic. Remove only that run's fixture tables and
Volume subdirectory when finished; the shared result table is not a cleanup target.
The workflow does not create schemas, Volumes or grants. Full failure/permission
qualification in #9 extends beyond these smoke tests.

 Use concurrency 2. No SQL warehouse ID is required:
these are serverless notebook/Job tasks. Postgres/Lakebase access is not needed for
file-based comparison tests; it becomes necessary when testing hosted app state.
Optional full row-evidence persistence additionally requires the serverless
DataFrame caching capability described in deployment.md; summaries do not.

## Sources checked 2026-11-04

- https://docs.databricks.com/aws/en/getting-started/free-edition-limitations
- https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m
- https://docs.databricks.com/aws/en/dev-tools/auth/provider-github
- https://docs.databricks.com/aws/en/dev-tools/auth/pat
- https://docs.databricks.com/aws/en/jobs/privileges
- https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control/privileges-reference
- https://docs.databricks.com/gcp/en/volumes/privileges

## Inspecting existing runs

For API/timeline troubleshooting, dispatch the same workflow with `inspect_run_ids`
set to comma-separated existing parent Job run IDs (at most ten). This mode skips
preflight, deployment, fixture setup and Job execution. It only reads run metadata
and publishes `run-inspection.json` plus the same JSON in the Actions log. The
report includes requested/returned run IDs, task keys, result states, iteration
counts and timings; it excludes parameters, notebook output and source paths.
Leave the input blank to run the full smoke test. A successful inspection means
metadata was retrieved, not that the comparison results or parallelism passed.
