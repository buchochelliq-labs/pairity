# Pairity workbench

The Databricks App follows DQX's FastAPI + React architecture. It imports the same
configuration and submission services as the CLI and never starts Spark in the web
process. Serverless Jobs keep the existing bounded parallel pair fan-out.

## Available workflow

Upload JSON or YAML, validate it, inspect effective settings and missing keys,
preview revisions, save, clone and export in either format. Edit pair sources,
keys and tags in the form or use the full document editor for templates and column
policies. Bulk patches require a pair or tag selection and a preview before apply.
Edits use optimistic revisions: stale writes return a conflict instead of silently
overwriting another user's configuration.

Submit a saved revision, follow Job status, cancel with the caller's permissions,
review historical counts and trends, and download an incident ZIP from a completed
report. The ZIP includes a count image, ticket text, pinned source diagnostic SQL,
configuration and same-engine Python reproduction. See [incident limitations](incidents.md).
The web download is aggregate-only; it does not read current source rows to invent
historical samples. Full SQL replay and persisted-sample retrieval remain follow-up work.

Postgres/Lakebase stores shared configuration revisions. Delta stores completed
results. UC Volumes receive immutable submission configs and optionally CLI-published
incident archives. Local mode is for development, stores files, and cannot submit Jobs.
Adapters are constructor-injected through `Workbench`; comparison logic stays in the core.

## Deployment prerequisites

Use an existing Pairity regression Job, initialized result table, UC artifact Volume
and serverless SQL warehouse. The App bundle provisions its own Lakebase project
`pairity-state`, using the project's implicit production branch, primary endpoint
and `databricks_postgres` database. Compute is capped at 0.5 CU and suspends after
five idle minutes. The bundle protects the project against accidental destruction.
Free Edition permits only one Lakebase project: this setup assumes that slot is
unused. If one already exists, stop and configure an explicit attachment instead
of creating a second project or deleting existing data.

The deployer must be allowed to create a Lakebase project and manage it to attach
its database to the App. The `pairity-app-ci` OAuth profile includes `postgres`
for provisioning; the existing smoke-test profile is unchanged. The database
resource creates the App's PostgreSQL role and grants CONNECT/CREATE. Databricks
injects PGHOST, PGUSER, PGDATABASE, PGPORT and PGSSLMODE. Pairity generates a fresh
short-lived database credential for each connection using the App identity.
No `postgres-dsn` secret or permanent database password is required.

The deployed startup command explicitly initializes Pairity's schema/tables before
serving requests. Initialization is transactional, serialized across App instances
with a PostgreSQL advisory lock, and preserves existing document revisions. An
initialization failure prevents startup. Normal requests do not perform DDL.
Connection factories stay replaceable: non-Lakebase deployments may use
`PAIRITY_DATABASE_AUTH=dsn` with `PAIRITY_POSTGRES_DSN` supplied securely and the
existing `pairity init-postgres` provisioning command. See [storage](storage.md).

The App's service principal needs `USE CATALOG`, `USE SCHEMA`, `READ VOLUME` and
`WRITE VOLUME` on its artifact destination. Its attached Job resource grants
`CAN_MANAGE_RUN`; the Job's run-as identity still needs the existing comparison and
result-write grants. The attached warehouse grants CAN_USE.
The deployer needs authority to attach those resources and grant the App access.

Users need access to the App, SQL warehouse, source tables and result table. Reads
use the forwarded user access token, and each report checks source access before
returning evidence. Job API calls use the App identity because Jobs is not a supported App user scope.
Before each call, Pairity reads the configured runner's ACL and checks the verified
caller's direct grants or group membership (using caller-authorized SQL `is_member`).
Status requires CAN_VIEW or higher; submission and cancellation require
CAN_MANAGE_RUN, CAN_MANAGE or IS_OWNER. Failed permission reads deny the operation;
source SELECT alone cannot launch a Job. The App needs permission to read the
runner ACL as well as manage its runs. Groups not visible to the caller's SQL session
are conservatively denied; use an explicit user grant if necessary. Jobs
execute as their configured run-as principal, so source row filters and masks may
differ from an operator's interactive view. Only grant this App and its result table
to trusted operators authorized for that run-as data scope. Configuration state is
shared by all authorized App users; this release has no per-suite ownership or ACLs.
Deploy separate instances/databases for separate trust groups. The application must
be behind the Databricks authenticated proxy; do not expose hosted mode directly.

Enable [user authorization](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/auth)
for the App with the declared `sql` user scope. Identity defaults are managed by
Databricks and must not be explicitly included in `user_api_scopes`. SQL covers
source/metadata/result reads and group checks. Resource permissions and OAuth
scopes are separate requirements.
The App deployer uses the selective `pairity-app-ci` profile, including files and postgres.

## GitHub Action

`Deploy Pairity workbench` is manual, main-branch only and uses the `Databricks-free`
environment. It builds/tests the frontend, assembles the exact checkout, validates
and deploys `app/databricks.yml`, then starts the App. It does not execute comparisons.

The bundle explicitly syncs `app/build`, including the Python package, requirements
and compiled frontend. This generated directory stays Git-ignored; the bundle's
`sync.include` overrides that exclusion for deployment. Build the UI and run
`scripts/build_app.py` before deploying locally too.

If App startup fails, the Action retrieves the last 200 App log lines before
returning the original deployment failure. Select `logs_only` when manually
running the Action to inspect an existing crash without rebuilding, deploying
or restarting the App. Log collection uses the existing service principal and
has a 60-second limit. The App name is resolved from the bundle target.
Alternatively, open the App's Logs tab in Databricks, or run
`databricks apps logs pairity-workbench --tail-lines 200` with an authorized
OAuth CLI profile. The generic deployment message alone does not identify the
startup exception. Read logs before stopping App compute, which clears them.

Deployment configuration lives in `app/databricks.yml`, including per-target values.
The Action no longer requires five additional GitHub environment secrets for the App.

| Bundle variable | Configured value |
|---|---|
| `app_name` | `pairity-workbench` |
| `warehouse_name` | `Serverless Starter Warehouse`; change if your warehouse has another name |
| `warehouse_id` | Resolved by name lookup; can be explicitly overridden |
| `job_id` | `656591313118833` in the `workspace` target, the existing Free Edition regression Job |
| `lakebase_project_id` | `pairity-state` |
| `postgres_schema` | `pairity` |

The runner ID comes from successful smoke run 41833551263; update the target if the
runner is recreated. It refers to an existing Job owned by the regression bundle,
not a new Job managed by the App bundle. Warehouse lookup must resolve one existing
warehouse; it does not create compute or silently select another warehouse.

The App and Lakebase project are created by the bundle. The Job, warehouse,
result table and Volume are existing resources. Lakebase role binding and schema
initialization are part of deployment/startup; no manual connection-string setup
is needed.

GitHub authentication still uses `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID` and
`DATABRICKS_CLIENT_SECRET` from the existing environment. The Action also retains
`PAIRITY_RESULT_TABLE` and `PAIRITY_ARTIFACT_ROOT` from the established smoke setup
for compatibility. Those two are configuration, not credentials; they can later
move to the target variables once their actual values are supplied. No additional
GitHub secret is needed for the database connection string.

Other deployments can override target variables or use the standard bundle `--var`
and `BUNDLE_VAR_*` mechanisms. The Action intentionally does not inject empty
optional environment values, which would override bundle defaults/lookups.

Free Edition qualification of the comparison Job is separate from App deployment.
The successful existing smoke does not establish Lakebase project-create privileges,
App user authorization, or runtime database connectivity. The first deployment
must verify project creation, App resource binding and schema initialization.

After deployment, check upload -> preview -> save -> run -> history -> incident ZIP
using two authorized users, a stale revision, and a user without source access.
Live App deployment and these permission checks remain required release gates.

### Dependency and Lakebase startup checks

To test an endpoint string directly, run **Test Lakebase endpoint format** on
`main`. It uses the `Databricks-free` service principal with only the `postgres`
scope and passes the input unchanged to `generate_database_credential`, bypassing
Pairity's local format check. Defaults compare `pairity-workbench/production/primary`
with `projects/pairity-state/branches/production/endpoints/primary`. Both requests
run even if one fails. The Action reports credential issuance or the SDK error
type/code, never the credential itself, and fails if either request fails.
Authentication/transport errors do not prove that an endpoint format was rejected.
This probe does not deploy the App, connect to PostgreSQL, or change its resources.

For the current Free Edition startup test, the App bundle temporarily sets
`PAIRITY_LAKEBASE_ENDPOINT` to the literal
`projects/pairity-state/branches/production/endpoints/primary`, which issued a
credential in the live probe. This bypasses resource-output substitution in the
App environment. If `lakebase_project_id` is overridden, update this literal to
match before deploying; the branch-consistency check still rejects a mismatch.
Startup logging remains enabled to verify the value received by the App.

The deployed pip environment uses Python 3.11. Its pre-installed Gradio and
Streamlit require Pillow below 11, so the App pins Pillow 10.4.0. The App also
pins OpenTelemetry API 1.42.1 to match the observed SDK 1.42.1 and semantic
conventions 0.63b1. FastAPI uses the documented runtime version 0.115.0: version
0.162.0 requires OpenTelemetry API >=1.44 and cannot coexist with that telemetry
stack. CI resolves the App requirements together with these telemetry
packages and the documented Gradio 4.44.0/Streamlit 1.38.0 versions, runs
`pip check`, and exercises the App and incident exporter. These runtime-specific
pins are separate from the general library's optional dependency ranges.

After bundle deployment, the workbench Action inspects the App's actual Lakebase
binding and checks that its endpoint and database belong to the same branch.
CLI 1.23.0 leaves resource-output references in `bundle summary` unresolved, so
the check resolves the endpoint's project reference using the deployed project's
resource ID before comparing branches. Missing deployment state or unresolved
endpoint configuration fails separately from an actual branch mismatch.
The Action logs the endpoint from the bundle summary and the resolved endpoint
used by its check. The App logs the received `PAIRITY_LAKEBASE_ENDPOINT` before
validating database settings, so a startup failure preserves the actual value.
Values are JSON-quoted to expose whitespace; an unset value appears as `null`.
Only this resource path is logged, not connection strings or credentials. If
the Action's runtime-log stream fails, read the startup value in the App Logs tab.
Using the deployer's existing OAuth identity, it opens a read-only database
connection and checks privileges **for the App's PostgreSQL role**: database
CONNECT/CREATE, existing schema USAGE/CREATE, and the operations needed on existing
Pairity document tables. Missing schemas/tables are left for normal initialization.
The check makes no grants or schema changes. It requires the deployer's Lakebase
owner role to connect from the Actions runner; a diagnostic connection failure
does not by itself establish that the App identity is denied.

This check does not impersonate the App or verify its OAuth login. Startup remains
the check of that identity. Startup failures now identify configuration, credential
generation, database connection or schema initialization; PostgreSQL SQLSTATE is
included when available. Driver messages, passwords and connection strings are
not logged. In particular, the former generic “check Lakebase resource access”
message was a troubleshooting hint, not proof of an authorization failure.

## Local development

```bash
python -m pip install -e '.[dev,app,artifacts]'
cd app/ui
npm ci
npm test
npm run build
cd ../..
python scripts/build_app.py
PAIRITY_APP_MODE=local pairity-app
```

Open http://127.0.0.1:8000. Local configuration state lives under `.pairity-local`.
Put saved result JSON documents in `.pairity-local/reports` for dashboard development.

## CLI

```bash
pairity suites --store-root postgres:
pairity history --result-table catalog.schema.results --warehouse-id WAREHOUSE_ID --interactive
pairity result --result-table catalog.schema.results --warehouse-id WAREHOUSE_ID --run-id 'job:RUN_ID' --output report.json
pairity incident report.json --output incident.zip
pairity incident report.json --output another-incident.zip --artifact-root /Volumes/catalog/schema/evidence/pairity
```

`submit --watch --timeout 1200` follows status transitions and exits nonzero on a
failed result or timeout. Timeout stops watching; it does not cancel a running Job.

## Lakebase references

- [Free Edition limits](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)
- [Lakebase bundle resources](https://docs.databricks.com/aws/en/oltp/projects/dabs-typical-project)
- [App database resource binding](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/lakebase)
- [OAuth credential rotation](https://docs.databricks.com/aws/en/oltp/projects/tutorial-databricks-apps-autoscaling)
