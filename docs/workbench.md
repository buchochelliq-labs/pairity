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

Use an existing Pairity regression Job, initialized result table, UC artifact Volume,
serverless SQL warehouse, and reachable Postgres database. The application does not
create a database or provision broad catalog grants. Initialize application tables
using `PAIRITY_POSTGRES_DSN=... pairity init-postgres` with a provisioning identity,
then store the runtime TLS connection string in a Databricks secret. Use the same
`PAIRITY_POSTGRES_SCHEMA` during provisioning and runtime (default `pairity`).
The runtime database identity needs access to the existing Pairity application tables;
see [storage](storage.md). Dynamic Lakebase credential refresh is not built in: provide
a supported connection factory or a managed credential rotation arrangement.

The App's service principal needs `USE CATALOG`, `USE SCHEMA`, `READ VOLUME` and
`WRITE VOLUME` on its artifact destination. Its attached Job resource grants
`CAN_MANAGE_RUN`; the Job's run-as identity still needs the existing comparison and
result-write grants. The attached secret grants READ and warehouse grants CAN_USE.
The deployer needs authority to attach those resources and grant the App access.

Users need access to the App, SQL warehouse, source tables and result table. Reads
use the forwarded user access token, and each report checks source access before
returning evidence. Job submission, status and cancellation use user identity. Operators must have
CAN_MANAGE_RUN on the configured Job; source SELECT alone cannot launch it. Jobs
execute as their configured run-as principal, so source row filters and masks may
differ from an operator's interactive view. Only grant this App and its result table
to trusted operators authorized for that run-as data scope. Configuration state is
shared by all authorized App users; this release has no per-suite ownership or ACLs.
Deploy separate instances/databases for separate trust groups. The application must
be behind the Databricks authenticated proxy; do not expose hosted mode directly.

Enable [user authorization](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/auth)
for the App with its declared identity-read, SQL, Jobs, Unity Catalog and workspace scopes. Resource permissions and OAuth scopes are separate requirements.
The CI service principal retains the repository's selective scopes, including files.

## GitHub Action

`Deploy Pairity workbench` is manual, main-branch only and uses the `Databricks-free`
environment. It builds/tests the frontend, assembles the exact checkout, validates
and deploys `app/databricks.yml`, then starts the App. It does not execute comparisons.

Deployment configuration lives in `app/databricks.yml`, including per-target values.
The Action no longer requires five additional GitHub environment secrets for the App.

| Bundle variable | Configured value |
|---|---|
| `app_name` | `pairity-workbench` |
| `warehouse_name` | `Serverless Starter Warehouse`; change if your warehouse has another name |
| `warehouse_id` | Resolved by name lookup; can be explicitly overridden |
| `job_id` | `656591313118833` in the `workspace` target, the existing Free Edition regression Job |
| `postgres_secret_scope` | `pairity` |
| `postgres_secret_key` | `postgres-dsn` |
| `postgres_schema` | `pairity` |

The runner ID comes from successful smoke run 41833551263; update the target if the
runner is recreated. It refers to an existing Job owned by the regression bundle,
not a new Job managed by the App bundle. Warehouse lookup must resolve one existing
warehouse; it does not create compute or silently select another warehouse.

These names are deployment defaults, not proof the resources exist. Provision the
initialized database and store its TLS connection string in Databricks Secrets at
`pairity/postgres-dsn`, or edit the scope/key variables to reference your existing
secret. Only the reference is committed; never put the DSN/password in bundle YAML.

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
The successful existing smoke does not establish availability of Postgres/Lakebase,
App user authorization, or all resources in a Free Edition workspace. Verify those
prerequisites in the target workspace before running this new deployment action.

After deployment, check upload -> preview -> save -> run -> history -> incident ZIP
using two authorized users, a stale revision, and a user without source access.
Live App deployment and these permission checks remain required release gates.

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
