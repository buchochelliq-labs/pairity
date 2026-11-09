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

If a suite already exists or changed while you were editing, choose **Review latest
revision** beside the run action. Pairity loads the latest saved revision and
previews your existing draft against it without replacing your edits. Review the
changes, then explicitly save or run again. A second concurrent change still
blocks the write. Importing a JSON/YAML file never silently overwrites an existing
suite with the same name.

Choose **Save & run** for a new or edited configuration to validate and save it,
then submit the exact saved revision. No separate preview is required for this
explicit action; preview and save remain available for reviewing changes first.
Validation errors and revision conflicts prevent submission and appear beside the
run action. Unchanged saved configurations use **Run** directly. Editing is locked
while a save or submission is pending to keep the submitted snapshot consistent.

Follow Job status, cancel with the caller's permissions,
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
[App deployment 42100391510](https://github.com/buchochelliq-labs/pairity/actions/runs/42100391510)
succeeded on November 5, 2026, and the operator confirmed that the App now works.
This establishes deployment/startup with the current Lakebase binding and literal
endpoint workaround. It does not establish the multi-user workflow or permission
checks below.

After deployment, check upload -> preview -> save -> run -> history -> incident ZIP
using two authorized users, a stale revision, and a user without source access.
These workflow and permission checks remain required release gates.

## Run analysis and monitor progress

Choose **Save & run** for a new or edited configuration, or **Run saved suite**,
**Run selected pairs** or **Run tagged pairs** for an unchanged saved revision.
While an analysis is active, the action becomes **View analysis progress** so it
reopens the existing run without submitting another. The analysis dialog opens
during submission and shows the run ID, saved revision, elapsed time, Job stage
and comparison-task progress. The Job remains responsible for executing comparisons
in parallel.

Stages follow Databricks metadata: queued, preparing, comparing, saving results,
cancelling and finished. Counts come from ForEach statistics, which count only
the latest retries. The attempt table can show multiple attempts for a pair;
those rows are not added together to calculate progress. Pair labels use the
submitted snapshot's indexes and resolved notebook parameters, never the order
of an API response. Completed tasks do not establish a passing regression:
disabled pairs can complete with a skipped result, and failing tasks can
represent regressions, blocked comparisons or execution errors.

Unknown progress is indeterminate. There is no estimated row percentage or ETA.
Completing all comparison tasks does not mark the Job complete while aggregation
is still saving results. Metadata reads are bounded to three iteration pages
(300 attempts); incomplete or temporarily unavailable details are labelled and
the Databricks run link remains available. No source rows, task inputs or raw
notebook error output are returned by the progress endpoint.

**Minimize** (or Escape) keeps monitoring in the current tab. **Show progress**
reopens the dialog without resubmitting. Closing/reloading the browser stops this
tab's monitor but does not cancel the Job. Polling is sequential and reports
monitoring failures separately from Job failures; **Retry status** never submits
another run. The most recent status update is shown alongside elapsed time.

**Cancel run** sends one cancellation request and keeps monitoring until Databricks
reports a terminal state. A rejected request can be retried. **View results**
loads the persisted `job:<run-id>` report through the existing caller-authorized
endpoint. If a failed/cancelled Job never saved its aggregate report, or the caller
cannot read it, the dialog shows the failure and allows an explicit retry; it
does not manufacture an empty report. Submission failures are shown immediately;
check Jobs before submitting again if a transport failure left the outcome unclear.

The initial implementation and live qualification are tracked in #52. Verify
parallel execution, cancellation, failed preparation, result loading and denied
Job/report access in the Databricks-free environment after deployment.

Before Job submission, caller-scoped SQL checks verify source access and discover
keys when needed. SQL reads continue after the initial 30-second response wait and
poll the same statement within a 120-second budget, allowing a suspended warehouse
to start. They request cancellation when that budget expires. Failed SQL checks
prevent submission and report the state, error class, SQLSTATE and statement ID
when available. Use that ID in SQL query history for the full error. App logs record
those diagnostic fields without raw SQL, provider messages or data values. A failed
cancellation is reported as unconfirmed, never as a successful cancellation.

**Submission steps that run as the App.** After your source checks pass, the App's service principal:
1. reads the Job's access control list to check your Job permission (it needs `CAN_MANAGE` on the Job to read it);
2. uploads the suite snapshot to the artifact Volume (`USE CATALOG`, `USE SCHEMA`, `READ VOLUME`, `WRITE VOLUME`);
3. starts the Job (`CAN_MANAGE_RUN`) and reads the run back.

A failure names its step and the service principal. It also says whether nothing was started, or whether a run was started and should be checked in Databricks Jobs before you resubmit. Unexpected errors return their exception type and are logged with a traceback in the App's logs.

**Which identity needs source access.** Two identities read the source tables, and both need `USE CATALOG`, `USE SCHEMA` and `SELECT`, directly or through a group:

1. **The signed-in workbench user.** Pre-submission checks, run history and incident downloads run as the person using the App: the App uses the access token Databricks forwards for that user, never the App's own service principal. A refused check names this user and the table.
2. **The identity the Job runs as.** The bundle sets no `run_as`, so the Job runs as its owner, the identity that deployed it. For CI deployments this is the `DATABRICKS_CLIENT_ID` service principal in the `Databricks-free` environment.

Tables created by the smoke workflow (`gha_*`) are owned by that service principal, so it can read them, but workbench users can't until they are granted access. For example, grant `SELECT` on the schema to give a user or group access to every fixture table:

```sql
GRANT USE CATALOG ON CATALOG <catalog> TO `user-or-group`;
GRANT USE SCHEMA  ON SCHEMA  <catalog>.<schema> TO `user-or-group`;
GRANT SELECT      ON SCHEMA  <catalog>.<schema> TO `user-or-group`;
```

## Discover table pairs and primary keys

The pair editor opens a dedicated Catalog Explorer with catalog/schema navigation,
a shared table-results view and a **Compare with…** action.
No public native menu extension was found in the [Catalog Explorer documentation](https://docs.databricks.com/aws/en/catalog-explorer/).
An optional browser extension could provide a [browser context-menu action](https://developer.chrome.com/docs/extensions/reference/api/contextMenus)
that opens a preselected table in Pairity; it would require installation and is
separate from a native UC integration.

In a configuration's pair editor, choose **Browse Unity Catalog**. Search accessible
Delta tables across catalogs, or narrow the scope using the catalog/schema navigation.
Search matches literal names and qualified paths; **Load more tables** retrieves
the next bounded page. Select a table name to inspect its columns and declared PKs.
This inspection does not change the pair or read source records.

**Compare with…** selects a baseline and searches for same-name candidates across
catalogs. The baseline stays visible. Confirm a candidate with **Use as candidate**;
suggestions never select it automatically. **Choose baseline** and **Choose candidate**
switch which side you are editing. Explicit table replacement uses the current
version, clears that side's version pin and retains other pair settings and keys.
Browsing alone does not change pins. Composed/template sources remain in the source
editor and are never flattened. **Done** or Escape closes the explorer; selections
remain in the draft, ready for validation and **Save & run**.

Choose **Inspect tables and keys** to check table visibility, SELECT access and
column metadata. Matching declared UC primary keys produce an editable suggestion.
Missing or differently named declarations require explicit mappings: use **Add key
column** and select columns from both sides. Composite keys use multiple rows.
Remove rows to discard a suggestion. No key is guessed from a column named `id`.
The existing manual key field remains available.

**Validate key mapping** checks missing/duplicate/excluded key columns and scalar
type compatibility. It resolves inherited implicit-cast settings, column opt-outs
and explicit `compare_as` using the engine's shared type policy. Planned casts are
shown; actual cast validity is checked on the data by the Job. Configure cast
directives in the JSON/YAML editor, then inspect again.

**Apply key mappings** updates the same JSON/YAML draft and invalidates any existing
save preview. Continue with **Save & run**, or preview and save to create a revision. The check is
an optional authoring aid; drafts and manual configurations retain their existing
save flow. Missing keys still block execution, and the engine checks keys again.
Only the key metadata is validated here, not the full value-column comparison.

Metadata discovery uses the verified caller's existing SQL warehouse connection
and `sql` user scope, not the App service principal. It reads information schema
and executes `SELECT ... LIMIT 0` to check table access, returning no source rows.
Page sizes are capped at 100 and table inspection at 500 columns; larger schemas
produce an explicit error and remain configurable manually. Query failures never
establish readability. Discovery is unavailable in local demo mode unless a
`CatalogDiscovery` v1 adapter is injected into the shared workbench facade.
Cross-catalog search is an optional `CatalogSearch` capability. Existing v1 adapters
without it can still browse after a catalog and schema are selected.

UC primary keys are informational. These checks do not prove uniqueness, null
absence or lossless casts. They inspect current metadata; the Job validates the
actual pinned snapshot. Deleted tables, revoked permissions and changed schemas
can invalidate an earlier check. There is no data scan in the App process.

Live qualification for [#50](https://github.com/buchochelliq-labs/pairity/issues/50):
use the Free Edition App to discover a readable Delta pair, apply declared and
renamed/composite keys, preview/save/run, then try missing keys, an inaccessible
table, and access revoked after inspection. Confirm no source metadata or rows
leak across caller identities. Automated adapter/API/UI tests do not replace this
workspace check. Global cross-catalog matching suggestions are a later UX step.

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
