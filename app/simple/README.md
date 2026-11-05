# Minimal Databricks App

This deployment check serves a static page at `/` and `{"status":"ok"}` at
`/health`. It uses only Python's standard library, binds to `0.0.0.0` and reads
`DATABRICKS_APP_PORT`. It has no database, warehouse, Job, user OAuth scopes,
frontend build or third-party Python dependencies.

Run **Deploy Pairity simple app** from GitHub Actions on `main`. It uses the
existing `Databricks-free` environment and only its `DATABRICKS_HOST`,
`DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` secrets. The workflow tests
the local HTTP server, deploys the bundle and waits for startup. It checks the
App state is `RUNNING` and writes its URL to the workflow summary. Open that URL
while signed into Databricks; the page must say **Pairity is running**. Also
check `/health`.

The bundle owns a separate App named `pairity-simple`. It does not update or
delete the workbench App or its Lakebase project. Stop this diagnostic App
when finished to release its compute; no database is created by this bundle.

Local smoke check: `python app/simple/test_app.py` from the repository root.
The test starts the actual entry point in isolated Python mode, verifies the
page, JSON health response and 404 response, then terminates the process.

After this passes in Databricks, add the workbench components incrementally:
FastAPI and the UI, then Lakebase connectivity, then Jobs and SQL access.
