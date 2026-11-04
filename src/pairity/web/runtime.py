"""Databricks composition root: OBO reads, app-owned config state and Job launch."""

import json
import os
from pathlib import Path

from fastapi import HTTPException

from pairity.workbench import FileSuiteCatalog, PostgresSuiteCatalog, Workbench


class FileResultReader:
    """Local demo/development only; reads saved reports, never executes comparisons."""

    def __init__(self, root):
        self.root = Path(root)

    def _reports(self, suite=None):
        reports = []
        for path in self.root.glob("*.json"):
            if path.stat().st_size > 4 * 1024 * 1024:
                continue
            document = json.loads(path.read_text())
            report = document.get("report", document)
            if report.get("result_schema_version") == 1 and (
                suite is None or report["suite"] == suite
            ):
                reports.append(report)
        return reports

    def history(self, *, suite=None, limit=50):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("History limit must be 1..100")
        return sorted(self._reports(suite), key=lambda r: r["finished_at"], reverse=True)[:limit]

    def load(self, run_id):
        matches = [r for r in self._reports() if r["run_id"] == run_id]
        if len(matches) != 1:
            raise FileNotFoundError("No unique saved report for this run")
        return {"report": matches[0]}


class JobAuthorization:
    """Check caller Job ACLs before using the App's Jobs API access.

    App user tokens support SQL, not Jobs/permissions REST scopes. The App reads
    the runner ACL; group membership is evaluated by SQL as the verified caller.
    Missing ACLs, unknown grants and failed lookups never authorize an operation.
    """

    def __init__(self, client, job_id, user, query):
        self.client, self.job_id, self.user, self.query = client, job_id, user, query

    def __call__(self, operation):
        if operation not in {"view", "manage"}:
            raise ValueError("Unknown Job operation")
        allowed = {"CAN_MANAGE_RUN", "CAN_MANAGE", "IS_OWNER"}
        if operation == "view":
            allowed.add("CAN_VIEW")
        permissions = self.client.permissions.get(
            request_object_type="jobs", request_object_id=str(self.job_id)
        )
        for entry in permissions.access_control_list or []:
            levels = {
                p.permission_level.value
                for p in entry.all_permissions or []
                if p.permission_level is not None
            }
            if not levels.intersection(allowed):
                continue
            if entry.user_name == self.user:
                return
            if entry.group_name:
                rows = self.query(
                    "SELECT is_member(:group_name) AS allowed", {"group_name": entry.group_name}
                )
                if len(rows) == 1 and (
                    rows[0].get("allowed") is True or rows[0].get("allowed") == "true"
                ):
                    return
        raise HTTPException(403, "Your Job permissions do not allow this operation")


class AppSubmitter:
    """App executes Job APIs only after checking caller permissions on that runner."""

    def __init__(self, submitter, observer, authorize):
        self.submitter, self.observer, self.authorize = submitter, observer, authorize

    def submit(self, config, *, request_id):
        self.authorize("manage")
        return self.submitter.submit(config, request_id=request_id)

    def status(self, handle):
        self.authorize("view")
        return self.observer.status(handle)

    def cancel(self, handle):
        self.authorize("manage")
        return self.observer.cancel(handle)


def service_for_request(request):
    mode = os.environ.get("PAIRITY_APP_MODE", "databricks")
    if mode == "local":
        root = Path(os.environ.get("PAIRITY_LOCAL_ROOT", ".pairity-local"))
        return "Local developer", Workbench(
            FileSuiteCatalog(root / "suites"), results=FileResultReader(root / "reports")
        )
    if mode != "databricks":
        raise ValueError("Unknown app mode")
    token = request.headers.get("x-forwarded-access-token")
    if not token:
        raise HTTPException(401, "Databricks user authentication is required")
    from databricks.sdk import WorkspaceClient
    from psycopg import connect

    from pairity.artifacts import VolumeArtifactStore
    from pairity.metadata import UnityCatalogMetadata
    from pairity.postgres import PostgresDocumentStore
    from pairity.submission import DatabricksJobSubmitter, VolumeConfigPublisher
    from pairity.web.storage import SqlQuery, SqlResultReader, source_authorizer

    # Force PAT strategy for the short-lived forwarded token, never the app's OAuth identity.
    caller = WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=token, auth_type="pat")
    try:
        user = caller.current_user.me().user_name
    except Exception as exc:
        raise HTTPException(401, "User identity could not be verified") from exc
    if not user:
        raise HTTPException(401, "User identity is missing")
    query = SqlQuery(caller, os.environ["PAIRITY_SQL_WAREHOUSE_ID"])

    def metadata_query(sql, params):
        return [
            row | {"ordinal_position": int(row["ordinal_position"])} for row in query(sql, params)
        ]

    dsn = os.environ.get("PAIRITY_POSTGRES_DSN")
    if not dsn:
        raise ValueError("PAIRITY_POSTGRES_DSN is required; initialize Postgres before deployment")
    documents = PostgresDocumentStore(
        lambda: connect(dsn), schema=os.environ.get("PAIRITY_POSTGRES_SCHEMA", "pairity")
    )
    app_client = WorkspaceClient()
    artifacts = VolumeArtifactStore(os.environ["PAIRITY_ARTIFACT_ROOT"], app_client)
    job_id = int(os.environ["PAIRITY_JOB_ID"])
    submitter = AppSubmitter(
        DatabricksJobSubmitter(app_client, job_id, VolumeConfigPublisher(artifacts)),
        DatabricksJobSubmitter(app_client, job_id, None),
        JobAuthorization(app_client, job_id, user, query),
    )
    return user, Workbench(
        PostgresSuiteCatalog(documents),
        metadata=UnityCatalogMetadata(metadata_query),
        submitter=submitter,
        results=SqlResultReader(query, os.environ["PAIRITY_RESULT_TABLE"]),
        authorize_sources=source_authorizer(query),
        artifacts=artifacts,
    )
