"""Add CAN_MANAGE for workspace admins and the workbench user to every deployed Job.

Uses the Jobs permissions PATCH API, which adds entries without removing others, so
grants made elsewhere (such as the App deployment's CAN_MANAGE_RUN for the App's
service principal) are preserved. Safe to run after every deploy.
"""

import argparse
import json
import re
from pathlib import Path

ADMINS = "admins"


def job_ids(summary):
    jobs = summary.get("resources", {}).get("jobs", {})
    ids = sorted({int(job["id"]) for job in jobs.values() if str(job.get("id", "")).isdigit()})
    if not ids:
        raise ValueError("The bundle summary lists no deployed Jobs")
    return ids


def grant(client, ids, manager):
    from databricks.sdk.service.jobs import JobAccessControlRequest, JobPermissionLevel

    if not re.fullmatch(r"[^@\s]+@[^@\s]+", manager or ""):
        raise ValueError("The job manager must be a Databricks user name (email)")
    entries = [
        JobAccessControlRequest(group_name=ADMINS, permission_level=JobPermissionLevel.CAN_MANAGE),
        JobAccessControlRequest(user_name=manager, permission_level=JobPermissionLevel.CAN_MANAGE),
    ]
    for job_id in ids:
        client.jobs.update_permissions(job_id=str(job_id), access_control_list=entries)
    return ids


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", required=True, help="databricks bundle summary JSON")
    parser.add_argument("--manager", required=True, help="Workbench user's email")
    args = parser.parse_args()
    from databricks.sdk import WorkspaceClient

    ids = grant(
        WorkspaceClient(), job_ids(json.loads(Path(args.summary).read_text())), args.manager
    )
    print(f"Granted CAN_MANAGE to {ADMINS} and the job manager on {len(ids)} Jobs: {ids}")


if __name__ == "__main__":
    main()
