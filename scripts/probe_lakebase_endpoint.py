"""Ask Databricks to evaluate endpoint strings without Pairity's format guard."""

import json
import os


def probe(client, label, endpoint):
    report = {"test": label, "endpoint": endpoint}
    try:
        credential = client.postgres.generate_database_credential(endpoint=endpoint)
        accepted = bool(credential.token)
        report["outcome"] = "credential_issued" if accepted else "no_credential_returned"
    except Exception as exc:
        accepted = False
        report.update(
            outcome="request_failed",
            error_type=type(exc).__name__,
            error_code=getattr(exc, "error_code", None),
        )
    # Never print credentials, SDK exception messages or the surrounding environment.
    print(json.dumps(report), flush=True)
    return accepted


def main():
    from databricks.sdk import WorkspaceClient

    required = (
        "DATABRICKS_HOST",
        "DATABRICKS_CLIENT_ID",
        "DATABRICKS_CLIENT_SECRET",
        "PAIRITY_PROBE_ENDPOINT",
        "PAIRITY_CONTROL_ENDPOINT",
    )
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise SystemExit("Missing probe settings: " + ", ".join(missing))
    try:
        client = WorkspaceClient()
    except Exception as exc:
        raise SystemExit(f"Databricks SDK setup failed ({type(exc).__name__})") from None
    # Evaluate both even when the requested endpoint fails.
    results = [
        probe(client, "requested", os.environ["PAIRITY_PROBE_ENDPOINT"]),
        probe(client, "control", os.environ["PAIRITY_CONTROL_ENDPOINT"]),
    ]
    raise SystemExit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
