"""Validate environment settings without displaying values or credentials."""

import os
import re
from urllib.parse import urlsplit

from pairity.metadata import quote_table


def validate(env):
    required = [
        "DATABRICKS_HOST",
        "PAIRITY_CATALOG",
        "PAIRITY_SCHEMA",
        "PAIRITY_ARTIFACT_ROOT",
        "PAIRITY_RESULT_TABLE",
        "PAIRITY_JOB_MANAGER",
    ]
    auth = env.get("DATABRICKS_AUTH_TYPE", "oauth-m2m")
    if auth == "oauth-m2m":
        required.extend(["DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET"])
    elif auth == "pat":
        required.append("DATABRICKS_TOKEN")
    elif auth == "github-oidc":
        required.append("DATABRICKS_CLIENT_ID")
    else:
        raise ValueError("Configure DATABRICKS_AUTH_TYPE as oauth-m2m, pat or github-oidc")
    missing = [name for name in required if not env.get(name, "").strip()]
    if missing:
        raise ValueError("Missing GitHub environment settings: " + ", ".join(missing))
    host = urlsplit(env["DATABRICKS_HOST"])
    if host.scheme != "https" or not host.hostname or host.username or host.password:
        raise ValueError("DATABRICKS_HOST must be an HTTPS workspace URL")
    manager = env["PAIRITY_JOB_MANAGER"].strip()
    if not re.fullmatch(r"[^@\s]+@[^@\s]+", manager):
        raise ValueError(
            "PAIRITY_JOB_MANAGER must be the workbench user's Databricks user name (email); "
            "it is granted CAN_MANAGE on the deployed Jobs"
        )
    quote_table(f"{env['PAIRITY_CATALOG']}.{env['PAIRITY_SCHEMA']}.fixture")
    quote_table(env["PAIRITY_RESULT_TABLE"])
    from types import SimpleNamespace

    from pairity.artifacts import VolumeArtifactStore

    VolumeArtifactStore(env["PAIRITY_ARTIFACT_ROOT"], SimpleNamespace(files=None))


if __name__ == "__main__":
    validate(os.environ)
    print("Required Databricks-free settings are present and syntactically valid")
