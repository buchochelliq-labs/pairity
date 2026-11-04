from types import SimpleNamespace

import pytest
from databricks.sdk.service.iam import (
    AccessControlResponse,
    ObjectPermissions,
    Permission,
    PermissionLevel,
)
from fastapi import HTTPException

from pairity.web.runtime import AppSubmitter


def acl(level, *, user=None, group=None):
    return AccessControlResponse(
        user_name=user,
        group_name=group,
        all_permissions=[Permission(permission_level=PermissionLevel(level), inherited=True)],
    )


def authorizer(entries, query=lambda *args: []):
    from pairity.web.runtime import JobAuthorization

    calls = []

    def get(**kwargs):
        calls.append(kwargs)
        return ObjectPermissions(access_control_list=entries)

    return JobAuthorization(
        SimpleNamespace(permissions=SimpleNamespace(get=get)), 123, "user", query
    ), calls


def test_view_does_not_allow_submission_or_cancellation():
    authorize, calls = authorizer([acl("CAN_VIEW", user="user")])
    events = []
    backend = SimpleNamespace(
        submit=lambda *a, **kw: events.append("submit"),
        cancel=lambda *a: events.append("cancel"),
        status=lambda *a: {"status": "running"},
    )
    service = AppSubmitter(backend, backend, authorize)
    assert service.status(object()) == {"status": "running"}
    for action in [
        lambda: service.submit(object(), request_id="request"),
        lambda: service.cancel(object()),
    ]:
        with pytest.raises(HTTPException) as exc:
            action()
        assert exc.value.status_code == 403
    assert not events
    assert all(c == {"request_object_type": "jobs", "request_object_id": "123"} for c in calls)


@pytest.mark.parametrize("level", ["CAN_MANAGE_RUN", "CAN_MANAGE", "IS_OWNER"])
def test_direct_job_grants_allow_managing_runs(level):
    authorize, _ = authorizer([acl(level, user="user")])
    authorize("manage")
    authorize("view")


def test_group_membership_uses_bound_obo_sql_and_denies_false():
    calls = []

    def query(sql, params):
        calls.append((sql, params))
        return [{"allowed": "false"}]

    group = "operators'quoted"
    authorize, _ = authorizer([acl("CAN_MANAGE_RUN", group=group)], query)
    with pytest.raises(HTTPException):
        authorize("manage")
    assert group not in calls[0][0]
    assert calls[0][1] == {"group_name": group}
    authorize, _ = authorizer(
        [acl("CAN_MANAGE_RUN", group=group)], lambda *a: [{"allowed": "true"}]
    )
    authorize("manage")


def test_other_principal_and_unavailable_acl_never_grant_access():
    authorize, _ = authorizer([acl("IS_OWNER", user="someone-else")])
    with pytest.raises(HTTPException):
        authorize("manage")

    def unavailable(**kw):
        raise PermissionError("ACL cannot be read")

    authorize.client.permissions.get = unavailable
    with pytest.raises(PermissionError):
        authorize("manage")


def test_app_declares_only_supported_required_user_scope():
    from pathlib import Path

    import yaml

    bundle = yaml.safe_load(Path("app/databricks.yml").read_text())
    assert bundle["resources"]["apps"]["workbench"]["user_api_scopes"] == ["sql"]
