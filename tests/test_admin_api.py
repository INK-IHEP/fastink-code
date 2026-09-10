from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.exc import NoResultFound

from fastink.auth import common, permission, user
from fastink.auth.permission import query_user_permissions
from fastink.common.config import get_config
from fastink.main import app
from fastink.routers.v2 import admin_manager
from fastink.routers.headers import UserValidationMiddleware
from fastink.routers.status import InkStatus


client = TestClient(app)
test_username = str(get_config("test", "username"))


def _headers(username: str) -> dict[str, str]:
    return {"Ink-Username": username, "Ink-Token": f"admin-api-test-{username}"}


ADMIN_ROUTE_REQUESTS = [
    ("GET", "/get_user", {"username": "missing"}),
    ("GET", "/list_users", None),
    ("POST", "/create_user", {"username": "missing"}),
    ("POST", "/update_user", {"username": "missing"}),
    ("POST", "/delete_user", {"username": "missing"}),
    ("GET", "/get_user_token", {"username": "missing"}),
    ("GET", "/get_user_permissions", {"username": "missing"}),
    ("POST", "/add_user_permission", {"username": "missing", "permission": "admin"}),
    ("POST", "/delete_user_permission", {"username": "missing", "permission": "admin"}),
    ("GET", "/list_permissions", None),
    ("POST", "/create_permission", {"permission": "missing"}),
    ("POST", "/delete_permission", {"permission": "missing"}),
    ("GET", "/get_users_by_permission", {"permission": "admin"}),
    ("GET", "/list_group_permissions", None),
    ("POST", "/add_group_permission", {"group_name": "missing", "permission": "admin"}),
    ("POST", "/delete_group_permission", {"group_name": "missing", "permission": "admin"}),
]


def _assert_success(response):
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == InkStatus.SUCCESS
    return body


def _ensure_user_permission(username: str, permission_name: str) -> None:
    user_record = common.get_user(username=username)
    permission_record = common.get_permission(permission=permission_name)
    try:
        common.get_user_permission(
            user_id=user_record["id"], permission_id=permission_record["id"]
        )
    except NoResultFound:
        permission.add_user_permission(
            username=username, permission=permission_name
        )


def _remove_user_permission(username: str, permission_name: str) -> None:
    try:
        user_record = common.get_user(username=username)
        permission_record = common.get_permission(permission=permission_name)
        common.get_user_permission(
            user_id=user_record["id"], permission_id=permission_record["id"]
        )
    except NoResultFound:
        return
    common.delete_user_permission(
        user_id=user_record["id"], permission_id=permission_record["id"]
    )


@pytest.fixture(autouse=True)
def accept_test_tokens(monkeypatch):
    monkeypatch.setattr(
        "fastink.routers.headers.validate_token",
        lambda username, token: bool(username and token),
    )


@pytest.fixture(scope="module")
def admin_username():
    if not user.get_user(username=test_username):
        common.add_user(username=test_username)
    try:
        common.get_permission(permission="admin")
    except NoResultFound:
        common.add_permission(permission="admin")
    _ensure_user_permission(test_username, "admin")
    return test_username


@pytest.fixture(scope="module")
def non_admin_username():
    created = False
    username = None
    for record in user.list_users():
        candidate = record["username"]
        if candidate == test_username:
            continue
        try:
            permissions = query_user_permissions(username=candidate)
        except Exception:
            permissions = []
        if "admin" not in permissions:
            username = candidate
            break

    if username is None:
        username = f"admin_api_non_admin_{uuid4().hex[:10]}"
        common.add_user(username=username)
        created = True
    yield username
    if created:
        user.delete_user(username=username)


def test_admin_routes_are_registered(non_admin_username):
    for method, path, payload in ADMIN_ROUTE_REQUESTS:
        request = client.request(
            method,
            f"/api/v2/admin{path}",
            params=payload if method == "GET" else None,
            json=payload if method == "POST" else None,
            headers=_headers(non_admin_username),
        )
        assert request.status_code != 404, f"{method} {path} is not registered"


@pytest.mark.parametrize("method, path, payload", ADMIN_ROUTE_REQUESTS)
def test_non_admin_is_denied_for_every_admin_route(
    method, path, payload, non_admin_username
):
    response = client.request(
        method,
        f"/api/v2/admin{path}",
        params=payload if method == "GET" else None,
        json=payload if method == "POST" else None,
        headers=_headers(non_admin_username),
    )

    body = response.json()
    assert response.status_code == 200
    assert body["status"] == InkStatus.PERMMISSION_DENIED
    assert "admin" in body["msg"].lower()


def test_auth_and_admin_get_user_routes_coexist(non_admin_username):
    auth_response = client.get(
        "/api/v2/auth/get_user",
        params={"username": non_admin_username},
        headers=_headers(non_admin_username),
    )
    assert auth_response.status_code == 200
    assert auth_response.json()

    admin_response = client.get(
        "/api/v2/admin/get_user",
        params={"username": test_username},
        headers=_headers(non_admin_username),
    )
    assert admin_response.status_code == 200
    assert admin_response.json()["status"] == InkStatus.PERMMISSION_DENIED


def test_missing_headers_are_rejected_by_middleware():
    protected_app = FastAPI()
    protected_app.add_middleware(UserValidationMiddleware, skip_routers=[])

    @protected_app.get("/api/v2/admin/list_users")
    def list_users():
        return {"status": InkStatus.SUCCESS, "msg": "unexpected", "data": None}

    response = TestClient(protected_app).get("/api/v2/admin/list_users")

    assert response.status_code == 200
    assert response.json() == {
        "status": InkStatus.TOKEN_INVALID,
        "msg": "Ink-Username or Ink-Token is missing in request headers",
        "data": None,
    }


def test_failure_logs_exception_without_exposing_details(monkeypatch):
    logged_calls = []

    def capture_warning(*args, **kwargs):
        logged_calls.append((args, kwargs))

    monkeypatch.setattr(admin_manager.logger, "warning", capture_warning)
    error = RuntimeError("sensitive SQL fragment")

    body = admin_manager._failure(
        InkStatus.USER_QUERY_FAILURE,
        "User query failed",
        error,
    )

    assert body["status"] == InkStatus.USER_QUERY_FAILURE
    assert body["msg"] == "User query failed"
    assert "sensitive SQL fragment" not in body["msg"]
    assert logged_calls
    assert "User query failed" in " ".join(str(value) for value in logged_calls[0][0])
    assert "sensitive SQL fragment" in " ".join(str(value) for value in logged_calls[0][0])


def test_admin_crud_round_trip(admin_username):
    username = f"admin_api_user_{uuid4().hex[:10]}"
    updated_username = f"admin_api_updated_{uuid4().hex[:10]}"
    permission_name = f"admin_api_permission_{uuid4().hex[:10]}"
    group_name = f"admin_api_group_{uuid4().hex[:10]}"
    headers = _headers(admin_username)

    try:
        body = _assert_success(
            client.post(
                "/api/v2/admin/create_user",
                json={"username": username},
                headers=headers,
            )
        )
        assert body["data"]["username"] == username

        body = _assert_success(
            client.get(
                "/api/v2/admin/get_user",
                params={"username": username},
                headers=headers,
            )
        )
        assert body["data"]["username"] == username

        _assert_success(
            client.post(
                "/api/v2/admin/update_user",
                json={"username": username, "new_username": updated_username},
                headers=headers,
            )
        )

        body = _assert_success(
            client.post(
                "/api/v2/admin/add_user_permission",
                json={"username": updated_username, "permission": "admin"},
                headers=headers,
            )
        )
        assert body["data"]["permission"] == "admin"

        body = _assert_success(
            client.get(
                "/api/v2/admin/get_user_permissions",
                params={"username": updated_username},
                headers=headers,
            )
        )
        assert "admin" in body["data"]

        _assert_success(
            client.post(
                "/api/v2/admin/delete_user_permission",
                json={"username": updated_username, "permission": "admin"},
                headers=headers,
            )
        )

        _assert_success(
            client.post(
                "/api/v2/admin/create_permission",
                json={"permission": permission_name},
                headers=headers,
            )
        )

        body = _assert_success(
            client.get("/api/v2/admin/list_permissions", headers=headers)
        )
        assert any(item["permission"] == permission_name for item in body["data"])

        _assert_success(
            client.post(
                "/api/v2/admin/delete_permission",
                json={"permission": permission_name},
                headers=headers,
            )
        )

        _assert_success(
            client.post(
                "/api/v2/admin/add_group_permission",
                json={"group_name": group_name, "permission": "admin"},
                headers=headers,
            )
        )

        body = _assert_success(
            client.get(
                "/api/v2/admin/list_group_permissions",
                params={"group": group_name},
                headers=headers,
            )
        )
        assert body["data"] == [{"group_name": group_name, "permission": "admin"}]

        _assert_success(
            client.post(
                "/api/v2/admin/delete_group_permission",
                json={"group_name": group_name, "permission": "admin"},
                headers=headers,
            )
        )

        body = _assert_success(
            client.get(
                "/api/v2/admin/get_users_by_permission",
                params={"permission": "admin"},
                headers=headers,
            )
        )
        assert any(item["username"] == admin_username for item in body["data"])

        body = _assert_success(
            client.get(
                "/api/v2/admin/get_user_permissions",
                params={"username": updated_username},
                headers=headers,
            )
        )
        for default_permission in ("cpu", "AlmaLinux9"):
            if default_permission in body["data"]:
                _assert_success(
                    client.post(
                        "/api/v2/admin/delete_user_permission",
                        json={
                            "username": updated_username,
                            "permission": default_permission,
                        },
                        headers=headers,
                    )
                )

        _assert_success(
            client.post(
                "/api/v2/admin/delete_user",
                json={"username": updated_username},
                headers=headers,
            )
        )
        assert not user.get_user(username=updated_username)
    finally:
        for candidate in (username, updated_username):
            for permission_name_to_remove in ("admin", "cpu", "AlmaLinux9"):
                _remove_user_permission(candidate, permission_name_to_remove)
            if user.get_user(username=candidate):
                user.delete_user(username=candidate)
        permission.delete_permission(permission_name)
        permission.delete_user_permission(username=updated_username, permission="admin")
        common.delete_group_permission(
            group_name=group_name,
            permission_id=common.get_permission(permission="admin")["id"],
        )


def test_admin_check_reads_fresh_rows_and_fails_closed_for_missing_user(admin_username):
    fresh_username = f"admin_api_fresh_{uuid4().hex[:10]}"
    missing_username = f"admin_api_missing_{uuid4().hex[:10]}"
    common.add_user(username=fresh_username)
    permission.add_user_permission(username=fresh_username, permission="admin")

    try:
        response = client.get(
            "/api/v2/admin/list_users",
            headers=_headers(fresh_username),
        )
        assert response.json()["status"] == InkStatus.SUCCESS

        response = client.get(
            "/api/v2/admin/list_users",
            headers=_headers(missing_username),
        )
        body = response.json()
        assert body["status"] == InkStatus.PERMMISSION_DENIED
        assert "admin" in body["msg"].lower()
    finally:
        _remove_user_permission(fresh_username, "admin")
        user.delete_user(username=fresh_username)


def test_admin_self_destruction_is_refused(admin_username):
    headers = _headers(admin_username)

    body = client.post(
        "/api/v2/admin/delete_user",
        json={"username": admin_username},
        headers=headers,
    ).json()
    assert body["status"] == InkStatus.PERMMISSION_DENIED
    assert "own account" in body["msg"]

    body = client.post(
        "/api/v2/admin/delete_user_permission",
        json={"username": admin_username, "permission": "admin"},
        headers=headers,
    ).json()
    assert body["status"] == InkStatus.PERMMISSION_DENIED
    assert "own admin" in body["msg"]

    body = client.post(
        "/api/v2/admin/delete_permission",
        json={"permission": "admin"},
        headers=headers,
    ).json()
    assert body["status"] == InkStatus.PERMMISSION_DENIED
    assert "built-in" in body["msg"]


def test_admin_gate_rejects_invalid_token_for_admin(admin_username, monkeypatch):
    monkeypatch.setattr(
        "fastink.routers.headers.validate_token",
        lambda username, token: token != "invalid-admin-token",
    )

    response = client.get(
        "/api/v2/admin/list_users",
        headers={"Ink-Username": admin_username, "Ink-Token": "invalid-admin-token"},
    )

    assert response.status_code == 200
    assert response.json()["status"] == InkStatus.PERMMISSION_DENIED


def test_admin_gate_rejects_missing_token_for_admin(admin_username):
    response = client.get(
        "/api/v2/admin/list_users",
        headers={"Ink-Username": admin_username},
    )

    assert response.status_code == 200
    assert response.json()["status"] == InkStatus.PERMMISSION_DENIED


def test_delete_user_guard_normalizes_case_and_padding(admin_username):
    response = client.post(
        "/api/v2/admin/delete_user",
        json={"username": f" {admin_username.upper()} "},
        headers=_headers(admin_username),
    )

    assert response.json()["status"] == InkStatus.PERMMISSION_DENIED


def test_delete_user_permission_guard_normalizes_case_and_padding(admin_username):
    response = client.post(
        "/api/v2/admin/delete_user_permission",
        json={"username": f" {admin_username.upper()} ", "permission": " ADMIN "},
        headers=_headers(admin_username),
    )

    assert response.json()["status"] == InkStatus.PERMMISSION_DENIED


@pytest.mark.parametrize("permission_name", ["ADMIN", "admin "])
def test_delete_permission_guard_normalizes_case_and_padding(
    permission_name, admin_username, monkeypatch
):
    called = []
    monkeypatch.setattr(
        admin_manager.permission,
        "delete_permission",
        lambda permission: called.append(permission) or True,
    )

    response = client.post(
        "/api/v2/admin/delete_permission",
        json={"permission": permission_name},
        headers=_headers(admin_username),
    )

    assert response.json()["status"] == InkStatus.PERMMISSION_DENIED
    assert called == []


def test_admin_cannot_rename_self_case_insensitively(admin_username):
    response = client.post(
        "/api/v2/admin/update_user",
        json={
            "username": f" {admin_username.upper()} ",
            "new_username": f"admin_api_renamed_{uuid4().hex[:10]}",
        },
        headers=_headers(admin_username),
    )

    assert response.json()["status"] == InkStatus.PERMMISSION_DENIED


def test_group_admin_cannot_remove_their_admin_source(admin_username, monkeypatch):
    username = f"admin_api_group_admin_{uuid4().hex[:10]}"
    group_name = f"admin_api_source_group_{uuid4().hex[:10]}"
    admin_permission_id = common.get_permission(permission="admin")["id"]
    common.add_user(username=username)
    common.add_group_permission(
        group_name=group_name,
        permission_id=admin_permission_id,
    )

    def get_user_groups(candidate):
        return [group_name] if candidate == username else []

    monkeypatch.setattr("fastink.auth.permission.get_user_groups", get_user_groups)

    try:
        response = client.post(
            "/api/v2/admin/delete_group_permission",
            json={"group_name": group_name, "permission": "admin"},
            headers=_headers(username),
        )
        assert response.json()["status"] == InkStatus.PERMMISSION_DENIED
    finally:
        common.delete_group_permission(
            group_name=group_name,
            permission_id=admin_permission_id,
        )
        user.delete_user(username=username)


def test_direct_admin_can_remove_unrelated_admin_group_mapping(admin_username):
    group_name = f"admin_api_unrelated_group_{uuid4().hex[:10]}"
    admin_permission_id = common.get_permission(permission="admin")["id"]
    common.add_group_permission(
        group_name=group_name,
        permission_id=admin_permission_id,
    )

    try:
        response = client.post(
            "/api/v2/admin/delete_group_permission",
            json={"group_name": group_name, "permission": "admin"},
            headers=_headers(admin_username),
        )
        assert response.json()["status"] == InkStatus.SUCCESS
    finally:
        common.delete_group_permission(
            group_name=group_name,
            permission_id=admin_permission_id,
        )


def test_get_users_by_permission_includes_group_derived_users(
    admin_username, monkeypatch
):
    username = f"admin_api_group_user_{uuid4().hex[:10]}"
    group_name = f"admin_api_lookup_group_{uuid4().hex[:10]}"
    admin_permission_id = common.get_permission(permission="admin")["id"]
    common.add_user(username=username)
    common.add_group_permission(
        group_name=group_name,
        permission_id=admin_permission_id,
    )

    def get_users_groups(usernames):
        return {
            candidate: [group_name] if candidate == username else []
            for candidate in usernames
        }

    monkeypatch.setattr("fastink.auth.groups.get_users_groups", get_users_groups)

    try:
        response = client.get(
            "/api/v2/admin/get_users_by_permission",
            params={"permission": "admin"},
            headers=_headers(admin_username),
        )
        body = response.json()
        assert body["status"] == InkStatus.SUCCESS
        assert any(record["username"] == username for record in body["data"])
    finally:
        common.delete_group_permission(
            group_name=group_name,
            permission_id=admin_permission_id,
        )
        user.delete_user(username=username)


def test_create_permission_rejects_whitespace_only(admin_username):
    response = client.post(
        "/api/v2/admin/create_permission",
        json={"permission": "   "},
        headers=_headers(admin_username),
    )

    assert response.status_code == 422
    assert response.json()["status"] == "422"


def test_create_permission_strips_whitespace(admin_username):
    permission_name = f"admin_api_trim_{uuid4().hex[:10]}"

    try:
        response = client.post(
            "/api/v2/admin/create_permission",
            json={"permission": f"  {permission_name}  "},
            headers=_headers(admin_username),
        )
        body = response.json()
        assert body["status"] == InkStatus.SUCCESS
        assert body["data"]["permission"] == permission_name
        assert any(
            item["permission"] == permission_name
            for item in common.get_all_permissions()
        )
    finally:
        permission.delete_permission(permission_name)
        permission.delete_permission(f"  {permission_name}  ")


def test_create_user_rejects_negative_uid(admin_username):
    username = f"admin_api_negative_uid_{uuid4().hex[:10]}"

    try:
        response = client.post(
            "/api/v2/admin/create_user",
            json={"username": username, "uid": -1},
            headers=_headers(admin_username),
        )
        assert response.status_code == 422
        assert response.json()["status"] == "422"
    finally:
        if user.get_user(username=username):
            for permission_name in ("cpu", "AlmaLinux9"):
                _remove_user_permission(username, permission_name)
            user.delete_user(username=username)


def test_group_admin_source_guard_matches_case_insensitively(admin_username, monkeypatch):
    username = f"admin_api_group_admin_{uuid4().hex[:10]}"
    group_name = f"admin_api_source_group_{uuid4().hex[:10]}"
    admin_permission_id = common.get_permission(permission="admin")["id"]
    common.add_user(username=username)
    common.add_group_permission(
        group_name=group_name,
        permission_id=admin_permission_id,
    )

    def get_user_groups(candidate):
        return [group_name] if candidate == username else []

    monkeypatch.setattr("fastink.auth.permission.get_user_groups", get_user_groups)

    try:
        response = client.post(
            "/api/v2/admin/delete_group_permission",
            json={"group_name": group_name.upper(), "permission": "admin"},
            headers=_headers(username),
        )
        assert response.json()["status"] == InkStatus.PERMMISSION_DENIED
    finally:
        common.delete_group_permission(
            group_name=group_name,
            permission_id=admin_permission_id,
        )
        user.delete_user(username=username)


def test_create_user_on_password_backend_requires_password(admin_username, monkeypatch):
    monkeypatch.setattr(
        "fastink.routers.v2.admin_manager.get_config",
        lambda *args: "password",
    )
    response = client.post(
        "/api/v2/admin/create_user",
        json={"username": f"admin_api_nopwd_{uuid4().hex[:10]}"},
        headers=_headers(admin_username),
    )
    body = response.json()
    assert body["status"] == InkStatus.PARAM_ERROR
    assert "password" in body["msg"]


def test_admin_get_endpoints_reject_empty_names(admin_username):
    response = client.get(
        "/api/v2/admin/get_user",
        params={"username": ""},
        headers=_headers(admin_username),
    )
    assert response.status_code == 422
