import pytest
from fastapi.testclient import TestClient

from fastink.auth.backends import krb5_validate_runtime as runtime
from fastink.auth.backends.ccache import parse_ccache_full
from fastink.auth.permission import query_user_permissions
from fastink.auth.user import get_user
from fastink.common.config import get_config
from fastink.main import app
from fastink.routers.status import InkStatus
from tests.test_krb5_validation import SERVICE_COMPONENTS, SERVICE_PRINCIPAL, _credential

client = TestClient(app)
test_username = get_config("test", "username")
test_password = get_config("test", "password")
AUTH_TYPE = get_config("auth", "type")

# Detect krb5 availability at module level
try:
    from fastink.auth.backends.krb5 import get_krb5

    _krb5_token = get_krb5(test_username)
    KRB5_AVAILABLE = True
except Exception:
    _krb5_token = None
    KRB5_AVAILABLE = False


def _stub_krb5_validation(monkeypatch):
    from fastink.auth.backends import krb5_validation

    settings = runtime.Krb5ValidateSettings(
        SERVICE_PRINCIPAL,
        "IHEPKRB5",
        600,
        5,
        5,
        8,
    )

    def run_kvno(cache, _settings):
        principal = parse_ccache_full(cache)["default_principal"]
        return cache + _credential(
            client_realm=principal["realm"],
            client_components=tuple(principal["components"]),
            server_realm=b"IHEPKRB5",
            server_components=tuple(SERVICE_COMPONENTS),
        )

    monkeypatch.setattr(krb5_validation, "load_krb5_validate_settings", lambda: settings)
    monkeypatch.setattr(krb5_validation, "run_kvno_validation", run_kvno)


class TestTokenAPI:

    def test_create_token(self):
        """Test token creation via krb5. In non-krb5 mode, expect graceful failure."""
        response = client.post(
            "/api/v2/auth/create_token",
            json={"username": test_username, "password": test_password},
        )
        assert response.status_code == 200
        data = response.json()
        if KRB5_AVAILABLE:
            assert data["status"] == InkStatus.SUCCESS
            assert data["msg"] == "Token updated successfully"
            assert data["data"]["method"] == "krb5"
        else:
            assert data["status"] == InkStatus.TOKEN_CREATION_FAILURE
            assert "Token update failed" in data["msg"]

    def test_get_token(self):
        response = client.get(
            "/api/v2/auth/get_token", params={"username": test_username}
        )
        assert response.status_code == 200
        data = response.json()
        if KRB5_AVAILABLE:
            assert data["status"] == InkStatus.SUCCESS
            assert data["msg"] == "Token obtained successfully"
            assert data["data"]["method"] == "krb5"
        else:
            assert data["status"] in (InkStatus.TOKEN_INVALID, InkStatus.TOKEN_CREATION_FAILURE)

    def test_create_and_get_token(self):
        """Test create_and_get_token endpoint."""
        response = client.post(
            "/api/v2/auth/create_and_get_token",
            json={"username": test_username, "password": test_password},
        )
        assert response.status_code == 200
        data = response.json()
        if KRB5_AVAILABLE:
            assert data["status"] == InkStatus.SUCCESS
            assert data["msg"] == "Token updated successfully"
            assert data["data"]["method"] == "krb5"
        else:
            # Endpoint will try krb5 path when auth.type == "krb5" and fail
            if AUTH_TYPE == "krb5":
                assert data["status"] == InkStatus.TOKEN_CREATION_FAILURE
            else:
                assert data["status"] == InkStatus.SUCCESS
                assert data["data"]["method"] == AUTH_TYPE

    def test_token_validation(self, monkeypatch):
        """Test token validation. Requires a valid token."""
        if not KRB5_AVAILABLE:
            pytest.skip("krb5 not available, cannot get valid token for validation test")
        _stub_krb5_validation(monkeypatch)
        response = client.post(
            "/api/v2/auth/validate_token",
            headers={
                "Ink-Username": f"{test_username}",
                "Ink-Token": f"{_krb5_token}",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.OK
        assert data["msg"] == "Token validation successful"

    def test_token_validation_failure(self):
        incorrect_token = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        response = client.post(
            "/api/v2/auth/validate_token",
            headers={
                "Ink-Username": f"{test_username}",
                "Ink-Token": f"{incorrect_token}",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.TOKEN_INVALID
        assert data["msg"] == "Token validation failed"

    def test_auth_request(self, monkeypatch):
        """Test auth_request endpoint. Requires a valid token."""
        if not KRB5_AVAILABLE:
            pytest.skip("krb5 not available, cannot get valid token for auth_request test")
        _stub_krb5_validation(monkeypatch)
        response = client.get(
            "/api/v2/auth/auth_request",
            headers={
                "Ink-Username": f"{test_username}",
                "Ink-Token": f"{_krb5_token}",
            },
        )
        assert response.status_code == 200
        assert response.headers["X-Auth-Request-User"] == test_username
        data = response.json()
        assert data["status"] == InkStatus.OK
        assert data["msg"] == "Token validation successful"
        assert data["data"] is None

    def test_auth_request_missing_headers(self):
        response = client.get("/api/v2/auth/auth_request")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.TOKEN_INVALID
        assert data["msg"] == "Ink-Username or Ink-Token is missing in request headers"

    def test_auth_request_failure(self):
        incorrect_token = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        response = client.get(
            "/api/v2/auth/auth_request",
            headers={
                "Ink-Username": f"{test_username}",
                "Ink-Token": f"{incorrect_token}",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.TOKEN_INVALID
        assert data["msg"] == "Token validation failed"

    def test_ip_whitelist(self):
        if get_config("common", "ip_whitelist_access"):
            response = client.get(
                "/api/v2/auth/get_token",
                params={"username": test_username},
                headers={"X-Real-IP": "172.17.0.1"},
            )
            assert response.status_code == 200
            response = client.get(
                "/api/v2/auth/get_token",
                params={"username": test_username},
                headers={"X-Real-IP": "8.8.8.8"},
            )
            # FastINK convention: always HTTP 200, business error in status
            assert response.status_code == 200
            assert response.json()["status"] == InkStatus.IP_BANNED
        else:
            pass

    def test_ip_whitelist_for_download_file(self):
        if get_config("common", "ip_whitelist_access"):
            token = _krb5_token or ""
            target_path = "~/ABCDEFGHIJKLMNOPQRSTUVWXYZ.txt"
            response = client.get(
                f"/api/v2/fs/download_file?TargetPath={target_path}",
                headers={
                    "X-Real-IP": "8.8.8.8",
                    "Ink-Username": f"{test_username}",
                    "Ink-Token": f"{token}",
                },
            )
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == InkStatus.EMPTY_PATH
            assert (
                data["msg"] == f"Failed to download {target_path}. TargetPath is empty"
            )
            assert data["data"] is None
        else:
            pass


class TestAuthAPI:

    def test_get_permission(self):
        response = client.get(
            "/api/v2/auth/get_permission",
            params={"username": test_username},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.SUCCESS
        assert data["msg"] == "Permissions obtained successfully"
        assert "permissions" in data["data"]
        assert "default" in data["data"]
        assert data["data"]["permissions"] == query_user_permissions(
            username=test_username
        )

    def test_get_user(self):
        response = client.get(
            "/api/v2/auth/get_user",
            params={"username": test_username},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.SUCCESS
        assert data["msg"] == f"User {test_username} retrieved successfully"
        raw_data = get_user(username=test_username)
        assert data["data"]["username"] == raw_data["username"]
        assert data["data"]["created_at"] == raw_data["created_at"].isoformat()
        assert data["data"]["updated_at"] == raw_data["updated_at"].isoformat()
        assert data["data"]["uid"] == raw_data["uid"]
        assert data["data"]["id"] == str(raw_data["id"])

    def test_get_user_missing_returns_failure_envelope(self):
        response = client.get(
            "/api/v2/auth/get_user",
            params={"username": "__missing_get_user_regression__"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.USER_QUERY_FAILURE
        assert data["data"] is None


def test_failed_validation_does_not_log_token(monkeypatch):
    import fastink.routers.headers as headers_mod
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    logged = []

    class FakeLogger:
        def warning(self, *args):
            logged.append(args)

        def error(self, *args):
            logged.append(args)

        def debug(self, *args):
            pass

    monkeypatch.setattr(headers_mod, "logger", FakeLogger())
    protected_app = FastAPI()
    protected_app.add_middleware(headers_mod.UserValidationMiddleware, skip_routers=[])

    @protected_app.get("/api/v2/probe")
    def probe():
        return {}

    secret = "s3cr3t-token-that-must-not-leak"
    response = TestClient(protected_app).get(
        "/api/v2/probe", headers={"Ink-Username": "alice", "Ink-Token": secret}
    )
    assert response.json()["status"] == InkStatus.USER_INVALID
    assert secret not in str(logged)
    assert logged


class TestGetConfigAPI:
    """/api/v2/auth/get_config: whitelisted service IP or admin token."""

    def _get(self, headers=None):
        return client.get("/api/v2/auth/get_config", headers=headers or {})

    def test_whitelisted_ip_allowed_without_token(self, monkeypatch):
        monkeypatch.setattr(
            "fastink.routers.headers.ip_is_whitelisted",
            lambda *a, **k: True,
        )
        response = self._get()
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.SUCCESS
        assert isinstance(data["data"], dict)
        assert "security" in data["data"]

    def test_admin_token_allowed(self, monkeypatch):
        monkeypatch.setattr(
            "fastink.routers.headers.ip_is_whitelisted",
            lambda *a, **k: False,
        )
        monkeypatch.setattr(
            "fastink.routers.headers.validate_token",
            lambda u, t: True,
        )
        monkeypatch.setattr(
            "fastink.auth.permission.check_user_permission",
            lambda **k: True,
        )
        response = self._get(
            headers={"Ink-Username": "admin", "Ink-Token": "valid-token"}
        )
        assert response.status_code == 200
        assert response.json()["status"] == InkStatus.SUCCESS

    def test_non_admin_token_denied(self, monkeypatch):
        monkeypatch.setattr(
            "fastink.routers.headers.ip_is_whitelisted",
            lambda *a, **k: False,
        )
        monkeypatch.setattr(
            "fastink.routers.headers.validate_token",
            lambda u, t: True,
        )
        monkeypatch.setattr(
            "fastink.auth.permission.check_user_permission",
            lambda **k: False,
        )
        response = self._get(
            headers={"Ink-Username": "user", "Ink-Token": "valid-token"}
        )
        assert response.status_code == 200
        assert response.json()["status"] == InkStatus.PERMMISSION_DENIED
        assert response.json()["data"] is None

    def test_no_token_denied(self, monkeypatch):
        monkeypatch.setattr(
            "fastink.routers.headers.ip_is_whitelisted",
            lambda *a, **k: False,
        )
        response = self._get()
        assert response.status_code == 200
        assert response.json()["status"] == InkStatus.PERMMISSION_DENIED

    def test_invalid_token_denied(self, monkeypatch):
        monkeypatch.setattr(
            "fastink.routers.headers.ip_is_whitelisted",
            lambda *a, **k: False,
        )
        monkeypatch.setattr(
            "fastink.routers.headers.validate_token",
            lambda u, t: False,
        )
        response = self._get(
            headers={"Ink-Username": "user", "Ink-Token": "garbage"}
        )
        assert response.status_code == 200
        assert response.json()["status"] == InkStatus.PERMMISSION_DENIED

    def test_permission_check_failure_fails_closed(self, monkeypatch):
        monkeypatch.setattr(
            "fastink.routers.headers.ip_is_whitelisted",
            lambda *a, **k: False,
        )
        monkeypatch.setattr(
            "fastink.routers.headers.validate_token",
            lambda u, t: True,
        )
        monkeypatch.setattr(
            "fastink.auth.permission.check_user_permission",
            lambda **k: (_ for _ in ()).throw(ConnectionError("db down")),
        )
        response = self._get(
            headers={"Ink-Username": "user", "Ink-Token": "valid-token"}
        )
        assert response.status_code == 200
        assert response.json()["status"] == InkStatus.PERMMISSION_DENIED

    def test_ip_is_whitelisted_helper(self):
        from fastink.routers.headers import ip_is_whitelisted

        assert ip_is_whitelisted("127.0.0.1", ["127.0.0.1"]) is True
        assert ip_is_whitelisted("192.168.1.5", ["192.168.0.0/16"]) is True
        assert ip_is_whitelisted("203.0.113.1", ["127.0.0.1"]) is False
        assert ip_is_whitelisted("not-an-ip", ["127.0.0.1"]) is False
        assert ip_is_whitelisted("1.2.3.4", ["malformed-entry"]) is False


def test_create_token_provision_passes_password(monkeypatch):
    """Regression: /create_token must forward the password when
    auto-provisioning an unknown user — the password backend requires
    kwargs["password"]; previously the missing argument raised KeyError
    that a bare except swallowed into USER_CREATION_FAILURE, so first
    login in password mode could never provision."""
    import fastink.auth.user as user_module
    import fastink.routers.v2.auth_manager as auth_manager_mod

    recorded = {}
    monkeypatch.setattr(user_module, "get_user", lambda **kw: {})
    monkeypatch.setattr(
        auth_manager_mod, "get_config",
        lambda section=None, option=None, **kw: (
            "password" if (section, option) == ("auth", "type") else None
        ),
    )

    def fake_add_user(**kw):
        recorded.update(kw)
        raise ValueError("stop before token creation")

    monkeypatch.setattr(user_module, "add_user", fake_add_user)
    response = client.post(
        "/api/v2/auth/create_token",
        json={"username": "provision_probe", "password": "pw123"},
    )
    assert response.status_code == 200
    assert response.json()["status"] == InkStatus.USER_CREATION_FAILURE
    assert recorded.get("password") == "pw123"


def test_create_token_no_provision_on_non_password_backend(monkeypatch):
    """Regression: on krb5/OIDC stacks an anonymous /create_token for an
    unknown user must not insert rows — provisioning is gated to the
    password backend and the typed USER_NOT_FOUND envelope is returned
    (previously the insert succeeded and the failed token issuance left
    an orphan user behind)."""
    import fastink.auth.user as user_module
    import fastink.routers.v2.auth_manager as auth_manager_mod
    from fastink.auth.backends.errors import UserNotFoundError

    monkeypatch.setattr(user_module, "get_user", lambda **kw: {})

    def forbid_add(**kw):
        raise AssertionError("add_user must not run on non-password backends")

    monkeypatch.setattr(user_module, "add_user", forbid_add)
    monkeypatch.setattr(
        auth_manager_mod, "get_config",
        lambda section=None, option=None, **kw: (
            "krb5" if (section, option) == ("auth", "type") else None
        ),
    )

    class FakeBackend:
        def create_token(self, username, password=None, expire_in=86400):
            raise UserNotFoundError(username)

    monkeypatch.setattr(auth_manager_mod, "get_auth_backend", lambda *a, **k: FakeBackend())
    response = client.post(
        "/api/v2/auth/create_token",
        json={"username": "ghost_user_probe", "password": "x"},
    )
    assert response.status_code == 200
    assert response.json()["status"] == InkStatus.USER_NOT_FOUND


def test_create_and_get_token_no_provision_on_non_password_backend(monkeypatch):
    """create_and_get_token must enforce the same provisioning gate as
    create_token: no user insert on non-password backends."""
    import fastink.auth.user as user_module
    import fastink.routers.v2.auth_manager as auth_manager_mod
    from fastink.auth.backends.errors import UserNotFoundError

    monkeypatch.setattr(user_module, "get_user", lambda **kw: {})

    def forbid_add(**kw):
        raise AssertionError("add_user must not run on non-password backends")

    monkeypatch.setattr(user_module, "add_user", forbid_add)
    monkeypatch.setattr(
        auth_manager_mod, "get_config",
        lambda section=None, option=None, **kw: (
            "krb5" if (section, option) == ("auth", "type") else None
        ),
    )

    class FakeBackend:
        def create_token(self, username, password=None, expire_in=86400):
            raise UserNotFoundError(username)

    monkeypatch.setattr(auth_manager_mod, "get_auth_backend", lambda *a, **k: FakeBackend())
    response = client.post(
        "/api/v2/auth/create_and_get_token",
        json={"username": "ghost_user_probe", "password": "x"},
    )
    assert response.status_code == 200
    assert response.json()["status"] == InkStatus.USER_NOT_FOUND
