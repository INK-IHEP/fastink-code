import pytest
from fastapi.testclient import TestClient

from fastink.auth.permission import query_user_permissions
from fastink.auth.user import get_user
from fastink.common.config import get_config
from fastink.main import app
from fastink.routers.status import InkStatus

client = TestClient(app)
test_username = get_config("test", "username")
test_password = get_config("test", "password")
AUTH_TYPE = get_config("auth", "type")

# Detect krb5 availability at module level
try:
    from fastink.auth.krb5 import get_krb5

    _krb5_token = get_krb5(test_username)
    KRB5_AVAILABLE = True
except Exception:
    _krb5_token = None
    KRB5_AVAILABLE = False


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
        """Test get_token endpoint. Adapts expectations based on auth type."""
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
            # krb5 not available, endpoint will fail to get token
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

    def test_token_validation(self):
        """Test token validation. Requires a valid token."""
        if not KRB5_AVAILABLE:
            pytest.skip("krb5 not available, cannot get valid token for validation test")
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
        assert response.status_code == 401
        data = response.json()
        assert data["status"] == InkStatus.TOKEN_INVALID
        assert data["msg"] == "Token validation failed"

    def test_auth_request(self):
        """Test auth_request endpoint. Requires a valid token."""
        if not KRB5_AVAILABLE:
            pytest.skip("krb5 not available, cannot get valid token for auth_request test")
        response = client.get(
            "/api/v2/auth/auth_request",
            headers={
                "Ink-Username": f"{test_username}",
                "Ink-Token": f"{_krb5_token}",
            },
        )
        assert response.status_code == 204
        assert response.headers["X-Auth-Request-User"] == test_username
        assert response.text == ""

    def test_auth_request_missing_headers(self):
        response = client.get("/api/v2/auth/auth_request")
        assert response.status_code == 401
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
        assert response.status_code == 401
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
            assert response.status_code == 403
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
        assert data["data"]["email"] == raw_data["email"]
        assert data["data"]["id"] == str(raw_data["id"])
