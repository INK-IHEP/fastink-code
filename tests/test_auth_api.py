from fastapi.testclient import TestClient

from src.auth.krb5 import get_krb5
from src.auth.permission import query_user_permissions
from src.auth.user import delete_user, get_user
from src.common.config import get_config
from src.main import app
from src.routers.status import InkStatus

client = TestClient(app)
test_username = get_config("test", "username")
test_password = get_config("test", "password")


class TestTokenAPI:

    def test_create_token(self):
        response = client.post(
            "/api/v2/auth/create_token",
            json={"username": test_username, "password": test_password},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.SUCCESS
        assert data["msg"] == "Token updated successfully"
        assert data["data"]["method"] == "krb5"

    def test_get_token(self):
        response = client.get(
            "/api/v2/auth/get_token", params={"username": test_username}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.SUCCESS
        assert data["msg"] == "Token obtained successfully"
        assert data["data"]["method"] == "krb5"
        assert data["data"]["token"] == get_krb5(test_username)

    def test_create_and_get_token(self):
        response = client.post(
            "/api/v2/auth/create_and_get_token",
            json={"username": test_username, "password": test_password},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.SUCCESS
        assert data["msg"] == "Token updated successfully"
        assert data["data"]["method"] == "krb5"
        assert data["data"]["token"] == get_krb5(test_username)

    def test_token_validation(self):
        correct_token = get_krb5(test_username)
        response = client.post(
            "/api/v2/auth/validate_token",
            headers={
                "Ink-Username": f"{test_username}",
                "Ink-Token": f"{correct_token}",
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
            token = get_krb5(test_username)
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

    # def test_create_user(self):
    #     try:
    #         delete_user("testuser")
    #     except:
    #         pass
    #     response = client.post(
    #         "/api/v2/auth/create_user",
    #         json={"username": "testuser"},
    #     )
    #     assert response.status_code == 200
    #     data = response.json()
    #     assert data["status"] == InkStatus.SUCCESS
    #     assert data["msg"] == "User testuser created successfully"
    #     assert data["data"]["username"] == "testuser"
    #     delete_user("testuser")

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
