#!/usr/bin/env python3
import time
import pytest
from datetime import datetime, timedelta

from src.auth.common import (
    add_user,
    get_user,
    update_user,
    delete_user,
    add_permission,
    get_permission,
    update_permission,
    delete_permission,
    add_app,
    get_app,
    update_app,
    delete_app,
    add_authorization,
    get_authorization,
    update_authorization,
    delete_authorization,
    add_kerberos_token,
    get_kerberos_token,
    update_kerberos_token,
    delete_kerberos_token,
)


class TestUser:
    def test_add_and_get_user(self):
        # Add user
        assert add_user("testuser", "test@example.com", uid=1)
        # Get user
        user = get_user(username="testuser")
        # Assertion
        assert user is not None
        assert user["username"] == "testuser"
        assert user["email"] == "test@example.com"
        assert user["uid"] == 1
        # Remove user
        user_id = user["id"]
        delete_user(user_id)

    def test_add_user_without_email_and_uid(self):
        assert add_user("testuser")
        user = get_user(username="testuser")
        assert user is not None
        assert user["username"] == "testuser"
        assert user["email"] is None
        assert user["uid"] is None

        user_id = user["id"]
        delete_user(user_id)

    def test_update_user(self):
        # Add user
        assert add_user("testuser", "test@example.com", uid=1)
        user = get_user(username="testuser")
        assert user is not None
        user_id = user["id"]
        # Update user
        assert update_user(user_id, username="updateduser")
        assert update_user(user_id, email="updated@example.com")
        assert update_user(user_id, uid=10)
        updated_user = get_user(username="updateduser")
        assert updated_user is not None
        assert updated_user["id"] == user_id
        assert updated_user["username"] == "updateduser"
        assert updated_user["email"] == "updated@example.com"
        assert updated_user["uid"] == 10
        # Remove user
        delete_user(user_id)

    def test_delete_user(self):
        # Add user
        assert add_user("testuser", "test@example.com", uid=1)
        user = get_user(username="testuser")
        assert user is not None
        user_id = user["id"]
        # Delete user
        assert delete_user(user_id)
        try:
            get_user(username="updateduser")
        except Exception:
            assert True


class TestPermission:
    def test_add_and_get_permission(self):
        # Add permission
        assert add_permission("testpermission")
        # Get permission
        permission = get_permission(permission="testpermission")
        assert permission is not None
        assert permission["permission"] == "testpermission"
        permission_id = permission["id"]
        # Remove permission
        assert delete_permission(permission_id)

    def test_update_permission(self):
        # Add permission
        assert add_permission("testpermission")
        permission = get_permission(permission="testpermission")
        assert permission is not None
        permission_id = permission["id"]
        # Update permission
        assert update_permission(permission_id, permission="updatedpermission")
        updated_permission = get_permission(permission="updatedpermission")
        assert updated_permission["id"] == permission_id
        # Remove permission
        assert delete_permission(permission_id)

    def test_delete_permission(self):
        # Add permission
        assert add_permission("testpermission")
        permission = get_permission(permission="testpermission")
        assert permission is not None
        permission_id = permission["id"]
        # Delete permission
        assert delete_permission(permission_id)
        try:
            get_permission(permission="testpermission")
        except Exception:
            assert True


class TestApp:
    def test_add_and_get_app(self):
        # Add app
        assert add_app("testapp")
        # Get app
        app = get_app(app_name="testapp")
        assert app is not None
        assert app["app"] == "testapp"
        app_id = app["id"]
        # Remove app
        assert delete_app(app_id)

    def test_update_app(self):
        # Add app
        assert add_app("testapp")
        app = get_app(app_name="testapp")
        assert app is not None
        app_id = app["id"]
        # Update app
        assert update_app(app_id, app_name="updatedapp")
        updated_app = get_app(app_name="updatedApp")
        assert updated_app["id"] == app_id
        # Remove app
        assert delete_app(app_id)

    def test_delete_app(self):
        # Add app
        assert add_app("testapp")
        app = get_app(app_name="testapp")
        assert app is not None
        app_id = app["id"]
        # Delete app
        assert delete_app(app_id)
        try:
            get_app(app_name="UpdatedApp")
        except Exception:
            assert True


# def test_authorizations():
#     print("Testing Authorizations...")

#     # Add authorization
#     assert add_authorization("AdminAccess")

#     # Get authorization
#     authorization = get_authorization(authorization="AdminAccess")
#     assert authorization is not None
#     print("Authorization:", authorization)

#     # Update authorization
#     authorization_id = authorization["id"]
#     assert update_authorization(authorization_id, authorization="UserAccess")
#     updated_authorization = get_authorization(authorization="UserAccess")
#     assert updated_authorization["id"] == authorization_id

#     # Delete authorization
#     assert delete_authorization(authorization_id)
#     try:
#         get_authorization(authorization="UserAccess")
#     except Exception as e:
#         print(str(e))
#     print("Authorizations Test Passed\n")


# def test_kerberos_tokens():
#     print("Testing Kerberos Tokens...")

#     # Add a new user
#     assert add_user("krb5user", "krb5test@example.com", uid=1)

#     # Add Kerberos Token
#     user_id = get_user(username="krb5user")["id"]
#     generated_at = datetime.utcnow()
#     expired_at = generated_at + timedelta(days=7)
#     token = "sample_token"
#     assert add_kerberos_token(
#         user_id=user_id, token=token, generated_at=generated_at, expired_at=expired_at
#     )

#     # Get Kerberos Token
#     kerberos_token = get_kerberos_token(user_id=user_id)
#     assert kerberos_token is not None
#     assert kerberos_token["user_id"] == user_id
#     assert kerberos_token["token"] == token
#     print("Kerberos Token Retrieved:", kerberos_token)

#     # Update Kerberos Token
#     time.sleep(5)
#     updated_token = "updated_token"
#     new_generated_at = datetime.utcnow()
#     new_expired_at = new_generated_at + timedelta(days=10)
#     assert update_kerberos_token(
#         user_id=user_id,
#         token=updated_token,
#         generated_at=new_generated_at,
#         expired_at=new_expired_at,
#     )
#     updated_kerberos_token = get_kerberos_token(user_id=user_id)
#     assert updated_kerberos_token["token"] == updated_token
#     print("Kerberos Token Updated:", updated_kerberos_token)

#     # Delete Kerberos Token
#     assert delete_kerberos_token(user_id=user_id)
#     try:
#         get_kerberos_token(user_id=user_id)
#     except Exception as e:
#         print(str(e))

#     # Delete User
#     assert delete_user(user_id)
#     try:
#         get_user(username="krb5user")
#     except Exception as e:
#         print(str(e))
#     print("Kerberos Tokens Test Passed\n")
