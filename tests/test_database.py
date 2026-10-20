#!/usr/bin/env python3
import time
from uuid import uuid4

import pytest
from datetime import datetime, timedelta
from sqlalchemy.exc import NoResultFound

from fastink.auth.backends import password as password_backend
from fastink.auth.backends.password import PasswordBackend
from fastink.auth.common import (
    add_user,
    get_user,
    get_user_permissions,
    update_user,
    delete_user,
    add_permission,
    get_permission,
    delete_permission,
    add_authentication,
    get_authentication,
    delete_authentication,
    add_kerberos_token,
    get_kerberos_token,
    add_token,
    get_token,
    delete_user_cascade,
)
from fastink.auth import permission as permission_auth
from fastink.auth import user as user_auth


def _ensure_password_authentication():
    try:
        return get_authentication(authentication="password")
    except NoResultFound:
        add_authentication(authentication="password")
        return get_authentication(authentication="password")


def _password_token_user(expired_at=None):
    username = f"password_token_{uuid4().hex}"
    assert add_user(username)
    authentication = _ensure_password_authentication()
    generated_at = datetime.now()
    token = password_backend._encrypt(username)
    add_token(
        user_id=get_user(username=username)["id"],
        authentication_id=authentication["id"],
        token=token,
        generated_at=generated_at,
        expired_at=expired_at or generated_at + timedelta(hours=1),
    )
    return username, get_user(username=username), authentication, token


class TestUser:
    def test_password_add_user_rejects_invalid_backend_validation(self, monkeypatch):
        backend = PasswordBackend()
        monkeypatch.setattr(
            user_auth,
            "get_config",
            lambda *args, **kwargs: "password",
        )
        monkeypatch.setattr(user_auth, "get_auth_backend", lambda name: backend)
        monkeypatch.setattr(backend, "validate_user", lambda **kwargs: False)
        username = f"pw_add_invalid_{uuid4().hex[:8]}"

        with pytest.raises(ValueError, match="Invalid username or password"):
            user_auth.add_user(username=username, password="invalid")

        with pytest.raises(NoResultFound):
            get_user(username=username)

    def test_password_add_user_creates_user_after_valid_backend_validation(
        self, monkeypatch
    ):
        backend = PasswordBackend()
        monkeypatch.setattr(
            user_auth,
            "get_config",
            lambda *args, **kwargs: "password",
        )
        monkeypatch.setattr(user_auth, "get_auth_backend", lambda name: backend)
        monkeypatch.setattr(backend, "validate_user", lambda **kwargs: True)
        username = f"pw_add_valid_{uuid4().hex[:8]}"

        try:
            assert user_auth.add_user(username=username, password="valid") is True
            assert get_user(username=username)["username"] == username
        finally:
            if user_auth.get_user(username=username):
                user_auth.delete_user(username=username)

    def test_delete_user_cascades_tokens_and_permissions(self):
        username = f"delete_cascade_user_{time.time_ns()}"
        permission_name = f"delete_cascade_perm_{time.time_ns()}"
        authentication_name = f"delete_cascade_authn_{time.time_ns()}"
        assert add_user(username)
        assert permission_auth.add_permission(permission_name)
        assert permission_auth.add_user_permission(username, permission_name)
        assert add_authentication(authentication_name)
        user = get_user(username=username)
        authentication_row = get_authentication(authentication_name)
        now = datetime.now()
        assert add_kerberos_token(
            user_id=user["id"],
            token="fake-ccache",
            generated_at=now,
            expired_at=now + timedelta(hours=25),
        )
        assert add_token(
            user_id=user["id"],
            authentication_id=authentication_row["id"],
            token="fake-token",
            generated_at=now,
            expired_at=now + timedelta(hours=25),
        )

        try:
            assert user_auth.delete_user(username=username)
            assert user_auth.get_user(username=username) == {}
            with pytest.raises(NoResultFound):
                get_user_permissions(user_id=user["id"])
            with pytest.raises(NoResultFound):
                get_kerberos_token(user_id=user["id"])
            with pytest.raises(NoResultFound):
                get_token(user_id=user["id"])
        finally:
            existing_user = user_auth.get_user(username=username)
            if existing_user:
                permission_auth.delete_user_permission(username, permission_name)
                delete_user(existing_user["id"])
            permission = get_permission(permission=permission_name)
            if permission:
                delete_permission(permission["id"])
            delete_authentication(authentication_row["id"])

    def test_delete_user_removes_user_permissions(self):
        username = f"delete_permission_user_{time.time_ns()}"
        permission_name = f"delete_permission_{time.time_ns()}"
        assert add_user(username)
        assert permission_auth.add_permission(permission_name)
        assert permission_auth.add_user_permission(username, permission_name)
        user = get_user(username=username)

        try:
            assert user_auth.delete_user(username=username)
            assert user_auth.get_user(username=username) == {}
            with pytest.raises(NoResultFound):
                get_user_permissions(user_id=user["id"])
        finally:
            existing_user = user_auth.get_user(username=username)
            if existing_user:
                permission_auth.delete_user_permission(username, permission_name)
                delete_user(existing_user["id"])
            permission = get_permission(permission=permission_name)
            if permission:
                delete_permission(permission["id"])

    def test_duplicate_user_permission_grant_returns_false_not_raises(self):
        """Regression: re-granting an existing permission must return False
        (idempotent), not crash — common.add_user_permission used to
        reconstruct IntegrityError with a single argument, which raises
        TypeError under SQLAlchemy 2.x and escaped all IntegrityError
        handlers."""
        username = f"dup_grant_user_{time.time_ns()}"
        permission_name = f"dup_grant_{time.time_ns()}"
        assert add_user(username)
        assert permission_auth.add_permission(permission_name)
        try:
            assert permission_auth.add_user_permission(username, permission_name) is True
            assert permission_auth.add_user_permission(username, permission_name) is False
        finally:
            permission_auth.delete_user_permission(username, permission_name)
            delete_user(get_user(username=username)["id"])
            permission = get_permission(permission=permission_name)
            if permission:
                delete_permission(permission["id"])

    def test_get_user_without_criteria_raises(self):
        """Regression: get_user() with no selectors must not run an
        unfiltered query that returns an arbitrary user or raises
        MultipleResultsFound on a multi-user database."""
        with pytest.raises(ValueError, match="non-empty username or a uid"):
            get_user()

    def test_get_user_blank_username_is_not_a_selector(self):
        """Regression: a blank username must be treated as absent, not as
        a pass-through to an unfiltered query."""
        with pytest.raises(ValueError, match="non-empty username or a uid"):
            get_user(username="")
        with pytest.raises(ValueError, match="non-empty username or a uid"):
            get_user(username="   ")
        assert user_auth.get_user(username="") == {}
        assert user_auth.get_user(username="   ") == {}

    def test_get_user_blank_username_with_uid_filters_by_uid(self):
        uid_val = int(time.time_ns() % 2_000_000_000)
        username = f"blank_uname_{time.time_ns()}"
        assert add_user(username, uid=uid_val)
        try:
            # Both empty and whitespace-only usernames are treated as
            # absent selectors, so the query resolves by uid alone.
            user = get_user(username="", uid=uid_val)
            assert user["username"] == username
            user = get_user(username="   ", uid=uid_val)
            assert user["username"] == username
        finally:
            delete_user(get_user(username=username)["id"])

    def test_add_and_get_user(self):
        # Add user
        assert add_user("testuser", uid=1)
        # Get user
        user = get_user(username="testuser")
        # Assertion
        assert user is not None
        assert user["username"] == "testuser"
        assert user["uid"] == 1
        # Remove user
        user_id = user["id"]
        delete_user(user_id)

    def test_add_user_without_uid(self):
        assert add_user("testuser")
        user = get_user(username="testuser")
        assert user is not None
        assert user["username"] == "testuser"
        assert user["uid"] is None

        user_id = user["id"]
        delete_user(user_id)

    def test_update_user(self):
        # Add user
        assert add_user("testuser", uid=1)
        user = get_user(username="testuser")
        assert user is not None
        user_id = user["id"]
        # Update user
        assert update_user(user_id, username="updateduser")
        assert update_user(user_id, uid=10)
        updated_user = get_user(username="updateduser")
        assert updated_user is not None
        assert updated_user["id"] == user_id
        assert updated_user["username"] == "updateduser"
        assert updated_user["uid"] == 10
        # Remove user
        delete_user(user_id)

    def test_delete_user(self):
        # Add user
        assert add_user("testuser", uid=1)
        user = get_user(username="testuser")
        assert user is not None
        user_id = user["id"]
        # Delete user
        assert delete_user(user_id)
        try:
            get_user(username="updateduser")
        except Exception:
            assert True


class TestPasswordBackendTokenValidation:
    def test_validate_token_accepts_stored_unexpired_token(self):
        username, user, _, token = _password_token_user()
        try:
            assert PasswordBackend().validate_token(username, token) is True
        finally:
            user_auth.delete_user(username=username)

    def test_validate_token_rejects_expired_stored_token(self):
        username, _, _, token = _password_token_user(
            expired_at=datetime.now() - timedelta(hours=1)
        )
        try:
            assert PasswordBackend().validate_token(username, token) is False
        finally:
            user_auth.delete_user(username=username)

    def test_validate_token_rejects_presented_token_different_from_stored_token(self):
        username, _, _, _ = _password_token_user()
        try:
            tampered_token = password_backend._encrypt(username)
            assert PasswordBackend().validate_token(username, tampered_token) is False
        finally:
            user_auth.delete_user(username=username)

    def test_validate_token_rejects_token_for_deleted_user(self):
        username, _, _, _ = _password_token_user()
        assert user_auth.delete_user(username=username) is True

        assert PasswordBackend().validate_token(
            username, password_backend._encrypt(username)
        ) is False

    def test_validate_token_rejects_garbage_token(self):
        username, _, _, _ = _password_token_user()
        try:
            assert PasswordBackend().validate_token(username, "garbage") is False
        finally:
            user_auth.delete_user(username=username)


class TestPermission:
    def test_add_duplicate_permission_keeps_session_usable(self):
        permission_name = f"duplicate_permission_regression_{time.time_ns()}"
        assert permission_auth.add_permission(permission_name)

        try:
            assert permission_auth.add_permission(permission_name) is False
            permission = get_permission(permission=permission_name)
            assert permission["permission"] == permission_name
        finally:
            permission = get_permission(permission=permission_name)
            assert delete_permission(permission["id"])

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


def test_delete_user_cascade_db_error_returns_false(monkeypatch):
    import fastink.auth.common as common_mod
    from sqlalchemy.exc import DatabaseError

    username = f"delete_cascade_error_{time.time_ns()}"
    assert add_user(username)

    def boom(*args, **kwargs):
        raise DatabaseError("stmt", {}, Exception("boom"))

    try:
        monkeypatch.setattr(common_mod, "delete", boom)
        assert user_auth.delete_user(username=username) is False
    finally:
        monkeypatch.undo()
        existing_user = user_auth.get_user(username=username)
        if existing_user:
            delete_user(existing_user["id"])

def test_update_user_returns_true():
    username = f"update_return_{time.time_ns()}"
    assert add_user(username)
    try:
        assert user_auth.update_user(username=username, new_uid=9999) is True
    finally:
        delete_user(get_user(username=username)["id"])


def test_add_token_debug_log_never_contains_token(monkeypatch):
    import fastink.auth.common as common_mod

    logged = []

    class FakeLogger:
        def debug(self, *args):
            logged.append(args)

    monkeypatch.setattr(common_mod, "logger", FakeLogger())
    username = f"token_log_user_{time.time_ns()}"
    authentication_name = f"token_log_authn_{time.time_ns()}"
    assert add_user(username)
    assert add_authentication(authentication_name)
    user = get_user(username=username)
    authentication = get_authentication(authentication_name)
    secret = "never-in-logs-token-value"
    now = datetime.now()
    try:
        assert add_token(
            user_id=user["id"],
            authentication_id=authentication["id"],
            token=secret,
            generated_at=now,
            expired_at=now + timedelta(hours=1),
        )
        assert secret not in str(logged)
    finally:
        delete_user_cascade(user["id"])
        delete_authentication(authentication["id"])


def test_uid_zero_create_get_update():
    username = f"uid_zero_user_{time.time_ns()}"
    assert add_user(username, uid=0)
    try:
        user = get_user(username=username)
        assert user["uid"] == 0
        by_uid = get_user(uid=0)
        assert by_uid["username"] == username
        assert user_auth.update_user(username=username, new_uid=0) is True
    finally:
        delete_user_cascade(get_user(username=username)["id"])

