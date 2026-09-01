"""Unit tests for the auth backend protocol + registry."""

import pytest

from fastink.auth.backends.base import AuthBackend
from fastink.auth.backends import registry


class TestRegistry:
    def test_builtin_backends_discovered(self):
        registry.discover()
        assert "krb5" in registry.names()
        assert "password" in registry.names()

    def test_get_krb5_backend(self):
        registry.discover()
        backend = registry.get_auth_backend("krb5")
        assert backend.name == "krb5"
        assert isinstance(backend, AuthBackend)  # protocol shape

    def test_get_password_backend(self):
        registry.discover()
        backend = registry.get_auth_backend("password")
        assert backend.name == "password"
        assert isinstance(backend, AuthBackend)

    def test_unknown_backend_raises_with_hint(self):
        registry.discover()
        with pytest.raises(LookupError) as exc:
            registry.get_auth_backend("no-such-backend")
        msg = str(exc.value)
        assert "no-such-backend" in msg
        assert "krb5" in msg  # lists available

    def test_register_name_mismatch_rejected(self):
        with pytest.raises(ValueError):
            @registry.register_backend("declared_name")
            class Mismatch:
                name = "actual_name"
                def create_token(self, username, password=None): ...
                def get_token(self, username): ...
                def validate_token(self, username, token, **kw): ...

    def test_register_incomplete_backend_rejected(self):
        with pytest.raises(TypeError):
            @registry.register_backend("incomplete")
            class Incomplete:
                name = "incomplete"
                # missing get_token / validate_token

    def test_override_warns(self, caplog):
        import logging

        @registry.register_backend("unit_test_override")
        class First:
            name = "unit_test_override"
            def create_token(self, username, password=None): ...
            def get_token(self, username): ...
            def validate_token(self, username, token, **kw): ...

        with caplog.at_level(logging.WARNING, logger="ink"):
            @registry.register_backend("unit_test_override")
            class Second:
                name = "unit_test_override"
                def create_token(self, username, password=None): ...
                def get_token(self, username): ...
                def validate_token(self, username, token, **kw): ...

        assert any("Auth backend override" in r.message for r in caplog.records)
        registry._BACKENDS.pop("unit_test_override", None)


class TestPasswordBackendValidate:
    def test_validate_token_roundtrip(self, monkeypatch):
        """valid stored token returns True; unknown user returns False."""
        from datetime import datetime, timedelta

        from fastink.auth import common
        from fastink.auth.backends import password as pw

        monkeypatch.setattr(pw, "_encrypt", lambda s: f"enc:{s}")
        monkeypatch.setattr(
            pw, "_decrypt",
            lambda t: t[4:] if t.startswith("enc:") else (_ for _ in ()).throw(ValueError("bad")),
        )
        username = f"validate_roundtrip_{datetime.now().timestamp()}"
        authentication = f"password_{datetime.now().timestamp()}"
        common.add_user(username=username)
        common.add_authentication(authentication=authentication)
        user_row = common.get_user(username=username)
        auth_row = common.get_authentication(authentication)
        token = pw._encrypt(username)
        common.add_token(
            user_id=user_row["id"],
            authentication_id=auth_row["id"],
            token=token,
            generated_at=datetime.now(),
            expired_at=datetime.now() + timedelta(hours=1),
        )
        try:
            backend = pw.PasswordBackend()
            assert backend.validate_token(username, token) is True
            assert backend.validate_token("definitely_missing_user", token) is False
        finally:
            common.delete_user_cascade(user_row["id"])
            common.delete_authentication(auth_row["id"])

    def test_validate_token_bad_token_is_false(self, monkeypatch):
        from fastink.auth.backends import password as pw

        def boom(_):
            raise ValueError("corrupt")
        monkeypatch.setattr(pw, "_decrypt", boom)
        assert pw.PasswordBackend().validate_token("alice", "garbage") is False
