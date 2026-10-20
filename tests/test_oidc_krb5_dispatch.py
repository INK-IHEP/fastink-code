"""Test that verify_login dispatches to the krb5 backend when auth.type=krb5."""
import pytest
from unittest.mock import patch


@pytest.fixture
def krb5_mode(monkeypatch):
    monkeypatch.setattr("fastink.auth.oidc.login.get_config", lambda *a, **kw: "krb5")


class TestKrb5Dispatch:
    def test_krb5_mode_calls_krb5_create_token(self, krb5_mode):
        from fastink.auth.oidc import login

        with patch.object(login, "_krb5_login") as mock_krb5:
            mock_krb5.return_value = True
            result = login.verify_login("alice", "secret")
            mock_krb5.assert_called_once_with("alice", "secret")
            assert result is True

    def test_krb5_mode_rejects_wrong_password(self, krb5_mode):
        from fastink.auth.oidc import login

        with patch.object(login, "_krb5_login") as mock_krb5:
            mock_krb5.return_value = False
            result = login.verify_login("alice", "wrong")
            assert result is False

    def test_password_mode_uses_shadow(self, monkeypatch):
        monkeypatch.setattr("fastink.auth.oidc.login.get_config", lambda *a, **kw: "password")
        from fastink.auth.oidc import login

        with patch.object(login, "_shadow_login") as mock_shadow:
            mock_shadow.return_value = True
            result = login.verify_login("alice", "secret")
            mock_shadow.assert_called_once_with("alice", "secret")
            assert result is True
