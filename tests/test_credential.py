import base64
import subprocess
from datetime import datetime, timedelta, timezone

import pytest

from fastink.auth import credential


def _make_ca_key(tmp_path):
    ca_key = tmp_path / "ca"
    subprocess.run(
        ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(ca_key)],
        check=True,
    )
    return ca_key.read_text()


def _config(krb5_enabled, ca_private_key=""):
    def fake_get_config(section, option, fallback=None, *, type=None):
        if (section, option) == ("common", "krb5_enabled"):
            return krb5_enabled
        if (section, option) == ("auth", "delegation"):
            return {"ca_private_key": ca_private_key}
        return fallback

    return fake_get_config


def test_krb5_provider_wraps_get_krb5(monkeypatch):
    calls = []
    token = base64.b64encode(b"fake ccache").decode()
    monkeypatch.setattr(credential, "get_config", _config(True))

    def fake_get_krb5(username):
        calls.append(username)
        return token

    monkeypatch.setattr(credential, "get_krb5", fake_get_krb5)

    result = credential.get_cluster_credential("alice")

    assert result == credential.ClusterCredential(
        kind="krb5", username="alice", ccache_token=token
    )
    assert calls == ["alice"]


def test_ssh_ca_provider_signs_short_lived_cert(monkeypatch, tmp_path):
    monkeypatch.setattr(credential, "get_config", _config(False, _make_ca_key(tmp_path)))

    before = datetime.now(timezone.utc)
    result = credential.get_cluster_credential("bob")
    after = datetime.now(timezone.utc)

    assert result.kind == "ssh-cert"
    assert result.username == "bob"
    assert result.certificate is not None
    assert result.private_key is not None
    assert result.expires_at is not None
    assert before + timedelta(minutes=4) < result.expires_at < after + timedelta(minutes=6)

    cert_path = tmp_path / "bob-cert.pub"
    cert_path.write_text(result.certificate)
    details = subprocess.run(
        ["ssh-keygen", "-L", "-f", str(cert_path)],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    assert "bob" in details


def test_ssh_ca_fails_closed_without_key(monkeypatch):
    monkeypatch.setattr(credential, "get_config", _config(False))

    with pytest.raises(ValueError, match="not configured"):
        credential.get_cluster_credential("bob")


def test_dispatch_selects_provider(monkeypatch):
    token = base64.b64encode(b"fake ccache").decode()
    monkeypatch.setattr(credential, "get_krb5", lambda username: token)
    monkeypatch.setattr(credential, "_sign_ssh_certificate", lambda username, ca_key: ("cert", "key", datetime.now(timezone.utc) + timedelta(minutes=5)))

    monkeypatch.setattr(credential, "get_config", _config(True))
    assert credential.get_cluster_credential("alice").kind == "krb5"

    monkeypatch.setattr(credential, "get_config", _config(False, "configured"))
    assert credential.get_cluster_credential("alice").kind == "ssh-cert"


def test_export_credential_returns_username_ccache_expiry(monkeypatch):
    from fastink.auth.oidc.principal import Principal
    token = base64.b64encode(b"ccache").decode()
    monkeypatch.setattr(
        credential,
        "get_cluster_credential",
        lambda username: credential.ClusterCredential(
            kind="krb5", username=username, ccache_token=token
        ),
    )
    monkeypatch.setattr(credential, "_ccache_expiry", lambda t: "2026-09-03T00:00:00")

    principal = Principal(username="alice", issuer="legacy", subject="alice", auth_method="legacy")
    result = credential.export_credential(principal)

    assert result["username"] == "alice"
    assert result["kind"] == "krb5"
    assert result["ccache"] == token
    assert result["expires_at"] == "2026-09-03T00:00:00"


def test_export_credential_uses_principal_username(monkeypatch):
    from fastink.auth.oidc.principal import Principal
    calls = []
    monkeypatch.setattr(
        credential,
        "get_cluster_credential",
        lambda username: calls.append(username) or credential.ClusterCredential(
            kind="krb5", username=username, ccache_token="x"
        ),
    )
    monkeypatch.setattr(credential, "_ccache_expiry", lambda t: "")

    principal = Principal(username="bob", issuer="oidc", subject="s1", auth_method="oidc")
    credential.export_credential(principal)
    assert calls == ["bob"]


def test_export_credential_ssh_cert_kind(monkeypatch):
    from datetime import datetime, timezone
    from fastink.auth.oidc.principal import Principal
    expiry = datetime(2026, 9, 3, tzinfo=timezone.utc)
    monkeypatch.setattr(
        credential,
        "get_cluster_credential",
        lambda username: credential.ClusterCredential(
            kind="ssh-cert", username=username,
            certificate="cert-data", private_key="key-data", expires_at=expiry,
        ),
    )

    principal = Principal(username="alice", issuer="legacy", subject="alice", auth_method="legacy")
    result = credential.export_credential(principal)

    assert result["kind"] == "ssh-cert"
    assert result["certificate"] == "cert-data"
    assert result["private_key"] == "key-data"
    assert result["expires_at"] == expiry.isoformat()
    assert "ccache" not in result
