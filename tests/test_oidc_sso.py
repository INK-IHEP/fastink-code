import json
from uuid import uuid4

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from sqlalchemy import delete

from fastink.auth import common
from fastink.auth.external_identity import map_external_identity, resolve_external_identity
from fastink.auth.oidc.flows import complete_sso_login
from fastink.auth.oidc.keys import public_jwk
from fastink.auth.oidc.sso import exchange_sso_code
from fastink.database.sqla import models
from fastink.database.sqla.session import get_session


TOKEN_URL = "https://newlogin.ihep.ac.cn/oauth2/token"
UMT_API = "https://login.ihep.ac.cn/umt/api/APIComputing"


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload


class FakeClient:
    def __init__(self, *args, **kwargs):
        self.requests = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def post(self, url, **kwargs):
        self.requests.append(("POST", url, kwargs))
        return FakeResponse(
            {"userInfo": {"cstnetId": "alice@example.com", "truename": "Alice", "umtId": "umt-123"}}
        )

    def get(self, url, **kwargs):
        self.requests.append(("GET", url, kwargs))
        return FakeResponse({"result": [{"afsaccount": "alice", "uid": 123}]})


@pytest.fixture
def sso_config(monkeypatch):
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    signing_key = private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    settings = {
        "issuer": "https://oidc.test.example",
        "audience": "test-client",
        "algorithms": ["RS256"],
        "signing_key": signing_key,
        "jwks_ttl": 300,
        "leeway": 0,
    }
    sso = {
        "app_key": "app-key",
        "app_secret": "app-secret",
        "token_url": TOKEN_URL,
        "umt_api": UMT_API,
        "redirect_uri": "https://fastink.test/sso/callback",
    }

    def fake_get_config(section=None, option=None, fallback=None, **_kwargs):
        if section == "auth" and option == "oidc":
            return settings
        if section == "auth" and option == "sso":
            return sso
        if section == "auth" and option == "mode":
            return "oidc"
        return fallback

    monkeypatch.setattr("fastink.common.config.get_config", fake_get_config)
    monkeypatch.setattr("fastink.auth.oidc.sso.httpx.Client", FakeClient)
    return sso


def _cleanup(issuer, subject, user_id=None):
    scoped = get_session()
    session = scoped()
    try:
        session.begin()
        session.execute(
            delete(models.ExternalIdentities).where(
                models.ExternalIdentities.issuer == issuer,
                models.ExternalIdentities.subject == subject,
            )
        )
        if user_id is not None:
            session.execute(
                delete(models.UserPermissions).where(models.UserPermissions.user_id == user_id)
            )
            session.execute(delete(models.Users).where(models.Users.id == user_id))
        session.commit()
    finally:
        scoped.remove()


def test_exchange_sso_code_returns_identity(sso_config):
    identity = exchange_sso_code("upstream-code")

    assert identity == {
        "subject": "umt-123",
        "local_username": "alice",
        "email": "alice@example.com",
        "uid": 123,
    }


def test_exchange_sso_code_error_description_raises(sso_config, monkeypatch):
    class ErrorClient(FakeClient):
        def post(self, url, **kwargs):
            return FakeResponse({"error_description": "invalid authorization code"})

    monkeypatch.setattr("fastink.auth.oidc.sso.httpx.Client", ErrorClient)

    with pytest.raises(ValueError, match="SSO authentication failed"):
        exchange_sso_code("bad-code")


def test_exchange_sso_code_handles_upstream_request_failure(sso_config, monkeypatch):
    class FailingClient(FakeClient):
        def post(self, url, **kwargs):
            raise httpx.ConnectError("upstream unavailable")

    monkeypatch.setattr("fastink.auth.oidc.sso.httpx.Client", FailingClient)

    with pytest.raises(ValueError, match="SSO authentication failed"):
        exchange_sso_code("code")


def test_exchange_sso_code_handles_malformed_upstream_response(sso_config, monkeypatch):
    class MalformedResponse:
        def json(self):
            raise json.JSONDecodeError("invalid", "not-json", 0)

    class MalformedClient(FakeClient):
        def post(self, url, **kwargs):
            return MalformedResponse()

    monkeypatch.setattr("fastink.auth.oidc.sso.httpx.Client", MalformedClient)

    with pytest.raises(ValueError, match="SSO authentication failed"):
        exchange_sso_code("code")


def test_exchange_sso_code_rejects_non_dict_token_response(sso_config, monkeypatch):
    class ListClient(FakeClient):
        def post(self, url, **kwargs):
            return FakeResponse([])

    monkeypatch.setattr("fastink.auth.oidc.sso.httpx.Client", ListClient)

    with pytest.raises(ValueError, match="SSO authentication failed"):
        exchange_sso_code("code")


def test_exchange_sso_code_rejects_none_identity_fields(sso_config, monkeypatch):
    class NoneIdentityClient(FakeClient):
        def post(self, url, **kwargs):
            return FakeResponse({"userInfo": {"cstnetId": None, "umtId": None}})

    monkeypatch.setattr("fastink.auth.oidc.sso.httpx.Client", NoneIdentityClient)

    with pytest.raises(ValueError, match="SSO authentication failed"):
        exchange_sso_code("code")


def test_exchange_sso_code_rejects_non_dict_umt_result_item(sso_config, monkeypatch):
    class NonDictItemClient(FakeClient):
        def get(self, url, **kwargs):
            return FakeResponse({"result": ["not-a-dict"]})

    monkeypatch.setattr("fastink.auth.oidc.sso.httpx.Client", NonDictItemClient)

    with pytest.raises(ValueError, match="SSO authentication failed"):
        exchange_sso_code("code")


def test_federate_maps_existing_user(sso_config, monkeypatch):
    issuer = "https://newlogin.ihep.ac.cn"
    username = f"sso_existing_{uuid4().hex}"
    user_id = None
    try:
        class ExistingUserClient(FakeClient):
            def post(self, url, **kwargs):
                return FakeResponse({"userInfo": {"cstnetId": "existing@example.com", "umtId": "umt-123"}})

            def get(self, url, **kwargs):
                return FakeResponse({"result": [{"afsaccount": username}]})

        monkeypatch.setattr("fastink.auth.oidc.sso.httpx.Client", ExistingUserClient)
        common.add_user(username)
        user_id = common.get_user(username=username)["id"]
        map_external_identity(issuer, "umt-123", user_id)

        token_response = complete_sso_login("upstream-code")
        claims = jwt.decode(
            token_response["access_token"],
            key=jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(public_jwk())),
            algorithms=["RS256"],
            audience="test-client",
        )

        assert claims["preferred_username"] == username
        assert claims["sub"] == "umt-123"
    finally:
        _cleanup(issuer, "umt-123", user_id)


def test_federate_creates_and_links_new_user(sso_config, monkeypatch):
    class NewUserClient(FakeClient):
        def post(self, url, **kwargs):
            return FakeResponse({"userInfo": {"cstnetId": "new@example.com", "umtId": "umt-456"}})

        def get(self, url, **kwargs):
            return FakeResponse({"result": [{"afsaccount": "newuser", "uid": 456}]})

    monkeypatch.setattr("fastink.auth.oidc.sso.httpx.Client", NewUserClient)
    issuer = "https://newlogin.ihep.ac.cn"
    user_id = None
    try:
        token_response = complete_sso_login("new-code")
        user = common.get_user(username="newuser")
        user_id = user["id"]

        assert token_response["access_token"]
        assert common.get_user(username="newuser")["id"] == user_id
        assert resolve_external_identity(issuer, "umt-456").id == user_id
    finally:
        _cleanup(issuer, "umt-456", user_id)


def test_federate_fails_closed_without_sso_config(sso_config, monkeypatch):
    monkeypatch.setattr(
        "fastink.common.config.get_config",
        lambda section=None, option=None, fallback=None, **_: (
            {"app_key": "", "app_secret": ""}
            if section == "auth" and option == "sso"
            else fallback
        ),
    )

    with pytest.raises(ValueError, match="SSO authentication failed"):
        exchange_sso_code("code")
