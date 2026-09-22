import base64
import hashlib
import json
from urllib.parse import parse_qs, urlencode, urlparse

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import FastAPI
from fastapi.testclient import TestClient

from fastink.auth.oidc.router import router as oidc_router


ISSUER = "https://oidc.test.example"
AUDIENCE = "test-client"


@pytest.fixture
def client(monkeypatch):
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    signing_key = private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    settings = {
        "issuer": ISSUER,
        "audience": AUDIENCE,
        "algorithms": ["RS256"],
        "signing_key": signing_key,
        "jwks_ttl": 300,
        "leeway": 0,
        "allowed_redirect_uris": ["https://client.example/callback"],
        "clients": [
            {
                "client_id": AUDIENCE,
                "redirect_uris": ["https://client.example/callback"],
                "grants": ["authorization_code"],
            },
            {
                "client_id": "test-cli",
                "redirect_uris": [],
                "grants": ["urn:ietf:params:oauth:grant-type:device_code"],
            },
        ],
    }
    sso_settings = {
        "app_key": "app-key",
        "app_secret": "app-secret",
        "authorize_url": "https://sso.example/authorize",
        "token_url": "https://sso.example/token",
        "umt_api": "https://sso.example/umt",
        "redirect_uri": f"{ISSUER}/authorize/sso/callback",
    }

    from fastink.common import config

    def fake_get_config(section=None, option=None, fallback=None, **_kwargs):
        if section == "auth" and option == "oidc":
            return settings
        if section == "auth" and option == "mode":
            return "oidc"
        if section == "auth" and option == "sso":
            return sso_settings
        return fallback

    monkeypatch.setattr(config, "get_config", fake_get_config)
    app = FastAPI()
    app.include_router(oidc_router)
    return TestClient(app, base_url=ISSUER)


def _pkce_challenge(code_verifier):
    digest = hashlib.sha256(code_verifier.encode()).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode()


def _oidc_params(code_verifier="correct-verifier"):
    return {
        "client_id": AUDIENCE,
        "redirect_uri": "https://client.example/callback",
        "response_type": "code",
        "scope": "openid profile",
        "state": "state-123",
        "code_challenge": _pkce_challenge(code_verifier),
        "code_challenge_method": "S256",
    }


def _authorize(client, monkeypatch, code_verifier="correct-verifier"):
    params = _oidc_params(code_verifier)
    monkeypatch.setattr("fastink.auth.oidc.router.verify_login", lambda username, password: True)
    response = client.get("/authorize", params=params, follow_redirects=False)
    assert response.status_code == 200
    response = client.post(
        "/authorize",
        data={**params, "username": "alice", "password": "x"},
        follow_redirects=False,
    )
    assert response.status_code == 302
    query = parse_qs(urlparse(response.headers["location"]).query)
    return query["code"][0]


def test_authorize_serves_login_page(client):
    response = client.get("/authorize", params=_oidc_params())

    assert response.status_code == 200
    assert '<form method="post" action="/authorize">' in response.text
    assert 'name="username"' in response.text
    assert 'name="password"' in response.text
    assert 'name="redirect_uri" value="https://client.example/callback"' in response.text
    assert "/authorize/sso?" in response.text


def test_authorize_username_query_param_is_ignored(client):
    response = client.get("/authorize", params={**_oidc_params(), "username": "alice"})

    assert response.status_code == 200
    assert "code=" not in response.text


def test_authorize_still_rejects_bad_oidc_params_first(client):
    params = _oidc_params()
    del params["code_challenge"]
    response = client.get("/authorize", params=params)

    assert response.status_code == 400
    assert response.json()["error"] == "invalid_request"


def test_login_invalid_password_shows_generic_error(client, monkeypatch):
    monkeypatch.setattr("fastink.auth.oidc.router.verify_login", lambda username, password: False)
    response = client.post(
        "/authorize",
        data={**_oidc_params(), "username": "unknown", "password": "bad"},
        follow_redirects=False,
    )

    assert response.status_code == 200
    assert "Invalid username or password" in response.text
    assert "code=" not in response.text
    assert "Location" not in response.headers


def test_sso_login_redirects_to_upstream(client):
    response = client.get("/authorize/sso", params=_oidc_params(), follow_redirects=False)

    assert response.status_code == 302
    location = urlparse(response.headers["location"])
    query = parse_qs(location.query)
    assert location.scheme + "://" + location.netloc + location.path == "https://sso.example/authorize"
    assert query["client_id"] == ["app-key"]
    state = json.loads(base64.urlsafe_b64decode(query["state"][0] + "=="))
    assert {key: state[key] for key in _oidc_params()} == _oidc_params()
    assert state["pending_id"]


def test_sso_login_rejects_empty_upstream_settings(client, monkeypatch):
    from fastink.common import config

    def fake_get_config(section=None, option=None, fallback=None, **_kwargs):
        if section == "auth" and option == "oidc":
            return {"issuer": ISSUER, "audience": AUDIENCE, "allowed_redirect_uris": ["https://client.example/callback"]}
        if section == "auth" and option == "sso":
            return {"authorize_url": "https://sso.example/authorize", "app_key": "", "redirect_uri": ""}
        return fallback

    monkeypatch.setattr(config, "get_config", fake_get_config)
    response = client.get("/authorize/sso", params=_oidc_params(), follow_redirects=False)

    assert response.status_code == 400
    assert response.json()["error"] == "invalid_request"


def test_sso_login_sets_secure_pending_cookie(client):
    response = client.get("/authorize/sso", params=_oidc_params(), follow_redirects=False)

    assert "oidc_sso_pending=" in response.headers["set-cookie"]
    assert "Secure" in response.headers["set-cookie"]


def test_sso_callback_issues_code(client, monkeypatch):
    monkeypatch.setattr(
        "fastink.auth.oidc.router.complete_sso_code",
        lambda code: {"subject": "umt-issuer", "local_username": "afsaccount"},
    )
    start = client.get("/authorize/sso", params=_oidc_params(), follow_redirects=False)
    state = parse_qs(urlparse(start.headers["location"]).query)["state"][0]
    response = client.get(
        "/authorize/sso/callback",
        params={"code": "upstream-code", "state": state},
        follow_redirects=False,
    )

    assert response.status_code == 302
    query = parse_qs(urlparse(response.headers["location"]).query)
    assert query["code"]
    token = client.post(
        "/token",
        data={
            "grant_type": "authorization_code",
            "code": query["code"][0],
            "client_id": AUDIENCE,
            "redirect_uri": "https://client.example/callback",
            "code_verifier": "correct-verifier",
        },
    ).json()
    claims = jwt.decode(token["access_token"], options={"verify_signature": False})
    assert claims["preferred_username"] == "afsaccount"


def test_sso_callback_requires_pending_cookie(client, monkeypatch):
    monkeypatch.setattr("fastink.auth.oidc.router.complete_sso_code", lambda code: {"subject": "s", "local_username": "alice"})
    state = base64.urlsafe_b64encode(json.dumps({"pending_id": "unknown"}).encode()).decode().rstrip("=")
    response = client.get("/authorize/sso/callback", params={"code": "code", "state": state}, follow_redirects=False)
    assert response.status_code == 400
    assert response.json()["error"] == "invalid_request"


def test_discovery_document(client):
    response = client.get("/.well-known/openid-configuration")

    assert response.status_code == 200
    document = response.json()
    assert document["issuer"] == ISSUER
    assert document["jwks_uri"] == f"{ISSUER}/jwks"
    assert document["authorization_endpoint"] == f"{ISSUER}/authorize"
    assert document["token_endpoint"] == f"{ISSUER}/token"
    assert document["userinfo_endpoint"] == f"{ISSUER}/userinfo"
    assert document["device_authorization_endpoint"] == f"{ISSUER}/device_authorization"
    assert "authorization_code" in document["grant_types_supported"]
    assert "refresh_token" in document["grant_types_supported"]
    assert "urn:ietf:params:oauth:grant-type:device_code" in document["grant_types_supported"]


def test_jwks_rsa(client):
    response = client.get("/jwks")

    assert response.status_code == 200
    key = response.json()["keys"][0]
    assert key["kty"] == "RSA"
    assert key["alg"] == "RS256"
    assert key["kid"]


def test_authorize_requires_pkce_s256(client):
    params = {
        "client_id": AUDIENCE,
        "redirect_uri": "https://client.example/callback",
        "response_type": "code",
        "scope": "openid",
    }
    assert client.get("/authorize", params=params).status_code == 400

    params.update(
        code_challenge=_pkce_challenge("verifier"),
        code_challenge_method="S256",
    )
    response = client.get("/authorize", params=params, follow_redirects=False)
    assert response.status_code == 200


def test_authorize_rejects_unregistered_redirect_uri(client):
    params = {
        "client_id": AUDIENCE,
        "redirect_uri": "https://evil.example/callback",
        "response_type": "code",
        "scope": "openid",
        "code_challenge": _pkce_challenge("verifier"),
        "code_challenge_method": "S256",
    }
    response = client.get("/authorize", params=params)

    assert response.status_code == 400
    assert response.json()["error"] == "invalid_request"


def test_token_exchange_pkce_roundtrip(client, monkeypatch):
    verifier = "roundtrip-verifier"
    code = _authorize(client, monkeypatch, verifier)
    response = client.post(
        "/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "client_id": AUDIENCE,
            "redirect_uri": "https://client.example/callback",
            "code_verifier": verifier,
        },
    )

    assert response.status_code == 200
    tokens = response.json()
    assert tokens["access_token"]
    assert tokens["id_token"]
    jwk = client.get("/jwks").json()["keys"][0]
    public_key = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(jwk))
    claims = jwt.decode(
        tokens["access_token"],
        public_key,
        algorithms=["RS256"],
        audience=AUDIENCE,
        issuer=ISSUER,
    )
    assert claims["iss"] == ISSUER
    assert claims["aud"] == AUDIENCE
    assert claims["sub"] == "alice"
    assert claims["preferred_username"] == "alice"
    assert claims["exp"] > claims["iat"]
    assert claims["typ"] == "at+jwt"


def test_token_rejects_wrong_verifier(client, monkeypatch):
    code = _authorize(client, monkeypatch, "correct-verifier")
    response = client.post(
        "/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "client_id": AUDIENCE,
            "redirect_uri": "https://client.example/callback",
            "code_verifier": "wrong-verifier",
        },
    )
    assert response.status_code == 400
    assert response.json()["error"] == "invalid_grant"


def test_failed_exchange_does_not_burn_code(client, monkeypatch):
    code = _authorize(client, monkeypatch, "correct-verifier")
    bad = client.post(
        "/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "client_id": AUDIENCE,
            "redirect_uri": "https://client.example/callback",
            "code_verifier": "wrong-verifier",
        },
    )
    assert bad.status_code == 400
    good = client.post(
        "/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "client_id": AUDIENCE,
            "redirect_uri": "https://client.example/callback",
            "code_verifier": "correct-verifier",
        },
    )
    assert good.status_code == 200
    assert good.json()["access_token"]


def test_device_poll_before_approval_does_not_burn_code(client, monkeypatch):
    created = client.post("/device_authorization", data={"client_id": "test-cli"}).json()
    pending = client.post("/token", data={
        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
        "device_code": created["device_code"],
        "client_id": "test-cli",
    })
    assert pending.status_code == 400
    assert pending.json()["error"] == "authorization_pending"
    monkeypatch.setattr("fastink.auth.oidc.router.verify_login", lambda u, p: True)
    client.post("/device", data={"user_code": created["user_code"], "username": "alice", "password": "x"})
    token = client.post("/token", data={
        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
        "device_code": created["device_code"],
        "client_id": "test-cli",
    })
    assert token.status_code == 200
    assert token.json()["access_token"]


def test_device_approval_requires_authenticated_login(client, monkeypatch):
    created = client.post("/device_authorization", data={"client_id": "test-cli"}).json()
    assert client.post("/device/approve", data={"user_code": created["user_code"], "username": "alice"}).status_code == 405
    assert client.get("/device", params={"user_code": created["user_code"]}).status_code == 200
    monkeypatch.setattr("fastink.auth.oidc.router.verify_login", lambda username, password: True)
    response = client.post("/device", data={"user_code": created["user_code"], "username": "alice", "password": "x"})
    assert response.status_code == 200
    token = client.post("/token", data={"grant_type": "urn:ietf:params:oauth:grant-type:device_code", "device_code": created["device_code"], "client_id": "test-cli"}).json()
    claims = jwt.decode(token["access_token"], options={"verify_signature": False})
    assert claims["preferred_username"] == "alice"


def test_token_response_is_not_cacheable(client, monkeypatch):
    code = _authorize(client, monkeypatch)
    response = client.post("/token", data={"grant_type": "authorization_code", "code": code, "client_id": AUDIENCE, "redirect_uri": "https://client.example/callback", "code_verifier": "correct-verifier"})
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["pragma"] == "no-cache"


def test_nonce_is_only_emitted_in_id_token(client, monkeypatch):
    params = {**_oidc_params(), "nonce": "nonce-123"}
    monkeypatch.setattr("fastink.auth.oidc.router.verify_login", lambda username, password: True)
    response = client.post("/authorize", data={**params, "username": "alice", "password": "x"}, follow_redirects=False)
    code = parse_qs(urlparse(response.headers["location"]).query)["code"][0]
    tokens = client.post("/token", data={"grant_type": "authorization_code", "code": code, "client_id": AUDIENCE, "redirect_uri": params["redirect_uri"], "code_verifier": "correct-verifier"}).json()
    assert jwt.decode(tokens["id_token"], options={"verify_signature": False})["nonce"] == "nonce-123"
    assert "nonce" not in jwt.decode(tokens["access_token"], options={"verify_signature": False})


def test_authorize_calls_ensure_credential(client, monkeypatch):
    calls = []
    monkeypatch.setattr(
        "fastink.auth.oidc.flows.get_krb5",
        lambda username: calls.append(username),
    )
    _authorize(client, monkeypatch)
    assert calls == ["alice"]


def test_authorize_credential_failure_does_not_block_login(client, monkeypatch):
    def fail(username):
        raise RuntimeError("torch unavailable")
    monkeypatch.setattr("fastink.auth.oidc.flows.get_krb5", fail)
    code = _authorize(client, monkeypatch)
    assert code  # login succeeded despite credential failure


def test_client_registration_rejects_unknown_client(client, monkeypatch):
    params = _oidc_params()
    params["client_id"] = "unknown-client"
    response = client.get("/authorize", params=params)
    assert response.status_code == 400
    assert response.json()["error"] == "unauthorized_client"


def test_device_code_rejected_for_web_only_client(client, monkeypatch):
    response = client.post(
        "/device_authorization",
        data={"client_id": AUDIENCE, "scope": "openid"},
    )
    assert response.status_code == 400
    assert "unauthorized" in response.json()["error"]


def test_client_empty_redirect_uris_falls_back_to_top_level(client, monkeypatch):
    from fastink.auth.oidc import flows
    settings = flows.oidc_settings()
    settings["clients"] = [
        {"client_id": AUDIENCE, "redirect_uris": [], "grants": ["authorization_code"]},
    ]
    monkeypatch.setattr(flows, "oidc_settings", lambda: settings)
    flows.validate_authorization_request(
        client_id=AUDIENCE,
        redirect_uri="https://client.example/callback",
        response_type="code",
        code_challenge="x",
        code_challenge_method="S256",
    )
