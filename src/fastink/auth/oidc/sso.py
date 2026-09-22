import json
from typing import Any
from urllib.parse import urlsplit

import httpx

from fastink.auth import user
from fastink.auth.external_identity import map_external_identity, resolve_external_identity
from fastink.common import config
from fastink.common.logger import logger

from .flows import _jwt_claims, _sign


def _sso_settings() -> dict[str, Any]:
    return config.get_config("auth", "sso", fallback={}) or {}


def _issuer(token_url: str) -> str:
    parts = urlsplit(token_url)
    return f"{parts.scheme}://{parts.netloc}"


def _authentication_failed(reason: str) -> None:
    logger.error("SSO authentication failed: %s", reason)
    raise ValueError("SSO authentication failed")


def exchange_sso_code(code: str) -> dict[str, Any]:
    settings = _sso_settings()
    app_key = str(settings.get("app_key", ""))
    app_secret = str(settings.get("app_secret", ""))
    token_url = str(settings.get("token_url", ""))
    umt_api = str(settings.get("umt_api", ""))
    redirect_uri = str(settings.get("redirect_uri", ""))
    if not app_key or not app_secret:
        _authentication_failed("missing app_key or app_secret")
    if not token_url or not umt_api or not redirect_uri:
        _authentication_failed("missing token_url, umt_api, or redirect_uri")

    try:
        with httpx.Client() as client:
            token_response = client.post(
                token_url,
                data={
                    "client_id": app_key,
                    "client_secret": app_secret,
                    "grant_type": "authorization_code",
                    "redirect_uri": redirect_uri,
                    "code": code,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            ).json()
            if not isinstance(token_response, dict):
                _authentication_failed("malformed upstream response")
            if token_response.get("error_description"):
                _authentication_failed(str(token_response["error_description"]))
            user_info = token_response.get("userInfo")
            if not isinstance(user_info, dict):
                _authentication_failed("missing userInfo")
            email = str(user_info.get("cstnetId") or "")
            subject = str(user_info.get("umtId") or "")
            if not email or not subject:
                _authentication_failed("missing cstnetId or umtId")
            umt_response = client.get(umt_api, params={"email": email}).json()
    except httpx.HTTPError:
        _authentication_failed("upstream request failed")
    except json.JSONDecodeError:
        _authentication_failed("malformed upstream response")

    if not isinstance(umt_response, dict):
        _authentication_failed("malformed upstream response")
    result = umt_response.get("result")
    if not isinstance(result, list) or not result:
        _authentication_failed("empty UMT result")
    if not isinstance(result[0], dict):
        _authentication_failed("malformed upstream response")
    local_username = str(result[0].get("afsaccount") or "")
    if not local_username:
        _authentication_failed("missing afsaccount")
    uid = result[0].get("uid")
    return {"subject": subject, "local_username": local_username, "email": email, "uid": uid}


def complete_sso_code(code: str) -> dict[str, Any]:
    return _map_sso_identity(exchange_sso_code(code))


def _map_sso_identity(identity: dict[str, Any]) -> dict[str, Any]:
    settings = _sso_settings()
    issuer = str(settings.get("issuer", "")) or _issuer(str(settings.get("token_url", "")))
    if not issuer:
        _authentication_failed("missing issuer")
    local_user = resolve_external_identity(issuer, identity["subject"])
    if local_user is None:
        user.add_user(
            username=identity["local_username"],
            uid=identity.get("uid"),
            external_verified=True,
        )
        local_user = user.get_user(username=identity["local_username"])
        map_external_identity(issuer, identity["subject"], local_user["id"])
    identity["local_username"] = local_user.username if hasattr(local_user, "username") else identity["local_username"]
    return identity


def federate_sso_identity(identity: dict[str, str]) -> dict[str, Any]:
    completed = _map_sso_identity(identity)
    preferred_username = completed["local_username"]
    try:
        from fastink.auth.backends.krb5 import get_krb5
        get_krb5(username=preferred_username)
    except Exception as e:
        logger.warning(
            "Post-SSO credential ensure failed for %s: %s",
            preferred_username, e,
        )
    claims = _jwt_claims(preferred_username, "at+jwt")
    claims["sub"] = completed["subject"]
    return {
        "access_token": _sign(claims),
        "token_type": "Bearer",
        "expires_in": 3600,
    }


def complete_sso_login(code: str) -> dict[str, Any]:
    return federate_sso_identity(exchange_sso_code(code))
