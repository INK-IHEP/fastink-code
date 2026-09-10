import json
from typing import Any
from urllib.request import urlopen

import jwt

from . import oidc_settings
from . import keys
from .principal import Principal


def _trusted_issuers(settings: dict[str, Any]) -> set[str]:
    configured = settings.get("trusted_issuers", settings.get("issuer"))
    if isinstance(configured, str):
        configured = [configured]
    if not isinstance(configured, (list, tuple, set)):
        return set()
    return {str(issuer).rstrip("/") for issuer in configured if issuer}


def jwks_for_issuer(issuer: str) -> dict[str, Any]:
    settings = oidc_settings()
    configured_issuer = settings.get("issuer")
    if isinstance(configured_issuer, str) and issuer == configured_issuer.rstrip("/"):
        return keys.jwks()

    jwks_uri = settings.get("jwks_uri") or f"{issuer.rstrip('/')}/jwks"
    with urlopen(jwks_uri, timeout=5) as response:
        return json.loads(response.read())


def _jwk_key(jwks: dict[str, Any], kid: str | None, algorithms: list[str]) -> Any:
    candidates = jwks.get("keys", [])
    if not isinstance(candidates, list):
        return None
    if kid:
        candidates = [candidate for candidate in candidates if candidate.get("kid") == kid]
    elif len(candidates) != 1:
        return None
    if len(candidates) != 1:
        return None
    jwk = candidates[0]
    if jwk.get("alg") and jwk["alg"] not in algorithms:
        return None
    return jwt.PyJWK(jwk).key


def _claim_values(claims: dict[str, Any], *names: str) -> list[str] | None:
    for name in names:
        value = claims.get(name)
        if value is None:
            continue
        if isinstance(value, str):
            return value.split()
        if isinstance(value, (list, tuple)) and all(isinstance(item, str) for item in value):
            return list(value)
        return None
    return None


def validate_bearer(token: str) -> Principal | None:
    try:
        settings = oidc_settings()
        trusted_issuers = _trusted_issuers(settings)
        audience = settings.get("audience")
        algorithms = settings.get("algorithms") or ["RS256"]
        if not trusted_issuers or not audience or not algorithms:
            return None

        header = jwt.get_unverified_header(token)
        unverified_claims = jwt.decode(token, options={"verify_signature": False})
        issuer = unverified_claims.get("iss")
        if not isinstance(issuer, str) or issuer.rstrip("/") not in trusted_issuers:
            return None
        if header.get("alg") not in algorithms:
            return None

        key = _jwk_key(jwks_for_issuer(issuer.rstrip("/")), header.get("kid"), algorithms)
        if key is None:
            return None
        claims = jwt.decode(
            token,
            key=key,
            algorithms=algorithms,
            audience=audience,
            issuer=issuer,
            leeway=settings.get("leeway", 0),
            options={"require": ["iss", "sub", "aud", "exp"]},
        )
        if claims.get("typ") != "at+jwt":
            return None
        subject = claims.get("sub")
        username = claims.get("preferred_username") or claims.get("username") or subject
        if not isinstance(subject, str) or not subject or not isinstance(username, str) or not username:
            return None
        return Principal(
            username=username,
            issuer=issuer,
            subject=subject,
            groups=_claim_values(claims, "groups"),
            scopes=_claim_values(claims, "scopes", "scope"),
        )
    except Exception:
        return None


__all__ = ["jwks_for_issuer", "validate_bearer"]
