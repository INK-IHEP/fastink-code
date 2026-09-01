import base64
import hashlib
from functools import lru_cache

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

from . import oidc_settings


def _base64url(value: int) -> str:
    size = (value.bit_length() + 7) // 8
    return base64.urlsafe_b64encode(value.to_bytes(size, "big")).rstrip(b"=").decode()


@lru_cache(maxsize=8)
def _load_private_key(signing_key: str) -> RSAPrivateKey:
    if not signing_key:
        raise RuntimeError("OIDC not configured: auth.oidc.signing_key is empty")
    try:
        key = serialization.load_pem_private_key(signing_key.encode(), password=None)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("OIDC not configured: invalid RSA signing key") from exc
    if not isinstance(key, RSAPrivateKey):
        raise RuntimeError("OIDC not configured: signing key must be RSA")
    return key


def private_key() -> RSAPrivateKey:
    return _load_private_key(oidc_settings().get("signing_key", ""))


def public_jwk() -> dict[str, str]:
    public_numbers = private_key().public_key().public_numbers()
    key_id = hashlib.sha256(
        private_key().public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    ).hexdigest()[:16]
    return {
        "kty": "RSA",
        "use": "sig",
        "alg": "RS256",
        "kid": key_id,
        "n": _base64url(public_numbers.n),
        "e": _base64url(public_numbers.e),
    }


def jwks() -> dict[str, list[dict[str, str]]]:
    return {"keys": [public_jwk()]}
