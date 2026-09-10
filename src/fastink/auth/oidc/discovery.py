from . import oidc_settings


def discovery_document() -> dict:
    issuer = str(oidc_settings().get("issuer", "")).rstrip("/")
    return {
        "issuer": issuer,
        "jwks_uri": f"{issuer}/jwks",
        "authorization_endpoint": f"{issuer}/authorize",
        "token_endpoint": f"{issuer}/token",
        "userinfo_endpoint": f"{issuer}/userinfo",
        "device_authorization_endpoint": f"{issuer}/device_authorization",
        "response_types_supported": ["code"],
        "response_modes_supported": ["query"],
        "grant_types_supported": [
            "authorization_code",
            "refresh_token",
            "urn:ietf:params:oauth:grant-type:device_code",
        ],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["RS256"],
        "token_endpoint_auth_methods_supported": ["none"],
        "code_challenge_methods_supported": ["S256"],
        "scopes_supported": ["openid", "profile", "email"],
    }
