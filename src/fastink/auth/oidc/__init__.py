from fastink.common import config


def get_auth_mode() -> str:
    return config.get_config("auth", "mode", fallback="legacy")


def should_mount_oidc() -> bool:
    """The OIDC issuer is mounted only when the deployment opted into it."""
    return get_auth_mode() in ("dual", "oidc")


def oidc_settings() -> dict:
    return config.get_config("auth", "oidc", fallback={}) or {}


__all__ = ["get_auth_mode", "should_mount_oidc", "oidc_settings"]
