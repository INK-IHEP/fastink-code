from dataclasses import dataclass

from fastink.auth.principal import Principal as _Principal
from fastink.auth.principal import get_current_principal


@dataclass
class Principal(_Principal):
    """v2 compatibility shim: keeps the historical auth_method default."""

    auth_method: str = "oidc"


__all__ = ["Principal", "get_current_principal"]
