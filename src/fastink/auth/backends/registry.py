"""Registry for FastINK auth backends.

Selects the authentication backend by ``auth.type``. Built-in backends
(password, krb5) register when this package is imported; plugin backends
(e.g. IHEP apikey/hai) register from the plugin's ``initialize()`` via
``unified_plugins.packages`` — the same registration model as computing
apps and site strategies.

Conflict semantics match the rest of the plugin system: registration
order = load order (built-ins first, then plugins in config order), the
LAST registration wins, and every override is logged as a warning.
"""

from __future__ import annotations

from typing import Dict, Type

from fastink.common.logger import logger
from fastink.auth.backends.base import AuthBackend

_BACKENDS: Dict[str, AuthBackend] = {}
_DISCOVERED: bool = False


def register_backend(name: str):
    """Class decorator: instantiate and register an auth backend.

    Usage::

        @register_backend("krb5")
        class Krb5Backend:
            name = "krb5"
            def create_token(self, username, password=None): ...
            def get_token(self, username): ...
            def validate_token(self, username, token, **_): ...

    The class need not inherit AuthBackend; it only has to satisfy the
    protocol shape (checked here via ``isinstance`` against the
    ``@runtime_checkable`` Protocol).
    """

    def deco(cls: Type) -> Type:
        instance = cls()
        if getattr(instance, "name", None) != name:
            # Keep the decorator arg and the .name attribute in sync so
            # lookups and DB record names cannot drift apart.
            raise ValueError(
                f"Auth backend {cls.__module__}.{cls.__name__} registers as "
                f"{name!r} but its .name is {getattr(instance, 'name', None)!r}"
            )
        if not isinstance(instance, AuthBackend):
            raise TypeError(
                f"Auth backend {cls.__module__}.{cls.__name__} does not satisfy "
                "the AuthBackend protocol (needs create_token/get_token/validate_token)"
            )
        existing = _BACKENDS.get(name)
        if existing is not None and type(existing) is not cls:
            logger.warning(
                "Auth backend override: %r previously registered by %s.%s, "
                "now being replaced by %s.%s (last registration wins)",
                name,
                type(existing).__module__, type(existing).__name__,
                cls.__module__, cls.__name__,
            )
        _BACKENDS[name] = instance
        logger.info("Registered auth backend: %s -> %s.%s",
                    name, cls.__module__, cls.__name__)
        return cls

    return deco


def discover(force: bool = False) -> None:
    """Import built-in backend modules so they self-register. Idempotent."""
    global _DISCOVERED
    if _DISCOVERED and not force:
        return
    # Importing the modules triggers their @register_backend decorators.
    from fastink.auth.backends import krb5 as _krb5  # noqa: F401
    from fastink.auth.backends import password as _password  # noqa: F401
    _DISCOVERED = True


def get_auth_backend(name: str = None) -> AuthBackend:
    """Return the backend for ``name`` (defaults to ``auth.type``)."""
    if not _DISCOVERED:
        discover()
    if name is None:
        from fastink.common.config import get_config
        name = get_config("auth", "type")
    try:
        return _BACKENDS[name]
    except KeyError:
        raise LookupError(
            f"Auth backend {name!r} is not registered. "
            f"Available: {sorted(_BACKENDS)}. Backends register on import; "
            "site backends are provided by plugin packages "
            "(unified_plugins.packages) — check the plugin is installed and loaded."
        )


def names() -> list:
    if not _DISCOVERED:
        discover()
    return sorted(_BACKENDS)
