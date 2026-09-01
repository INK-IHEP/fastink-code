"""Auth backend protocol for FastINK.

An "auth backend" bundles the full lifecycle of one authentication mode
(password, krb5, apikey, ...) behind a single object:

- ``create_token``   — mint / refresh a credential from username+password
- ``get_token``      — return a valid credential for a username
- ``validate_token`` — verify a presented credential (hot path)

Backends are duck-typed against this Protocol (no inheritance required)
and register themselves with :mod:`fastink.auth.backends.registry` via
the ``@register_backend("<name>")`` decorator. The backend for a request
is selected by the ``auth.type`` config key, which is also the backend's
``name`` and the authentication record name in the database.

Built-in backends (password, krb5) live under this package. Site
backends (e.g. IHEP's apikey/hai) live in plugin packages and register
the same way — the plugin's ``initialize()`` imports its backend module.
"""

from __future__ import annotations

from typing import Optional, Protocol, runtime_checkable


@runtime_checkable
class AuthBackend(Protocol):
    """Duck-typed contract for one authentication mode.

    Implementations must expose ``name`` and the three methods below.
    They are NOT required to inherit anything; ``@runtime_checkable``
    lets the registry sanity-check the shape at registration time.
    """

    #: Canonical backend name; equals ``auth.type`` and the DB
    #: authentication record name.
    name: str

    def create_token(self, username: str, password: Optional[str] = None) -> Optional[dict]:
        """Mint or refresh a credential for ``username``.

        Password-based backends require ``password``. Backends whose
        credentials are issued externally (e.g. apikey) may treat this
        as a no-op and return None.
        """
        ...

    def get_token(self, username: str) -> str:
        """Return a currently-valid credential string for ``username``.

        Backends that persist/renew credentials (krb5) do so here;
        lookup-only backends (password) read their store. Raises when no
        valid credential can be produced.
        """
        ...

    def validate_token(
        self,
        username: str,
        token: str,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        issuer: Optional[str] = None,
    ) -> bool:
        """Verify a presented credential (called on every API request).

        ``client_id`` / ``client_secret`` / ``issuer`` are only used by
        backends that talk to an external identity provider (apikey);
        password/krb5 ignore them. They are listed explicitly rather
        than hidden behind ``**kwargs`` for readability.
        """
        ...
