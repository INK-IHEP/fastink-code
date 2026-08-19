"""External user-directory verification for FastINK.

Provides a hookable function to verify a user's identity against an
external directory service before the user is added to the database.
The default implementation performs no verification. Site plugins
(e.g., IHEP) override this hook to validate against their identity
provider (IHEP overrides it with a UMT lookup).
"""

from fastink.common.hooks import hookable


@hookable
def verify_user_identity(username: str, email=None, uid=None) -> None:
    """Verify that a user exists in the site's external directory.

    Default implementation is a no-op (no external directory).
    Plugins can override this via
    register_hook("fastink.auth.directory.verify_user_identity").

    Args:
        username: Requested username.
        email: Optional email address to cross-check.
        uid: Optional numeric UID to cross-check.

    Raises:
        ValueError: If the identity does not match the directory.
    """
    return None
