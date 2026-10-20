"""Account validity checks for FastINK.

Provides a hookable function to check whether a user's account and
password are still valid at the site level (independent of token
validity). The default implementation treats every account as valid.
Site plugins (e.g., IHEP) override this hook to query their account
database (IHEP overrides it with a CCS database lookup).
"""

from fastink.common.hooks import hookable


@hookable
def validate_account_status(username: str) -> dict:
    """Return the site-level validity of a user account.

    Default implementation treats all accounts as valid.
    Plugins can override this via
    register_hook("fastink.auth.account.validate_account_status").

    Args:
        username: The username to check.

    Returns:
        Dict with two boolean keys:
        - "account_valid": the account itself has not expired
        - "password_valid": the account password has not expired
    """
    return {"account_valid": True, "password_valid": True}
