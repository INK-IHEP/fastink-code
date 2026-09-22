"""Structured auth-backend failure types.

These exceptions let routers distinguish *why* authentication failed and
map each cause to a specific ``InkStatus`` code instead of collapsing
everything into one generic failure. They are backend-agnostic: any
backend (krb5, password, a site plugin) may raise them.
"""


class UserNotFoundError(Exception):
    """The username does not exist in the authentication source."""


class AccountExpiredError(Exception):
    """The account exists but is expired or revoked."""


class PasswordExpiredError(Exception):
    """The password is expired and must be changed."""
