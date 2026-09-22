from html import escape
from urllib.parse import urlencode

from fastink.auth.backends.registry import get_auth_backend
from fastink.common.config import get_config
from fastink.common.logger import logger


def _krb5_login(username: str, password: str) -> bool:
    """Verify via kinit; on success the TGT is persisted for the delegation layer."""
    try:
        get_auth_backend("krb5").create_token(username, password)
        return True
    except Exception as error:
        logger.warning("krb5 login failed for %s: %s", username, error)
        return False


def _shadow_login(username: str, password: str) -> bool:
    return get_auth_backend("password").validate_user(username, password)


def verify_login(username: str, password: str) -> bool:
    if get_config("auth", "type", fallback="password") == "krb5":
        return _krb5_login(username, password)
    return _shadow_login(username, password)


def login_page(params: dict[str, str | None], error: str = "", action: str = "/authorize") -> str:
    hidden = "".join(
        f'<input type="hidden" name="{escape(name)}" value="{escape(value or "")}">'
        for name, value in params.items()
    )
    query = escape(urlencode({name: value for name, value in params.items() if value is not None}))
    error_html = f"<p>{escape(error)}</p>" if error else ""
    return (
        "<!doctype html><html><body><main>"
        f"{error_html}<form method=\"post\" action=\"{escape(action)}\">"
        f"{hidden}<input name=\"username\" required><input name=\"password\" type=\"password\" required>"
        "<button type=\"submit\">Log in</button></form>"
        f"<a href=\"/authorize/sso?{query}\">Log in with IHEP SSO</a>"
        "</main></body></html>"
    )
