import base64
import os
import pexpect
import tempfile
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy.exc import NoResultFound

from fastink.auth.backends.ccache import parse_ccache
from fastink.auth.backends.errors import (
    AccountExpiredError,
    PasswordExpiredError,
    UserNotFoundError,
)
from fastink.common.config import get_config
from fastink.common.hooks import hookable
from fastink.common.logger import logger
from fastink.common.utils import ccachefile_to_token
from fastink.auth.account import validate_account_status
from fastink.auth.common import (
    get_kerberos_token,
    get_user,
    save_token,
)


class PasswordlessTicketUnavailable(Exception):
    """Raised when no passwordless mechanism is available to mint a ccache.

    The default implementation of ``acquire_ccache_passwordless`` always
    raises this. Sites that support SSO/passwordless login (e.g. IHEP)
    override the hook to actually mint a ticket, so this exception only
    surfaces on deployments without such a backend.
    """


@hookable
def acquire_ccache_passwordless(username: str) -> str:
    """Mint a fresh Kerberos ccache for ``username`` WITHOUT a password.

    Used by ``get_krb5`` when the database has no usable ticket for a user
    who never submitted a password (the SSO login flow). Returns a base64
    ccache token in the same format as
    :func:`fastink.common.utils.ccachefile_to_token`.

    Default implementation: no passwordless mechanism exists, so it fails
    honestly. Site plugins override this via
    ``register_hook("fastink.auth.backends.krb5.acquire_ccache_passwordless")``.

    Args:
        username: The Kerberos principal to mint a ticket for.

    Returns:
        A base64-encoded ccache token.

    Raises:
        PasswordlessTicketUnavailable: When no backend is configured.
    """
    raise PasswordlessTicketUnavailable(
        f"No passwordless ticket mechanism configured for {username}"
    )


def _pexpect_output(value) -> str:
    if value is None:
        return "<none>"
    if value in (pexpect.EOF, pexpect.TIMEOUT):
        return getattr(value, "__name__", str(value))
    if isinstance(value, bytes):
        return value.decode(errors="ignore")
    return str(value)


def _get_encrypt_key():
    delegation = get_config("auth", "delegation", fallback={}) or {}
    if isinstance(delegation, dict):
        return delegation.get("ccache_encrypt_key", "")
    return ""


def _get_fernet():
    from cryptography.fernet import Fernet, MultiFernet
    key = _get_encrypt_key()
    keys = [key] if isinstance(key, str) else list(key)
    keys = [k for k in keys if k]
    if not keys:
        raise RuntimeError("auth.delegation.ccache_encrypt_key not configured")
    fernets = [Fernet(k.encode() if isinstance(k, str) else k) for k in keys]
    return MultiFernet(fernets) if len(fernets) > 1 else fernets[0]


def _encrypt_ccache(token: str) -> str:
    return _get_fernet().encrypt(token.encode()).decode()


def _decrypt_ccache(encrypted: str) -> str:
    return _get_fernet().decrypt(encrypted.encode()).decode()


def _encryption_enabled() -> bool:
    return bool(_get_encrypt_key())


def _generate_tgt(username: str, password: str, ccachefile: str) -> bool:
    logger.debug(f"Generating TGT for {username}")

    try:
        child = pexpect.spawn(f"kinit -c {ccachefile} {username}")
        try:
            index = child.expect(
                [
                    "Password for .*:",
                    "kinit: Client .* not found in Kerberos database while getting initial credentials",
                    "kinit: Client's entry in database has expired while getting initial credentials",
                    "kinit: Generic preauthentication failure while getting initial credentials",
                    pexpect.TIMEOUT,
                ],
                timeout=5,
            )
        except Exception as e:
            output = _pexpect_output(child.before)
            after = _pexpect_output(child.after)
            log = f"kerberos5 pexpect exception: {e}\nchild.before: {output}\nchild.after: {after}"
            logger.error(log)
            raise ValueError(log)

        if index == 0:
            # authenticate user with its password
            logger.debug(f"Input password for {username}.")
            child.sendline(password)
            password = None
            auth_index = child.expect(
                [
                    "kinit: Password incorrect",
                    "kinit: Password has expired.*",
                    pexpect.EOF,
                    pexpect.TIMEOUT,
                ],
                timeout=5,
            )
            if auth_index == 0:
                log = f"Password for {username} is incorrect."
                logger.error(log)
                raise ValueError(log)
            elif auth_index == 1:
                log = f"Password for {username} has expired."
                logger.error(log)
                raise PasswordExpiredError(log)
            elif auth_index == 2:
                log = f"Authentication for {username} succeeded."
                logger.debug(log)
            elif auth_index == 3:
                log = f"Password for {username} verification timeout."
                logger.error(log)
                raise TimeoutError(log)

        elif index == 1:
            logs = f"User {username} does not exist."
            logger.error(logs)
            raise UserNotFoundError(logs)
        elif index == 2:
            logs = f"AFS account for {username} is expired."
            logger.error(logs)
            raise AccountExpiredError(logs)
        elif index == 3:
            logs = f"Preauthentication for {username} failed."
            logger.error(logs)
            raise ValueError(logs)
        elif index == 4:
            logs = f"Kerberos server connection timeout."
            logger.error(logs)
            raise TimeoutError(logs)

    except pexpect.EOF:
        logger.debug("Child process exited normally.")
    except pexpect.TIMEOUT:
        logger.error("Operation timed out.")
    finally:
        child.close()

    logger.debug(f"Successfully got TGT for {username}")
    return True


def create_krb5(username: str, password: str) -> bool:
    """Create a new kerberos token by username and password.

    Args:
        username (str): Username of the Kerberos principal.
        password (str): Password of the Kerberos principal.

    Returns:
        bool: Whether the token is created successfully.
    """
    # Generating token.
    logger.debug(f"User {username} is trying to get TGT.")
    fd, ccachefile = tempfile.mkstemp()
    os.close(fd)
    try:
        _generate_tgt(username, password, ccachefile)
    except Exception:
        os.remove(ccachefile)
        raise
    token = ccachefile_to_token(ccachefile)
    os.remove(ccachefile)

    _persist_ccache_token(username, token)
    return True


def _persist_ccache_token(username: str, token: str) -> None:
    generated_at = datetime.now()
    expired_at = datetime.fromtimestamp(
        parse_ccache(base64.b64decode(token))["expired_at"]
    )
    if _encryption_enabled():
        token = _encrypt_ccache(token)

    logger.debug(f"Save {username} token to database")
    user_item = get_user(username=username)
    logger.debug(f"{username} user_item is {user_item}")
    user_id = user_item["id"]
    logger.debug(f"{username} user_id is {user_id}")
    try:
        save_token(
            user_id=user_id,
            token=token,
            generated_at=generated_at,
            expired_at=expired_at,
        )
    except Exception:
        logger.debug(f"Save {username} token to database failed")
        raise


def _refill_passwordless_or_raise(username: str, original_error: Exception) -> str:
    """Try to mint a fresh ticket without a password, else re-raise.

    Called from ``get_krb5`` dead-ends. If the passwordless hook mints a
    ccache, it is persisted and returned. If no passwordless backend is
    configured, the original error is raised so behaviour matches deployments
    without SSO.

    Args:
        username: Principal to mint a ticket for.
        original_error: The error to raise if passwordless refill is
            unavailable.

    Returns:
        A base64 ccache token.

    Raises:
        Exception: ``original_error`` when passwordless refill is
            unavailable.
    """
    try:
        token = acquire_ccache_passwordless(username)
    except PasswordlessTicketUnavailable:
        logger.error(
            "Passwordless refill unavailable for %s; raising original error: %s",
            username,
            original_error,
        )
        raise original_error
    except Exception as err:
        logger.error(
            "Passwordless refill failed for %s (%s); original error was: %s",
            username,
            err,
            original_error,
        )
        raise
    logger.info("Passwordless ticket acquired for %s, persisting.", username)
    _persist_ccache_token(username, token)
    return token


def get_krb5(
    username: Optional[str] = None,
    uid: Optional[int] = None,
    expire_in: int = 21600,
) -> Optional[str]:
    """Get a kerberos token by username or uid, reissuing expiring tickets.

    Args:
        username (Optional[str], optional): Username in database. Defaults to None.
        uid (Optional[int], optional): UID in database. Defaults to None.
        expire_in (int, optional): Expiring limits. Defaults to 21600.

    Returns:
        Optional[str]: Token, None if not found.
    """
    # Resolve the user first so uid-only callers get a real username for the
    # account-status check below (site plugins may query an external account
    # database, e.g. IHEP CCS, and cannot validate a None username).
    try:
        user_item = get_user(username=username, uid=uid)
    except NoResultFound:
        raise UserNotFoundError(f"User not found (username={username!r}, uid={uid!r})")
    user_id = user_item["id"]
    # The passwordless mint API keys off the Kerberos principal name, which
    # may not have been passed in when get_krb5 is called by uid.
    principal = user_item.get("username") or username

    validate_result = validate_account_status(username=principal)
    if not validate_result.get("account_valid", True):
        logger.warning(
            "Account is expired for %s (validate_result=%s).",
            principal,
            validate_result,
        )
        raise AccountExpiredError("Account is expired")
    if not validate_result.get("password_valid", True):
        logger.warning(
            "Account password is expired for %s (validate_result=%s).",
            principal,
            validate_result,
        )
        raise PasswordExpiredError("Account password is expired")

    # Getting Token from database.
    logger.debug(f"User {principal} is trying to extend TGT.")
    try:
        ticket = get_kerberos_token(user_id=user_id)
    except Exception as err:
        # No ticket in the database (e.g. an SSO user who never submitted a
        # password). Try to mint one without a password before giving up.
        logger.info(
            "No Kerberos token in DB for %s (%s); attempting passwordless refill.",
            principal,
            err,
        )
        return _refill_passwordless_or_raise(
            principal, ValueError(f"Token not exists in database: {err}")
        )

    if ticket["expired_at"] - datetime.now() < timedelta(seconds=expire_in):
        remaining = (ticket["expired_at"] - datetime.now()).total_seconds()
        logger.info(
            "DB token for %s expired or expiring soon (db_expired_at=%s, "
            "remaining=%ss, threshold=%ss); re-issuing.",
            principal,
            ticket["expired_at"],
            remaining,
            expire_in,
        )
        return _refill_passwordless_or_raise(principal, ValueError("Token is expired"))

    remaining = (ticket["expired_at"] - datetime.now()).total_seconds()
    logger.info(
        "Returning DB token directly for %s (db_expired_at=%s, remaining=%ss).",
        principal,
        ticket["expired_at"],
        remaining,
    )
    token = ticket["token"]
    if _encryption_enabled():
        token = _decrypt_ccache(token)
    return token


def validate_krb5_token(
    username: str,
    token: str,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    issuer: Optional[str] = None,
) -> bool:
    """Verify a presented Kerberos ccache token (moved from plugins/krb5.py)."""
    try:
        tgt = parse_ccache(base64.b64decode(token))
    except ValueError:
        raise Exception("Invalid Kerberos token")
    if tgt["username"] != username:
        raise Exception("username not match")
    if tgt["expired_at"] <= int(datetime.now().timestamp()):
        raise Exception("Kerberos token expired")
    return True


from fastink.auth.backends.registry import register_backend


@register_backend("krb5")
class Krb5Backend:
    """Kerberos authentication backend.

    Wraps the module-level TGT implementation behind
    the AuthBackend protocol. The heavy lifting stays in the module
    functions above so the tested logic is unchanged.
    """

    name = "krb5"

    def create_token(self, username: str, password: Optional[str] = None) -> Optional[dict]:
        if password is None:
            raise ValueError("krb5 backend requires a password to create a ticket")
        create_krb5(username, password)
        return {"method": self.name}

    def get_token(self, username: str) -> str:
        return get_krb5(username=username)

    def validate_token(
        self,
        username: str,
        token: str,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        issuer: Optional[str] = None,
    ) -> bool:
        return validate_krb5_token(username, token, client_id, client_secret, issuer)
