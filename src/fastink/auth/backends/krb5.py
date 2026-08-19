import hashlib
import os
import pexpect
import subprocess
import tempfile
from datetime import datetime, timedelta
from typing import Optional

from fastink.common.hooks import hookable
from fastink.common.logger import logger
from fastink.common.utils import ccachefile_to_token, token_to_ccachefile
from fastink.auth.account import validate_account_status
from fastink.auth.common import (
    add_kerberos_token,
    get_kerberos_token,
    get_user,
    update_kerberos_token,
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


def _generate_tgt(username: str, password: str, ccachefile: str) -> bool:
    logger.debug(
        f"Generating TGT for {username}, with hashed password {hashlib.sha256(password.encode('utf-8')).hexdigest()}"
    )

    try:
        # use `with` to manage `pexpect.spawn`, need python3.12+
        with pexpect.spawn(f"kinit -c {ccachefile} {username}") as child:
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
                    ["kinit: Password incorrect", pexpect.EOF, pexpect.TIMEOUT],
                    timeout=5,
                )
                if auth_index == 0:
                    log = f"Password for {username} is incorrect."
                    logger.error(log)
                    raise ValueError(log)
                elif auth_index == 1:
                    log = f"Authentication for {username} succeeded."
                    logger.debug(log)
                elif auth_index == 2:
                    log = f"Password for {username} verification timeout."
                    logger.error(log)
                    raise TimeoutError(log)

            elif index == 1:
                logs = f"User {username} does not exist."
                logger.error(logs)
                raise ValueError(logs)
            elif index == 2:
                logs = f"AFS account for {username} is expired."
                logger.error(logs)
                raise ValueError(logs)
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

    logger.debug(f"Successfully got TGT for {username}")
    return True


def _renew_tgt(ccachefile: str) -> bool:
    logger.debug(f"Renewing TGT for {ccachefile}")
    try:
        result = subprocess.run(
            ["krenew", "-k", f"{ccachefile}", "-t"], capture_output=True, check=True
        )
        if result.returncode == 0:
            return True
    except subprocess.CalledProcessError as error:
        raise ValueError(f"Failed to renew TGT for {ccachefile}. {error}")


def resolve_tgt(ccache_file: str):
    def time_to_timestamp(date_str, time_str):
        datetime_str = f"{date_str} {time_str}"
        dt = datetime.strptime(datetime_str, "%m/%d/%y %H:%M:%S")
        timestamp = dt.timestamp()
        return int(timestamp)

    logger.debug(f"Resolving tgt")
    try:
        result = subprocess.run(
            ["klist", "-c", f"{ccache_file}"],
            capture_output=True,
            text=True,
            env={"LC_TIME": "C"},
        )
    except subprocess.CalledProcessError as error:
        raise ValueError(f"Failed to resolve tgt for {ccache_file}. {error}")
    result = result.stdout.split("\n")
    username = result[1].split()[2].split("@")[0]
    expired_at = result[4].split()[2:4]
    renew_until = result[5].split()[2:4]
    logger.debug(
        f"Username: {username}, Expired at: {expired_at}, Renew until: {renew_until}"
    )
    expired_at_int = time_to_timestamp(*expired_at)
    renew_until_int = time_to_timestamp(*renew_until)
    return {
        "username": username,
        "expired_at": expired_at_int,
        "renew_until": renew_until_int,
    }


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
    except Exception as error:
        os.remove(ccachefile)
        raise Exception(f"User {username} failed to get TGT: {error}")
    token = ccachefile_to_token(ccachefile)
    logger.debug(f"User {username} token: {token[0:100]}......")
    os.remove(ccachefile)

    _persist_ccache_token(username, token)
    return True


def _persist_ccache_token(username: str, token: str) -> None:
    """Save a base64 ccache token to the database for ``username``.

    Adds a new kerberos token row if the user has none, otherwise updates
    the existing one. ``generated_at``/``expired_at`` are computed here so
    every caller (password kinit via ``create_krb5`` and passwordless
    refill via ``get_krb5``) persists tickets consistently. ``expired_at``
    is the conventional 25h estimate used throughout this module, not the
    ticket's exact klist expiry.

    Args:
        username: Username whose ticket is being stored.
        token: Base64-encoded ccache token.

    Raises:
        Exception: When the add/update database operation fails.
    """
    generated_at = datetime.now()
    expired_at = datetime.now() + timedelta(hours=25)

    logger.debug(f"Save {username} token to database")
    user_item = get_user(username=username)
    logger.debug(f"{username} user_item is {user_item}")
    user_id = user_item["id"]
    logger.debug(f"{username} user_id is {user_id}")
    try:
        get_kerberos_token(user_id=user_id)
    except:
        logger.debug(f"No token found, add a new one")
        try:
            result = add_kerberos_token(
                user_id=user_id,
                token=token,
                generated_at=generated_at,
                expired_at=expired_at,
            )
            if result:
                logger.debug(f"Add {username} token succeeded!!!")
        except:
            logger.debug(f"Add {username} token to database failed")
            raise Exception(f"Add {username} token to database failed")
    else:
        logger.debug(f"Token found, try to update it")
        try:
            update_kerberos_token(
                user_id=user_id,
                token=token,
                generated_at=generated_at,
                expired_at=expired_at,
            )
        except:
            logger.debug(f"Update {username} token to database failed")
            raise Exception(f"Update {username} token to database failed")


def _refill_passwordless_or_raise(username: str, original_error: Exception) -> str:
    """Try to mint a fresh ticket without a password, else re-raise.

    Called from ``get_krb5`` dead-ends (no ticket / expired / renew
    failed). If the passwordless hook mints a ccache, it is persisted and
    returned. If no passwordless backend is configured, the original error
    is raised so behaviour matches deployments without SSO.

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
        raise original_error
    logger.debug(f"Passwordless ticket acquired for {username}, persisting.")
    _persist_ccache_token(username, token)
    return token


def get_krb5(
    username: Optional[str] = None,
    email: Optional[str] = None,
    uid: Optional[int] = None,
    expire_in: int = 3600,
) -> Optional[str]:
    """Get a kerberos token by username, email or uid. If the token is expiring, it will be renewed.

    Args:
        username (Optional[str], optional): Username in database. Defaults to None.
        email (Optional[str], optional): Email in database. Defaults to None.
        uid (Optional[int], optional): UID in database. Defaults to None.
        expire_in (int, optional): Expiring limits. Defaults to 3600.

    Returns:
        Optional[str]: Token, None if not found.
    """
    # Validating account status (site plugins may check an external
    # account database, e.g. IHEP CCS).
    validate_result = validate_account_status(username=username)
    if not validate_result.get("account_valid", True):
        raise ValueError("Account is expired")
    if not validate_result.get("password_valid", True):
        raise ValueError("Account password is expired")

    # Getting Token from database.
    logger.debug(f"User {username} is trying to extend TGT.")
    user_item = get_user(username=username, email=email, uid=uid)
    user_id = user_item["id"]
    # The passwordless mint API keys off the Kerberos principal name, which
    # may not have been passed in when get_krb5 is called by email/uid.
    principal = user_item.get("username") or username
    try:
        ticket = get_kerberos_token(user_id=user_id)
    except Exception as err:
        # No ticket in the database (e.g. an SSO user who never submitted a
        # password). Try to mint one without a password before giving up.
        return _refill_passwordless_or_raise(
            principal, ValueError(f"Token not exists in database: {err}")
        )

    # Judging if it is expired.
    if ticket["expired_at"] < datetime.now():
        # Expired ticket; try a passwordless refill before failing.
        return _refill_passwordless_or_raise(
            principal, ValueError("Token is expired")
        )
    elif ticket["expired_at"] - datetime.now() < timedelta(seconds=expire_in):
        logger.debug(f"Token for {username} is expiring soon.")
        fd, ccachefile = tempfile.mkstemp()
        os.close(fd)
        token_to_ccachefile(token=ticket["token"], ccachefile=ccachefile)
        generated_at = datetime.now()
        expired_at = datetime.now() + timedelta(hours=25)
        try:
            _renew_tgt(ccachefile)
        except Exception as err:
            # renew_until has likely passed; fall back to a passwordless
            # refill before failing.
            os.remove(ccachefile)
            return _refill_passwordless_or_raise(principal, err)
        token = ccachefile_to_token(ccachefile)
        os.remove(ccachefile)
        update_kerberos_token(
            user_id=user_id,
            token=token,
            generated_at=generated_at,
            expired_at=expired_at,
        )
        return token
    else:
        return ticket["token"]


def validate_krb5_token(
    username: str,
    token: str,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    issuer: Optional[str] = None,
) -> bool:
    """Verify a presented Kerberos ccache token (moved from plugins/krb5.py)."""
    fd, ccachefile = tempfile.mkstemp()
    os.close(fd)
    try:
        token_to_ccachefile(token, ccachefile)
        try:
            tgt_result = resolve_tgt(ccachefile)
        except ValueError:
            raise Exception("Invalid Kerberos token")
        if tgt_result["username"] != username:
            raise Exception("username not match")
        if tgt_result["renew_until"] <= int(datetime.now().timestamp()):
            raise Exception("Kerberos token expired")
        return True
    finally:
        # Always clean up the temp ccache, even when the token is
        # malformed/invalid/expired, so hot-path validation of bad
        # tokens cannot leak temp files.
        try:
            os.remove(ccachefile)
        except OSError:
            pass


from fastink.auth.backends.registry import register_backend


@register_backend("krb5")
class Krb5Backend:
    """Kerberos authentication backend.

    Wraps the module-level TGT implementation (kinit/krenew/klist) behind
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
