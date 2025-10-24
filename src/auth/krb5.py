import hashlib
import os
import pexpect
import subprocess
import tempfile
from datetime import datetime, timedelta
from typing import Optional

from src.common.logger import logger
from src.common.utils import ccachefile_to_token, token_to_ccachefile
from src.auth.common import (
    add_kerberos_token,
    get_kerberos_token,
    get_user,
    update_kerberos_token,
)


def _generate_tgt(username: str, password: str, ccachefile: str) -> bool:
    logger.debug(
        f"Generating TGT for {username}, with hashed password {hashlib.sha256(password.encode('utf-8')).hexdigest()}"
    )

    try:
        # use `with` to manage `pexpect.spawn`, need python3.12+
        with pexpect.spawn(f"kinit -c {ccachefile} {username}@IHEPKRB5") as child:
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
                output = (
                    child.before.decode(errors="ignore")
                    if child.before
                    else "<no output>"
                )
                after = (
                    child.after.decode(errors="ignore") if child.after else "<no after>"
                )
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
            ["klist", "-c", f"{ccache_file}"], capture_output=True, text=True, env={"LC_TIME": "C"}
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
    """Create a new kerberos token by username and password, only for IHEP-SSO.

    Args:
        username (str): Username of IHEP-AFS account.
        password (str): Password of IHEP-AFS account.

    Returns:
        bool: Whether the token is created successfully.
    """
    # Generating token.
    logger.debug(f"User {username} is trying to get TGT.")
    fd, ccachefile = tempfile.mkstemp()
    os.close(fd)
    try:
        generated_at = datetime.now()
        expired_at = datetime.now() + timedelta(hours=25)
        _generate_tgt(username, password, ccachefile)
    except Exception as error:
        os.remove(ccachefile)
        raise Exception(f"User {username} failed to get TGT: {error}")
    token = ccachefile_to_token(ccachefile)
    logger.debug(f"User {username} token: {token[0:100]}......")
    os.remove(ccachefile)

    # Saving token to database.
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
    return True


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
    # Getting Token from database.
    logger.debug(f"User {username} is trying to extend TGT.")
    user_id = get_user(username=username, email=email, uid=uid)["id"]
    ticket = get_kerberos_token(user_id=user_id)

    # Judging if it is expired.
    if ticket["expired_at"] < datetime.now():
        raise ValueError("Token expired")
    elif ticket["expired_at"] - datetime.now() < timedelta(seconds=expire_in):
        logger.debug(f"Token for {username} is expiring soon.")
        fd, ccachefile = tempfile.mkstemp()
        os.close(fd)
        token_to_ccachefile(token=ticket["token"], ccachefile=ccachefile)
        generated_at = datetime.now()
        expired_at = datetime.now() + timedelta(hours=25)
        _renew_tgt(ccachefile)
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
