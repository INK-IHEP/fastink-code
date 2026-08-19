"""Password authentication backend for FastINK.

Consolidates the former ``auth/token.py`` (DB token orchestration) and
``auth/plugins/password.py`` (Fernet crypto + /etc/passwd validation)
into one :class:`PasswordBackend`.

Tokens are the username encrypted with a Fernet key derived from the
``password`` authentication record's UUID. Validation decrypts and
compares; user validation checks the system password database.
"""

from __future__ import annotations

import base64
import crypt
import functools
import pwd
import spwd
import uuid
from datetime import datetime, timedelta
from typing import Any, Optional

from cryptography.fernet import Fernet

from fastink.auth.backends.registry import register_backend
from fastink.auth.common import (
    add_token,
    get_authentication,
    get_token,
    get_user,
    update_token,
)
from fastink.common.logger import logger


@functools.lru_cache(maxsize=1)
def _fernet_key() -> bytes:
    """Derive the Fernet key from the 'password' authentication UUID.

    Cached via lru_cache so the DB is queried only once per process
    lifetime (the UUID is stable for the 'password' authentication record).
    """
    uuid_str = str(get_authentication(authentication="password")["id"])
    uuid_bytes = uuid.UUID(uuid_str).bytes
    return base64.urlsafe_b64encode(uuid_bytes.ljust(32, b"\0"))


def _encrypt(payload: str) -> str:
    return Fernet(_fernet_key()).encrypt(payload.encode("utf-8")).decode("utf-8")


def _decrypt(token: str) -> str:
    return Fernet(_fernet_key()).decrypt(token.encode("utf-8")).decode("utf-8")


def _validate_user(username: str, password: str, uid: Optional[str] = None) -> bool:
    try:
        user_info = pwd.getpwnam(username)
        if uid and user_info.pw_uid != uid:
            logger.error("UID does not match UID in passwd, please check your input")
            return False
        encrypted_pw = spwd.getspnam(username).sp_pwd
        if not crypt.crypt(password, encrypted_pw) == encrypted_pw:
            logger.error("Password does not match passwd, please check your input")
            return False
    except KeyError:
        logger.error("User does not exist in passwd, please check your input")
        return False
    return True


@register_backend("password")
class PasswordBackend:
    """Password authentication backend."""

    name = "password"

    # exposed for user.add_user (password mode validates before insert)
    def validate_user(self, username: str, password: str, uid: Optional[str] = None) -> bool:
        return _validate_user(username, password, uid)

    def create_token(self, username: str, password: Optional[str] = None, expire_in: int = 86400) -> Optional[dict]:
        if password is None:
            raise ValueError("password backend requires a password to create a token")
        if not _validate_user(username, password):
            raise ValueError("Invalid username or password")

        user_id = get_user(username=username)["id"]
        authentication_id = get_authentication(authentication=self.name)["id"]
        token_value = _encrypt(username)
        generated_at = datetime.now()
        expired_at = datetime.now() + timedelta(seconds=expire_in)

        logger.debug(f"Create token for {username} = {user_id}")
        try:
            get_token(user_id=user_id)
        except Exception:
            logger.debug(f"No token found for {username}, add a new one")
            try:
                add_token(
                    user_id=user_id,
                    authentication_id=authentication_id,
                    token=token_value,
                    generated_at=generated_at,
                    expired_at=expired_at,
                )
            except Exception:
                logger.error(f"Failed to create token for {username}")
                raise Exception(f"Failed to create token for {username}")
        else:
            logger.debug("Token exists, need to update it")
            try:
                update_token(
                    user_id=user_id,
                    authentication_id=authentication_id,
                    token=token_value,
                    generated_at=generated_at,
                    expired_at=expired_at,
                )
            except Exception:
                logger.error(f"Failed to update token for {username}")
                raise Exception(f"Failed to update token for {username}")
        return {"method": self.name}

    def get_token(self, username: str, expire_in: int = 3600) -> str:
        user_id = get_user(username=username)["id"]
        token = get_token(user_id=user_id)
        logger.debug(f"Query token for {username} = {user_id}")
        if token is None or token["expired_at"] < datetime.now():
            logger.debug(f"Token for {username} is expired.")
            raise ValueError("Token expired")
        elif token["expired_at"] - datetime.now() < timedelta(seconds=expire_in):
            logger.debug(f"Token for {username} is expiring soon.")
            authentication_id = get_authentication(authentication=self.name)["id"]
            generated_at = datetime.now()
            expired_at = datetime.now() + timedelta(seconds=expire_in)
            update_token(
                user_id=user_id,
                authentication_id=authentication_id,
                token=token["token"],
                generated_at=generated_at,
                expired_at=expired_at,
            )
            return token["token"]
        else:
            logger.debug(f"Token for {username} is valid.")
            return token["token"]

    def validate_token(
        self,
        username: str,
        token: str,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        issuer: Optional[str] = None,
    ) -> bool:
        try:
            return _decrypt(token) == username
        except Exception:
            return False
