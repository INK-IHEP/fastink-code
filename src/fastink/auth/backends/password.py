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
import functools
import hmac
import warnings
from pathlib import Path
import pwd
import uuid
from datetime import datetime, timedelta
from typing import Any, Optional

from cryptography.fernet import Fernet
from passlib.context import CryptContext
from passlib.exc import UnknownHashError

from fastink.auth.backends.registry import register_backend
from fastink.auth.common import (
    get_authentication,
    get_token,
    get_user,
    save_token,
    update_token,
)
from fastink.common.logger import logger


password_context = CryptContext(
    schemes=["sha512_crypt", "sha256_crypt", "md5_crypt", "des_crypt"]
)


def _system_crypt(password: str, shadow_hash: str) -> Optional[str]:
    """Hash via the system crypt(3) (libxcrypt) — covers formats passlib
    does not implement, e.g. yescrypt ``$y$`` default on Debian/Ubuntu/
    Fedora. Returns None when unavailable or the format is unsupported."""
    try:
        with warnings.catch_warnings():
            # Python 3.12 deprecates crypt; the runtime image pins 3.12
            # and libxcrypt provides the wide format support we need.
            warnings.simplefilter("ignore", DeprecationWarning)
            import crypt

            return crypt.crypt(password, shadow_hash)
    except Exception:
        return None


def _verify_password_hash(password: str, shadow_hash: str) -> bool:
    try:
        return bool(password_context.verify(password, shadow_hash))
    except UnknownHashError:
        pass
    except ValueError:
        return False
    computed = _system_crypt(password, shadow_hash)
    return bool(computed) and hmac.compare_digest(computed, shadow_hash)


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


def _read_shadow() -> str:
    return Path("/etc/shadow").read_text()


# Precomputed sha512_crypt hash verified against on missing-user paths so
# their latency resembles a typical real shadow lookup instead of returning
# instantly; the verify result is discarded and both branches still return
# False. Implicit rounds=5000 matches the glibc/shadow-utils default (the
# dominant $6$ case) — exact parity with per-site custom round counts or
# yescrypt cost parameters is inherently unachievable for a user that does
# not exist, so this narrows the enumeration oracle without claiming to
# close it.
_DUMMY_SHADOW_HASH = (
    "$6$TYUPolFTHThlIQSn$"
    "kWzv/pzxMozOy3s3KNEGm/TMC.kmIJT2WCjfitFUsZdE5UpI3v9Cy2uvGpNi1d."
    "Xs0NatRj0Fei41aVdULEbi1"
)


def _get_shadow_hash(username: str, shadow_text: str) -> Optional[str]:
    for line in shadow_text.splitlines():
        fields = line.split(":")
        if len(fields) > 1 and fields[0] == username:
            return fields[1]
    return None


def _validate_user(username: str, password: str, uid: Optional[int] = None) -> bool:
    try:
        user_info = pwd.getpwnam(username)
        if uid is not None and user_info.pw_uid != uid:
            logger.error("UID does not match UID in passwd, please check your input")
            return False
        shadow_hash = _get_shadow_hash(username, _read_shadow())
        if shadow_hash is None:
            logger.error("User does not exist in passwd, please check your input")
            _verify_password_hash(password, _DUMMY_SHADOW_HASH)
            return False
        verified = _verify_password_hash(password, shadow_hash)
        if not verified:
            logger.error("Password does not match passwd, please check your input")
            return False
    except KeyError:
        logger.error("User does not exist in passwd, please check your input")
        _verify_password_hash(password, _DUMMY_SHADOW_HASH)
        return False
    return True


@register_backend("password")
class PasswordBackend:
    """Password authentication backend."""

    name = "password"

    # exposed for user.add_user (password mode validates before insert)
    def validate_user(self, username: str, password: str, uid: Optional[int] = None) -> bool:
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
            save_token(
                user_id=user_id,
                authentication_id=authentication_id,
                token=token_value,
                generated_at=generated_at,
                expired_at=expired_at,
            )
        except Exception:
            logger.error(f"Failed to save token for {username}")
            raise
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
            user_record = get_user(username=username)
            stored_token = get_token(user_id=user_record["id"])
            if stored_token is None or stored_token["expired_at"] < datetime.now():
                return False
            if not hmac.compare_digest(stored_token["token"], token):
                return False
            return _decrypt(token) == username
        except Exception:
            return False
