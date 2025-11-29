import base64
import crypt
import pwd
import spwd
import uuid

from cryptography.fernet import Fernet
from datetime import datetime, timedelta
from typing import Any, Optional

from src.auth.common import get_authentication
from src.common.logger import logger


UUID_STR = str(get_authentication(authentication="password")["id"])


def derive_key_from_uuid(uuid_str: str = UUID_STR) -> bytes:
    """
    将 UUID 派生成 32 字节的 key（Fernet 需要 32 bytes base64 编码）
    """
    # 将 UUID 转为 16 字节二进制，然后再 base64 编码成符合 Fernet 规范的 key
    uuid_bytes = uuid.UUID(uuid_str).bytes
    key = base64.urlsafe_b64encode(uuid_bytes.ljust(32, b"\0"))  # 填充到 32 bytes
    return key


def encrypt_payload(payload: str, uuid_str: str = UUID_STR) -> str:
    key = derive_key_from_uuid(uuid_str)
    f = Fernet(key)
    token = f.encrypt(payload.encode("utf-8"))
    return token.decode("utf-8")


def decrypt_payload(token: str, uuid_str: str = UUID_STR) -> str:
    key = derive_key_from_uuid(uuid_str)
    f = Fernet(key)
    payload = f.decrypt(token.encode("utf-8"))
    return payload.decode("utf-8")


def create_token(
    username: str, password: str, expire_in: int = 86400
) -> dict[str, Any]:
    if not validate_user(username, password):
        return False
    key = encrypt_payload(username)
    token = {
        "token": key,
        "generated_at": datetime.now(),
        "expired_at": datetime.now() + timedelta(seconds=expire_in),
    }
    return token


def update_token(username: str, token=str, expire_in: int = 86400) -> dict[str, Any]:
    updated_token = {
        "token": token,
        "generated_at": datetime.now(),
        "expired_at": datetime.now() + timedelta(seconds=expire_in),
    }
    return updated_token


def validate_token(
    username: str,
    token: str,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    issuer: Optional[str] = None,
) -> bool:
    if decrypt_payload(token) != username:
        return False
    return True


def validate_user(username: str, password: str, uid: Optional[str] = None) -> bool:
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
