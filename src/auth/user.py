import importlib
from sqlalchemy.exc import NoResultFound, IntegrityError, DatabaseError
from typing import Any, Optional

from src.auth import common
from src.auth import permission
from src.common.logger import logger
from src.common.utils import query_umt_uid, query_umt_username, query_umt_group
from src.common.config import get_config


AUTH_TYPE = get_config("auth", "type")
AUTH_PLUGIN = importlib.import_module(f"src.auth.plugins.{AUTH_TYPE}")

def add_user(
    username: str, email: Optional[str] = None, uid: Optional[int] = None, **kwargs
) -> bool:
    try:
        common.get_user(username=username, email=email, uid=uid)
    except NoResultFound:
        pass
    # check if user could be added
    if get_config("auth", "type") == "krb5":
        if email:
            username_umt = query_umt_username(email)
            uid_umt = query_umt_uid(email)
            if not username_umt:
                logger.error("Email not found in IHEP UMT, user does not exist")
                raise ValueError("Email not found in IHEP UMT, user does not exist")
            if username != username_umt:
                logger.error(
                    "Username does not match IHEP UMT username, please check your input"
                )
                raise ValueError(
                    "Username does not match IHEP UMT username, please check your input"
                )
            if uid and uid != int(uid_umt):
                logger.error("UID does not match IHEP UMT UID, please check your input")
                raise ValueError(
                    "UID does not match IHEP UMT UID, please check your input"
                )
    elif get_config("auth", "type") == "password":
        AUTH_PLUGIN.validate_user(username=username, password=kwargs["password"], uid=uid)
    try:
        logger.debug(
            f"Checking user {username} with email {email} and uid {uid} in IHEP UMT"
        )
        common.add_user(username=username, email=email, uid=uid)
    except IntegrityError:
        logger.error("User already exists")
        raise ValueError("User already exists")

    # Add user default permission
    permission.add_user_permission(username=username, permission="cpu")
    permission.add_user_permission(username=username, permission="AlmaLinux9")
    if query_umt_group(email) == "physics":
        permission.add_user_permission(username=username, permission="CentOS7")

    return True


def delete_user(
    username: Optional[str] = None,
    email: Optional[str] = None,
    uid: Optional[int] = None,
) -> bool:
    try:
        user = common.get_user(username=username, email=email, uid=uid)
    except NoResultFound:
        return False
    try:
        common.delete_user(user_id=user["id"])
    except DatabaseError:
        return False
    return True


def update_user(
    username: Optional[str] = None,
    email: Optional[str] = None,
    uid: Optional[int] = None,
    new_username: Optional[str] = None,
    new_email: Optional[str] = None,
    new_uid: Optional[int] = None,
) -> bool:
    try:
        user = common.get_user(username=username, email=email, uid=uid)
    except NoResultFound:
        return False
    try:
        common.update_user(
            user_id=user["id"],
            username=new_username,
            email=new_email,
            uid=new_uid,
        )
    except IntegrityError:
        return False


def list_users() -> list[dict[str, Any]]:
    try:
        users = common.get_users()
    except NoResultFound:
        return []
    return users


def get_user(
    username: Optional[str] = None,
    email: Optional[str] = None,
    uid: Optional[int] = None,
) -> dict[str, Any]:
    try:
        user = common.get_user(username=username, email=email, uid=uid)
    except NoResultFound:
        return {}
    return user
