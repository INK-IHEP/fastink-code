from sqlalchemy.exc import NoResultFound, IntegrityError, DatabaseError
from typing import Any, Optional

from fastink.auth import common
from fastink.auth import permission
from fastink.auth.directory import verify_user_identity
from fastink.auth.backends.registry import get_auth_backend
from fastink.common.logger import logger
from fastink.common.config import get_config


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
            # Site plugins may verify the identity against an external
            # directory (e.g. IHEP verifies against UMT).
            verify_user_identity(username=username, email=email, uid=uid)
    elif get_config("auth", "type") == "password":
        get_auth_backend("password").validate_user(
            username=username, password=kwargs["password"], uid=uid
        )
    try:
        logger.debug(
            f"Checking user {username} with email {email} and uid {uid}"
        )
        common.add_user(username=username, email=email, uid=uid)
    except IntegrityError:
        logger.error("User already exists")
        raise ValueError("User already exists")

    # Add user default permissions
    permission.add_user_permission(username=username, permission="cpu")
    permission.add_user_permission(username=username, permission="AlmaLinux9")

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
