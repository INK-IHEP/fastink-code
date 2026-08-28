from sqlalchemy.exc import NoResultFound, IntegrityError, DatabaseError
from typing import Any, Optional

from fastink.auth import common
from fastink.auth import permission
from fastink.auth.directory import verify_user_identity
from fastink.auth.backends.registry import get_auth_backend
from fastink.common.logger import logger
from fastink.common.config import get_config


def add_user(
    username: str, uid: Optional[int] = None, **kwargs
) -> bool:
    # check if user could be added
    if get_config("auth", "type") == "krb5":
        if uid is not None:
            # Site plugins may verify the identity against an external
            # directory (e.g. IHEP verifies against UMT). Verification is
            # opt-in via uid, mirroring the old email-triggered behavior.
            verify_user_identity(username=username, uid=uid)
    elif get_config("auth", "type") == "password":
        is_valid = get_auth_backend("password").validate_user(
            username=username, password=kwargs["password"], uid=uid
        )
        if not is_valid:
            raise ValueError("Invalid username or password")
    try:
        logger.debug(f"Checking user {username} with uid {uid}")
        common.add_user(username=username, uid=uid)
    except IntegrityError:
        logger.error("User already exists")
        raise ValueError("User already exists")

    # Add user default permissions
    permission.add_user_permission(username=username, permission="cpu")
    permission.add_user_permission(username=username, permission="AlmaLinux9")

    return True


def delete_user(
    username: Optional[str] = None,
    uid: Optional[int] = None,
) -> bool:
    try:
        user = common.get_user(username=username, uid=uid)
    except NoResultFound:
        return False
    try:
        common.delete_user_cascade(user_id=user["id"])
    except DatabaseError:
        return False
    return True


def update_user(
    username: Optional[str] = None,
    uid: Optional[int] = None,
    new_username: Optional[str] = None,
    new_uid: Optional[int] = None,
) -> bool:
    try:
        user = common.get_user(username=username, uid=uid)
    except NoResultFound:
        return False
    try:
        common.update_user(
            user_id=user["id"],
            username=new_username,
            uid=new_uid,
        )
    except IntegrityError:
        return False
    return True


def list_users() -> list[dict[str, Any]]:
    return common.get_users()


def get_user(
    username: Optional[str] = None,
    uid: Optional[int] = None,
) -> dict[str, Any]:
    if not str(username or "").strip() and uid is None:
        return {}
    try:
        user = common.get_user(username=str(username or "").strip(), uid=uid)
    except NoResultFound:
        return {}
    return user
