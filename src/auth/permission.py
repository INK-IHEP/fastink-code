from functools import wraps
from sqlalchemy.exc import IntegrityError, NoResultFound, DatabaseError
from typing import Optional, Callable

from src.auth import common
from src.auth.krb5 import get_krb5
from src.common.logger import logger
from src.common.utils import timer


def add_permission(permission: str) -> bool:
    try:
        common.get_permission(permission=permission)
    except NoResultFound:
        pass
    try:
        common.add_permission(permission=permission)
    except IntegrityError:
        return False
    return True


def delete_permission(permission: str) -> bool:
    try:
        permission_id = common.get_permission(permission=permission)["id"]
    except NoResultFound:
        return False
    try:
        common.delete_permission(permission_id=permission_id)
    except IntegrityError:
        return False
    return True


def add_user_permission(username: str, permission: str) -> bool:
    try:
        user_id = common.get_user(username=username)["id"]
    except NoResultFound:
        return False
    try:
        permission_id = common.get_permission(permission=permission)["id"]
    except NoResultFound:
        return False
    try:
        common.add_user_permission(user_id=user_id, permission_id=permission_id)
    except IntegrityError:
        return False
    return True


def delete_user_permission(username: str, permission: str) -> bool:
    try:
        user_id = common.get_user(username=username)["id"]
    except NoResultFound:
        return False
    try:
        permission_id = common.get_permission(permission=permission)["id"]
    except NoResultFound:
        return False
    try:
        common.delete_user_permission(user_id=user_id, permission_id=permission_id)
    except DatabaseError:
        return False
    return True


@timer
def query_user_permissions(
    username: str = None, email: str = None, uid: str = None
) -> list:
    try:
        user_id = common.get_user(username=username, email=email, uid=uid)["id"]
    except Exception:
        raise Exception("User not found")
    try:
        user_permissions = common.get_user_permissions(user_id=user_id)
    except:
        return []
    permissions = list()
    for user_permission in user_permissions:
        permission_name = common.get_permission_name(user_permission["permission_id"])
        permissions.append(permission_name)
    # Stupid hack to make CentOS7 and AlmaLinux9 permissions appear first in the list
    if "AlmaLinux9" in permissions:
        permissions.remove("AlmaLinux9")
        permissions.insert(0, "AlmaLinux9")
    if "CentOS7" in permissions:
        permissions.remove("CentOS7")
        permissions.insert(0, "CentOS7")
    return permissions


def check_user_permission(username: str, permission: str) -> bool:
    user_id = common.get_user(username=username)["id"]
    perm_id = common.get_permission(permission=permission)["id"]

    if common.get_user_permission(user_name=user_id, permission_id=perm_id):
        logger.debug(f"User {username} has permission {permission}")
        return True
    else:
        logger.debug(f"User {username} does not have permission {permission}")
        return False


def check_user_app(username: str, app: str) -> bool:
    user_id = common.get_user(username=username)["id"]
    app_id = common.get_app(app=app)["id"]

    if common.get_user_app(user_name=user_id, app_id=app_id):
        logger.debug(f"User {username} has access to app {app}")
        return True
    else:
        logger.debug(f"User {username} does not have access to app {app}")
        return False


def has_permission(
    user: Optional[str] = None,
    permission: Optional[str] = None,
    token_flag: bool = False,
) -> Callable:
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger.debug(f"args: {args}, kwargs: {kwargs}")
            permission_name = func.__name__
            if permission:
                permission_name = permission
            elif "permission" in kwargs:
                permission_name = kwargs["permission"]

            if user:
                user_name = user
            elif "user" in kwargs:
                user_name = kwargs["user"]
            else:
                raise ValueError("User name is required")

            if check_user_permission(username=user_name, permission=permission_name):
                if token_flag:
                    token = get_krb5(username=user_name)
                    kwargs["token"] = token
            else:
                raise PermissionError(
                    f"User {user_name} does not have permission {permission_name}"
                )

            return func(*args, **kwargs)

        return wrapper

    return decorator


def has_app(
    user: Optional[str] = None,
    app: Optional[str] = None,
) -> Callable:
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            app_name = func.__name__
            if app:
                app_name = app
            elif "app" in kwargs:
                app_name = kwargs["app"]

            if user:
                user_name = user
            elif "user" in kwargs:
                user_name = kwargs["user"]
            else:
                raise ValueError("User name is required")

            if check_user_app(user_name, app_name):
                return func(*args, **kwargs)
            else:
                raise PermissionError(f"User {user_name} does not have app {app_name}")

        return wrapper

    return decorator
