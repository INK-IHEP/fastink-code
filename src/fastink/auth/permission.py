from functools import wraps
from sqlalchemy.exc import IntegrityError, NoResultFound, DatabaseError
from typing import Optional, Callable

from fastink.auth import common
from fastink.auth.groups import get_user_groups
from fastink.auth.krb5 import get_krb5
from fastink.common.logger import logger
from fastink.common.utils import timer
from fastink.common.hooks import hookable


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
    """Delete a permission and all associated user_permission and group_permission records.

    This function first deletes all user_permission and group_permission records
    that reference this permission, then deletes the permission itself to avoid
    foreign key constraint violations.

    Returns:
        bool: True if deletion was successful, False otherwise
    """
    try:
        permission_id = common.get_permission(permission=permission)["id"]
    except NoResultFound:
        return False
    try:
        # First delete all user_permission and group_permission records
        common.delete_all_user_permissions_by_permission(permission_id=permission_id)
        common.delete_all_group_permissions_by_permission(permission_id=permission_id)
        # Then delete the permission itself
        common.delete_permission(permission_id=permission_id)
    except (IntegrityError, DatabaseError):
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
        user_record = common.get_user(username=username, email=email, uid=uid)
        user_id = user_record["id"]
        resolved_username = user_record["username"]
    except NoResultFound:
        raise NoResultFound("User not found")

    permissions = list()

    # Step 1: Collect direct user_permissions
    try:
        user_permissions = common.get_user_permissions(user_id=user_id)
        for up in user_permissions:
            permission_name = common.get_permission_name(up["permission_id"])
            permissions.append(permission_name)
    except NoResultFound:
        pass  # No direct permissions, continue to group check

    # Step 2: Collect group-based permissions (NEW)
    try:
        user_groups = get_user_groups(resolved_username)
    except Exception as e:
        logger.warning(
            "Failed to resolve groups for user %s: %s", resolved_username, e
        )
        user_groups = []

    for group_name in user_groups:
        try:
            group_perms = common.get_permissions_by_group_name(
                group_name=group_name
            )
        except (NoResultFound, DatabaseError):
            continue
        for perm_name in group_perms:
            if perm_name not in permissions:
                permissions.append(perm_name)

    # Stupid hack to make CentOS7 and AlmaLinux9 permissions appear first in the list
    if "AlmaLinux9" in permissions:
        permissions.remove("AlmaLinux9")
        permissions.insert(0, "AlmaLinux9")
    if "CentOS7" in permissions:
        permissions.remove("CentOS7")
        permissions.insert(0, "CentOS7")
    return permissions


def query_users_by_permission(permission: str) -> list:
    """Query all users who have a specific permission (direct OR via group).

    Raises NoResultFound if the permission does not exist.
    Returns an empty list if the permission exists but no users have it.
    """
    # Validate that the permission exists (raises NoResultFound if not)
    common.get_permission(permission=permission)

    # Step 1: Get users with direct permission (pure DB query)
    direct_users = common.get_users_by_permission(permission_name=permission)
    direct_ids = {u["id"] for u in direct_users}

    # Step 2: Get Linux groups that grant this permission
    group_names = common.get_group_names_by_permission(permission_name=permission)
    if not group_names:
        return direct_users

    # Step 3: For each user NOT already included, check their Linux groups
    all_users = common.get_users()
    result = list(direct_users)

    for user in all_users:
        if user["id"] in direct_ids:
            continue
        try:
            user_groups = get_user_groups(user["username"])
            if any(g in group_names for g in user_groups):
                result.append(user)
        except Exception as e:
            logger.warning(
                "Failed to resolve groups for user %s: %s",
                user["username"], e,
            )
            continue

    return result


@hookable
def check_user_permission(username: str, permission: str) -> bool:
    if not username or not permission:
        raise ValueError(
            f"username and permission must be non-empty strings, "
            f"got username={username!r}, permission={permission!r}"
        )

    user_id = common.get_user(username=username)["id"]
    perm_id = common.get_permission(permission=permission)["id"]

    # Step 1: Check direct user_permissions (existing logic)
    try:
        if common.get_user_permission(user_id=user_id, permission_id=perm_id):
            logger.debug(f"User {username} has direct permission {permission}")
            return True
    except NoResultFound:
        pass  # No direct permission, continue to group check

    # Step 2: Check group-based permissions (NEW)
    try:
        user_groups = get_user_groups(username)
    except Exception as e:
        logger.warning(
            "Failed to resolve groups for user %s: %s", username, e
        )
        user_groups = []

    if user_groups:
        try:
            group_names = common.get_group_names_by_permission(
                permission_name=permission
            )
        except (NoResultFound, DatabaseError) as e:
            logger.warning(
                "Failed to query group permissions for %s: %s", permission, e
            )
            group_names = []
        if any(g in group_names for g in user_groups):
            logger.debug(
                f"User {username} has permission {permission} via group membership"
            )
            return True

    logger.debug(f"User {username} does not have permission {permission}")
    return False


@hookable
def check_user_app(username: str, app: str) -> bool:
    if not username or not app:
        raise ValueError(
            f"username and app must be non-empty strings, "
            f"got username={username!r}, app={app!r}"
        )

    user_id = common.get_user(username=username)["id"]
    app_id = common.get_app(app=app)["id"]

    try:
        if common.get_user_app(user_id=user_id, app_id=app_id):
            logger.debug(f"User {username} has access to app {app}")
            return True
    except NoResultFound:
        pass

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
