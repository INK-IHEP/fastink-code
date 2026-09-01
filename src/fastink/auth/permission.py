from typing import Optional

from sqlalchemy.exc import IntegrityError, NoResultFound, DatabaseError

from fastink.auth import common, groups
from fastink.auth.groups import get_user_groups
from fastink.common.logger import logger
from fastink.common.utils import timer
from fastink.common.hooks import hookable


def add_permission(permission: str) -> bool:
    try:
        common.get_permission(permission=permission)
        return False
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


def can_remove_group_admin_source(caller: Optional[str], group_name: str) -> bool:
    if not caller:
        return False
    try:
        user_record = common.get_user(username=caller)
        admin_record = common.get_permission(permission="admin")
    except Exception as error:
        logger.debug("Admin source lookup failed for %s: %s", caller, error)
        return False
    try:
        direct_permission = common.get_user_permission(
            user_id=user_record["id"],
            permission_id=admin_record["id"],
        )
    except NoResultFound:
        direct_permission = None
    except Exception as error:
        logger.debug("Direct admin lookup failed for %s: %s", caller, error)
        return False
    if direct_permission:
        return True
    try:
        return group_name.strip().casefold() not in {
            group.strip().casefold() for group in get_user_groups(caller)
        }
    except Exception as error:
        logger.debug("Group admin source lookup failed for %s: %s", caller, error)
        return False


def get_users_by_permission_including_groups(
    permission_name: str,
) -> list[dict]:
    common.get_permission(permission=permission_name)
    direct_users = common.get_users_by_permission(permission_name=permission_name)
    direct_ids = {record["id"] for record in direct_users}
    group_names = common.get_group_names_by_permission(permission_name=permission_name)
    if not group_names:
        return direct_users

    candidates = [
        record for record in common.get_users() if record["id"] not in direct_ids
    ]
    if not candidates:
        return direct_users

    groups_map = groups.get_users_groups(
        [record["username"] for record in candidates]
    )
    granting_groups = set(group_names)
    return direct_users + [
        record
        for record in candidates
        if granting_groups.intersection(groups_map.get(record["username"], []))
    ]


@hookable
@timer
def query_user_permissions(
    username: str = None, uid: str = None
) -> list:
    try:
        user_record = common.get_user(username=username, uid=uid)
        user_id = user_record["id"]
        resolved_username = user_record["username"]
    except NoResultFound:
        raise NoResultFound("User not found")

    permissions = list()

    # Step 1: Collect direct user_permissions
    try:
        user_permissions = common.get_user_permissions(user_id=user_id)
        permission_names = common.get_permission_names_by_ids(
            permission_ids=[up["permission_id"] for up in user_permissions]
        )
        for up in user_permissions:
            permission_id = up["permission_id"]
            if permission_id not in permission_names:
                break
            permissions.append(permission_names[permission_id])
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

    try:
        group_permissions = common.get_permissions_by_group_names(
            group_names=user_groups
        )
    except (NoResultFound, DatabaseError):
        group_permissions = {}

    for group_name in user_groups:
        group_perms = group_permissions.get(group_name, [])
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
