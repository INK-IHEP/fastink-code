from datetime import datetime
from sqlalchemy.exc import (
    NoResultFound,
    IntegrityError,
    DatabaseError,
)
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import update, select, delete
from typing import Optional, Any

from fastink.database.sqla import models
from fastink.database.sqla.session import read_session, transactional_session
from fastink.common.logger import logger


@transactional_session
def add_user(
    username: str,
    uid: Optional[int] = None,
    *,
    session: Session,
) -> bool:
    new_user = models.Users(
        username=username,
        uid=uid,
    )
    new_user.save(session=session, flush=True)
    return True


@read_session
def get_users(*, session: Session) -> list[dict[str, Any]]:
    stmt = select(models.Users)
    result = session.execute(stmt).scalars().all()
    return [user.to_dict() for user in result]


@read_session
def get_users_by_permission(
    permission_name: str, *, session: Session
) -> list[dict[str, Any]]:
    """Return all users who hold a specific permission (by permission string name).

    Returns an empty list if the permission exists but no users have it.
    """
    stmt = (
        select(models.Users)
        .join(
            models.UserPermissions,
            models.Users.id == models.UserPermissions.user_id,
        )
        .join(
            models.Permissions,
            models.UserPermissions.permission_id == models.Permissions.id,
        )
        .where(models.Permissions.permission == permission_name)
    )
    result = session.execute(stmt).scalars().all()
    return [user.to_dict() for user in result]


@read_session
def get_user(
    username: Optional[str] = None,
    uid: Optional[int] = None,
    *,
    session: Session,
) -> Optional[dict[str, Any]]:
    normalized_username = str(username or "").strip()
    if not normalized_username and uid is None:
        raise ValueError("get_user requires a non-empty username or a uid")
    stmt = select(models.Users)
    if normalized_username:
        stmt = stmt.where(models.Users.username == normalized_username)
    if uid is not None:
        stmt = stmt.where(models.Users.uid == uid)
    try:
        result = session.execute(stmt).scalar_one()
        return result.to_dict()
    except NoResultFound:
        raise NoResultFound("User not found")


@transactional_session
def update_user(
    user_id: str,
    username: Optional[str] = None,
    uid: Optional[int] = None,
    *,
    session: Session,
) -> bool:
    try:
        stmt = update(models.Users).where(models.Users.id == user_id)
        update_values = {}
        if username:
            update_values["username"] = username
        if uid is not None:
            update_values["uid"] = uid

        if update_values:
            stmt = stmt.values(**update_values)
            session.execute(stmt)
            session.flush()
        else:
            raise DatabaseError("No user to update")
        return True
    except IntegrityError:
        raise


@transactional_session
def delete_user(user_id: str, *, session: Session) -> bool:
    try:
        stmt = delete(models.Users).where(models.Users.id == user_id)
        session.execute(stmt)
        session.flush()
        return True
    except DatabaseError:
        raise DatabaseError("Failed to delete user")


@transactional_session
def delete_user_cascade(user_id: str, *, session: Session) -> bool:
    for model in (
        models.UserPermissions,
        models.KerberosTokens,
        models.Tokens,
    ):
        session.execute(delete(model).where(model.user_id == user_id))
    session.execute(delete(models.Users).where(models.Users.id == user_id))
    session.flush()
    return True


@transactional_session
def add_authentication(authentication: str, *, session: Session) -> bool:
    try:
        new_authentication = models.Authentications(authentication=authentication)
        new_authentication.save(session=session, flush=True)
        return True
    except IntegrityError:
        raise


@read_session
def get_authentication(
    authentication: str, *, session: Session
) -> Optional[dict[str, Any]]:
    stmt = select(models.Authentications).where(
        models.Authentications.authentication == authentication
    )
    try:
        result = session.execute(stmt).scalar_one()
        return result.to_dict()
    except NoResultFound:
        raise NoResultFound("Authentication not found")


@transactional_session
def delete_authentication(authentication_id: str, *, session: Session) -> bool:
    session.execute(
        delete(models.Authentications).where(
            models.Authentications.id == authentication_id
        )
    )
    session.flush()
    return True


@transactional_session
def add_permission(permission: str, *, session: Session) -> bool:
    try:
        new_permission = models.Permissions(permission=permission)
        new_permission.save(session=session, flush=True)
        return True
    except IntegrityError:
        raise


@read_session
def get_permission(permission: str, *, session: Session) -> Optional[dict[str, Any]]:
    stmt = select(models.Permissions).where(models.Permissions.permission == permission)
    try:
        result = session.execute(stmt).scalar_one()
        return result.to_dict()
    except NoResultFound:
        raise NoResultFound("Permission not found")


@read_session
def get_permission_names_by_ids(permission_ids: list, *, session: Session) -> dict:
    stmt = select(models.Permissions).where(
        models.Permissions.id.in_(permission_ids)
    )
    result = session.execute(stmt).scalars().all()
    return {permission.to_dict()["id"]: permission.permission for permission in result}


@read_session
def get_all_permissions(*, session: Session) -> list[dict[str, Any]]:
    stmt = select(models.Permissions)
    result = session.execute(stmt).scalars().all()
    return [i.to_dict() for i in result]


@transactional_session
def delete_permission(permission_id: str, *, session: Session) -> bool:
    try:
        stmt = delete(models.Permissions).where(models.Permissions.id == permission_id)
        session.execute(stmt)
        session.flush()
        return True
    except DatabaseError:
        raise DatabaseError("Failed to delete permission")


@transactional_session
def add_token(
    user_id: str,
    authentication_id: str,
    token: str,
    generated_at: datetime,
    expired_at: datetime,
    *,
    session: Session,
) -> bool:
    try:
        logger.debug(
            f"Adding token for user_id: {user_id}, auth_id: {authentication_id}, "
            f"generated_at: {generated_at}, expired_at: {expired_at}"
        )
        new_token = models.Tokens(
            user_id=user_id,
            authentication_id=authentication_id,
            token=token,
            generated_at=generated_at,
            expired_at=expired_at,
        )
        new_token.save(session=session, flush=True)
        return True
    except IntegrityError:
        raise


def save_token(
    user_id: str,
    token: str,
    generated_at: datetime,
    expired_at: datetime,
    authentication_id: Optional[str] = None,
) -> bool:
    # ponytail: single get-then-add/update path for both token families
    try:
        if authentication_id is None:
            get_kerberos_token(user_id=user_id)
        else:
            get_token(user_id=user_id)
    except Exception:
        try:
            if authentication_id is None:
                add_kerberos_token(
                    user_id=user_id,
                    token=token,
                    generated_at=generated_at,
                    expired_at=expired_at,
                )
            else:
                add_token(
                    user_id=user_id,
                    authentication_id=authentication_id,
                    token=token,
                    generated_at=generated_at,
                    expired_at=expired_at,
                )
        except Exception:
            raise Exception(f"Failed to save token for user_id={user_id}")
    else:
        try:
            if authentication_id is None:
                update_kerberos_token(
                    user_id=user_id,
                    token=token,
                    generated_at=generated_at,
                    expired_at=expired_at,
                )
            else:
                update_token(
                    user_id=user_id,
                    authentication_id=authentication_id,
                    token=token,
                    generated_at=generated_at,
                    expired_at=expired_at,
                )
        except Exception:
            raise Exception(f"Failed to save token for user_id={user_id}")
    return True


@read_session
def get_token(
    user_id: Optional[str] = None, *, session: Session
) -> Optional[dict[str, Any]]:
    stmt = select(models.Tokens)
    if user_id:
        stmt = stmt.where(models.Tokens.user_id == user_id)
    try:
        result = session.execute(stmt).scalar_one()
        return result.to_dict()
    except NoResultFound:
        raise NoResultFound("Token not found")


@transactional_session
def update_token(
    user_id: str,
    authentication_id: str,
    token: str,
    generated_at: datetime,
    expired_at: datetime,
    *,
    session: Session,
) -> bool:
    try:
        stmt = update(models.Tokens).where(
            models.Tokens.user_id == user_id,
            models.Tokens.authentication_id == authentication_id,
        )
        update_values = {}
        update_values["token"] = token
        update_values["generated_at"] = generated_at
        update_values["expired_at"] = expired_at

        stmt = stmt.values(**update_values)
        session.execute(stmt)
        session.flush()
        return True
    except IntegrityError:
        raise


@transactional_session
def add_kerberos_token(
    user_id: str,
    token: str,
    generated_at: datetime,
    expired_at: datetime,
    *,
    session: Session,
) -> bool:
    try:
        new_token = models.KerberosTokens(
            user_id=user_id,
            token=token,
            generated_at=generated_at,
            expired_at=expired_at,
        )
        new_token.save(session=session, flush=True)
        return True
    except IntegrityError:
        raise


@read_session
def get_kerberos_token(
    user_id: Optional[str] = None,
    *,
    session: Session,
) -> Optional[dict[str, Any]]:
    stmt = select(models.KerberosTokens)
    if user_id:
        stmt = stmt.where(models.KerberosTokens.user_id == user_id)
    try:
        result = session.execute(stmt).scalar_one()
        return result.to_dict()
    except NoResultFound:
        raise NoResultFound("Kerberos token not found")


@transactional_session
def update_kerberos_token(
    user_id: str,
    token: str,
    generated_at: datetime,
    expired_at: datetime,
    *,
    session: Session,
) -> bool:
    try:
        stmt = update(models.KerberosTokens).where(
            models.KerberosTokens.user_id == user_id
        )
        update_values = {}
        update_values["token"] = token
        update_values["generated_at"] = generated_at
        update_values["expired_at"] = expired_at

        stmt = stmt.values(**update_values)
        session.execute(stmt)
        session.flush()
        return True
    except IntegrityError:
        raise


@transactional_session
def add_user_permission(user_id: str, permission_id: str, *, session: Session) -> bool:
    try:
        new_user_permission = models.UserPermissions(
            user_id=user_id,
            permission_id=permission_id,
        )
        new_user_permission.save(session=session, flush=True)
        return True
    except IntegrityError:
        raise


@read_session
def get_user_permission(
    user_id: str, permission_id: str, *, session: Session
) -> Optional[dict[str, Any]]:
    stmt = select(models.UserPermissions).where(
        models.UserPermissions.user_id == user_id,
        models.UserPermissions.permission_id == permission_id,
    )
    try:
        result = session.execute(stmt).scalar_one()
        return result.to_dict()
    except NoResultFound:
        raise NoResultFound("User permission not found")


@read_session
def get_user_permissions(
    user_id: str, *, session: Session
) -> Optional[list[dict[str, Any]]]:
    stmt = select(models.UserPermissions).where(
        models.UserPermissions.user_id == user_id
    )
    try:
        result = session.execute(stmt).scalars().all()
    except NoResultFound:
        raise NoResultFound("User permission not found")
    result = [i.to_dict() for i in result]
    if not result:
        raise NoResultFound("No Permission found")
    return result


@transactional_session
def delete_user_permission(
    user_id: str, permission_id: str, *, session: Session
) -> bool:
    try:
        stmt = delete(models.UserPermissions).where(
            models.UserPermissions.user_id == user_id,
            models.UserPermissions.permission_id == permission_id,
        )
        session.execute(stmt)
        session.flush()
        return True
    except DatabaseError:
        raise DatabaseError("Failed to delete user permission")


@transactional_session
def delete_all_user_permissions_by_permission(
    permission_id: str, *, session: Session
) -> int:
    """Delete all user_permission records associated with a permission.

    Returns the number of records deleted.
    """
    try:
        stmt = delete(models.UserPermissions).where(
            models.UserPermissions.permission_id == permission_id
        )
        result = session.execute(stmt)
        session.flush()
        return result.rowcount
    except DatabaseError:
        raise DatabaseError("Failed to delete user permissions by permission")


# ---------------------------------------------------------------------------
# Group-Permission mapping DAL
# ---------------------------------------------------------------------------


@transactional_session
def add_group_permission(
    group_name: str, permission_id: str, *, session: Session
) -> bool:
    """Map a Linux group to a permission.

    Raises sqlalchemy.exc.IntegrityError if the mapping already exists.
    """
    new_mapping = models.GroupPermissions(
        group_name=group_name,
        permission_id=permission_id,
    )
    new_mapping.save(session=session, flush=True)
    return True


@transactional_session
def delete_group_permission(
    group_name: str, permission_id: str, *, session: Session
) -> bool:
    """Remove a group-to-permission mapping."""
    try:
        stmt = delete(models.GroupPermissions).where(
            models.GroupPermissions.group_name == group_name,
            models.GroupPermissions.permission_id == permission_id,
        )
        session.execute(stmt)
        session.flush()
        return True
    except DatabaseError:
        raise DatabaseError("Failed to delete group permission")


@transactional_session
def delete_all_group_permissions_by_permission(
    permission_id: str, *, session: Session
) -> int:
    """Delete all group_permission records associated with a permission.

    Returns the number of records deleted.
    """
    try:
        stmt = delete(models.GroupPermissions).where(
            models.GroupPermissions.permission_id == permission_id
        )
        result = session.execute(stmt)
        session.flush()
        return result.rowcount
    except DatabaseError:
        raise DatabaseError("Failed to delete group permissions by permission")


# —— Reverse-lookup functions ——


@read_session
def get_permissions_by_group_names(group_names: list, *, session: Session) -> dict:
    stmt = (
        select(models.GroupPermissions.group_name, models.Permissions.permission)
        .join(
            models.Permissions,
            models.GroupPermissions.permission_id == models.Permissions.id,
        )
        .where(models.GroupPermissions.group_name.in_(group_names))
    )
    result = session.execute(stmt).all()
    permissions_by_group = {}
    for row in result:
        permissions_by_group.setdefault(row[0], []).append(row[1])
    return permissions_by_group


@read_session
def get_group_names_by_permission(
    permission_name: str, *, session: Session
) -> list[str]:
    """Return Linux group names that grant a given permission.

    Returns empty list if no groups grant this permission.
    """
    stmt = (
        select(models.GroupPermissions.group_name)
        .join(
            models.Permissions,
            models.Permissions.id == models.GroupPermissions.permission_id,
        )
        .where(models.Permissions.permission == permission_name)
    )
    result = session.execute(stmt).scalars().all()
    return list(result)


@read_session
def get_all_group_permissions(
    *, session: Session
) -> list[dict[str, Any]]:
    """Return all group-permission mappings with resolved permission names."""
    stmt = (
        select(
            models.GroupPermissions.group_name,
            models.Permissions.permission,
        )
        .join(
            models.Permissions,
            models.Permissions.id == models.GroupPermissions.permission_id,
        )
        .order_by(models.GroupPermissions.group_name, models.Permissions.permission)
    )
    result = session.execute(stmt).all()
    return [{"group_name": r[0], "permission": r[1]} for r in result]
