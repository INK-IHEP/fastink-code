from datetime import datetime
from sqlalchemy.exc import (
    NoResultFound,
    IntegrityError,
    DatabaseError,
)
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import update, select, delete
from typing import Optional, Any

from src.database.sqla import models
from src.database.sqla.session import read_session, transactional_session
from src.common.logger import logger


@transactional_session
def add_user(
    username: str,
    email: Optional[str] = None,
    uid: Optional[int] = None,
    *,
    session: Session,
) -> bool:
    try:
        new_user = models.Users(
            username=username,
            email=email,
            uid=uid,
        )
        new_user.save(session=session, flush=True)
        return True
    except IntegrityError:
        raise


@read_session
def get_users(*, session: Session) -> list[dict[str, Any]]:
    stmt = select(models.Users)
    result = session.execute(stmt).scalars().all()
    return [user.to_dict() for user in result]


@read_session
def get_user(
    username: Optional[str] = None,
    email: Optional[str] = None,
    uid: Optional[int] = None,
    *,
    session: Session,
) -> Optional[dict[str, Any]]:
    logger.debug(f"what happend for {username}, {email}, {uid} ???")
    stmt = select(models.Users)
    if username:
        stmt = stmt.where(models.Users.username == username)
    if email:
        stmt = stmt.where(models.Users.email == email)
    if uid:
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
    email: Optional[str] = None,
    uid: Optional[int] = None,
    *,
    session: Session,
) -> bool:
    try:
        stmt = update(models.Users).where(models.Users.id == user_id)
        update_values = {}
        if username:
            update_values["username"] = username
        if email:
            update_values["email"] = email
        if uid:
            update_values["uid"] = uid

        if update_values:
            stmt = stmt.values(**update_values)
            session.execute(stmt)
            session.flush()
        else:
            raise DatabaseError("No user to update")
        return True
    except IntegrityError:
        raise IntegrityError("Updated user is duplicated with another existing user")


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
def add_authentication(authentication: str, *, session: Session) -> bool:
    try:
        new_authentication = models.Authentications(authentication=authentication)
        new_authentication.save(session=session, flush=True)
        return True
    except IntegrityError:
        raise IntegrityError("Authentication already exists")


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
def add_permission(permission: str, *, session: Session) -> bool:
    try:
        new_permission = models.Permissions(permission=permission)
        new_permission.save(session=session, flush=True)
        return True
    except IntegrityError:
        raise IntegrityError("Permission already exists")


@read_session
def get_permission(permission: str, *, session: Session) -> Optional[dict[str, Any]]:
    stmt = select(models.Permissions).where(models.Permissions.permission == permission)
    try:
        result = session.execute(stmt).scalar_one()
        return result.to_dict()
    except NoResultFound:
        raise NoResultFound("Permission not found")


@read_session
def get_permission_name(permission_id: str, *, session: Session) -> Optional[str]:
    stmt = select(models.Permissions).where(models.Permissions.id == permission_id)
    try:
        result = session.execute(stmt).scalar_one()
        return result.permission
    except NoResultFound:
        raise NoResultFound("Permission not found")


@transactional_session
def update_permission(
    permission_id: str, permission: Optional[str] = None, *, session: Session
) -> bool:
    try:
        stmt = update(models.Permissions).where(models.Permissions.id == permission_id)
        update_valuse = {}
        if permission:
            update_valuse["permission"] = permission

        if update_valuse:
            stmt = stmt.values(**update_valuse)
            session.execute(stmt)
            session.flush()
        return True
    except IntegrityError:
        raise IntegrityError(
            "Updated permission is duplicated with another existing permission"
        )


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
def add_app(app_name: str, *, session: Session) -> bool:
    try:
        new_app = models.Apps(app=app_name)
        new_app.save(session=session, flush=True)
        return True
    except IntegrityError:
        raise IntegrityError("App already exists")


@read_session
def get_app(app_name: Optional[str], *, session: Session) -> Optional[dict[str, Any]]:
    stmt = select(models.Apps)
    if app_name:
        stmt = stmt.where(models.Apps.app == app_name)
    try:
        result = session.execute(stmt).scalar_one()
        return result.to_dict()
    except NoResultFound:
        raise NoResultFound("App not found")


@transactional_session
def update_app(
    app_id: str, app_name: Optional[str] = None, *, session: Session
) -> bool:
    try:
        stmt = update(models.Apps).where(models.Apps.id == app_id)
        update_values = {}
        if app_name:
            update_values["app"] = app_name

        if update_values:
            stmt = stmt.values(**update_values)
            session.execute(stmt)
            session.flush()
        return True
    except IntegrityError:
        raise IntegrityError("Updated app is duplicated with another existing app")


@transactional_session
def delete_app(app_id: str, *, session: Session) -> bool:
    try:
        stmt = delete(models.Apps).where(models.Apps.id == app_id)
        session.execute(stmt)
        session.flush()
        return True
    except DatabaseError:
        raise DatabaseError("Failed to delete app")


@transactional_session
def add_authorization(authorization: str, *, session: Session) -> bool:
    try:
        new_auth = models.Authorizations(authorization=authorization)
        new_auth.save(session=session, flush=True)
        return True
    except IntegrityError:
        raise IntegrityError("Authorization already exists")


@read_session
def get_authorization(
    authorization: Optional[str], *, session: Session
) -> Optional[dict[str, Any]]:
    stmt = select(models.Authorizations)
    if authorization:
        stmt = stmt.where(models.Authorizations.authorization == authorization)
    try:
        result = session.execute(stmt).scalar_one()
        return result.to_dict()
    except NoResultFound:
        raise NoResultFound("Authorization not found")


@transactional_session
def update_authorization(
    authorization_id: str, authorization: Optional[str] = None, *, session: Session
) -> bool:
    try:
        stmt = update(models.Authorizations).where(
            models.Authorizations.id == authorization_id
        )
        update_values = {}
        if authorization:
            update_values["authorization"] = authorization

        if update_values:
            stmt = stmt.values(**update_values)
            session.execute(stmt)
            session.flush()
        return True
    except IntegrityError:
        raise IntegrityError(
            "Updated authorization is duplicated with another existing authorization"
        )


@transactional_session
def delete_authorization(authorization_id: str, *, session: Session) -> bool:
    try:
        stmt = delete(models.Authorizations).where(
            models.Authorizations.id == authorization_id
        )
        session.execute(stmt)
        session.flush()
        return True
    except DatabaseError:
        raise DatabaseError("Failed to delete authorization")


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
            f"Adding token for user_id: {user_id}, token: {token}, auth_id: {authentication_id}, generated_at: {generated_at}, expired_at: {expired_at}"
        )
        new_token = models.Tokens(
            user_id=user_id,
            authentication_id=authentication_id,
            token=token,
            generated_at=generated_at,
            expired_at=expired_at,
        )
        logger.debug("abcdef")
        new_token.save(session=session, flush=True)
        logger.debug("abcdefghi")
        return True
    except IntegrityError:
        raise IntegrityError("Token already exists")


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

        if update_values:
            stmt = stmt.values(**update_values)
            session.execute(stmt)
            session.flush()
        return True
    except IntegrityError:
        raise IntegrityError("Updated token is duplicated with another existing token")


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
        raise IntegrityError("Kerberos token already exists")


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

        if update_values:
            stmt = stmt.values(**update_values)
            session.execute(stmt)
            session.flush()
        return True
    except IntegrityError:
        raise IntegrityError(
            "Updated kerberos token is duplicated with another existing kerberos token"
        )


@transactional_session
def delete_kerberos_token(user_id: str, *, session: Session) -> bool:
    try:
        stmt = delete(models.KerberosTokens).where(
            models.KerberosTokens.user_id == user_id
        )
        session.execute(stmt)
        session.flush()
        return True
    except DatabaseError:
        raise DatabaseError("Failed to delete kerberos token")


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
        raise IntegrityError("User permission already exists")


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


@transactional_session
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
def add_user_app(user_id: str, app_id: str, *, session: Session) -> bool:
    try:
        new_user_app = models.UserApps(
            user_id=user_id,
            app_id=app_id,
        )
        new_user_app.save(session=session, flush=True)
        return True
    except IntegrityError:
        raise IntegrityError("User app already exists")


@read_session
def get_user_app(
    user_id: str, app_id: str, *, session: Session
) -> Optional[dict[str, Any]]:
    stmt = select(models.UserApps).where(
        models.UserApps.user_id == user_id,
        models.UserApps.app_id == app_id,
    )
    try:
        result = session.execute(stmt).scalar_one()
        return result.to_dict()
    except NoResultFound:
        raise NoResultFound("User app not found")


@transactional_session
def delete_user_app(user_id: str, app_id: str, *, session: Session) -> bool:
    try:
        stmt = delete(models.UserApps).where(
            models.UserApps.user_id == user_id,
            models.UserApps.app_id == app_id,
        )
        session.execute(stmt)
        session.flush()
        return True
    except DatabaseError:
        raise DatabaseError("Failed to delete user app")


@transactional_session
def add_user_authorization(
    user_id: str, authorization_id: str, *, session: Session
) -> bool:
    try:
        new_user_authorization = models.UserAuthorizations(
            user_id=user_id,
            authorization_id=authorization_id,
        )
        new_user_authorization.save(session=session, flush=True)
        return True
    except IntegrityError:
        raise IntegrityError("User authorization already exists")


@read_session
def get_user_authorization(
    user_id: str, authorization_id: str, *, session: Session
) -> Optional[dict[str, Any]]:
    stmt = select(models.UserAuthorizations).where(
        models.UserAuthorizations.user_id == user_id,
        models.UserAuthorizations.authorization_id == authorization_id,
    )
    try:
        result = session.execute(stmt).scalar_one()
        return result.to_dict()
    except NoResultFound:
        raise NoResultFound("User authorization not found")


@transactional_session
def delete_user_authorization(
    user_id: str, authorization_id: str, *, session: Session
) -> bool:
    try:
        stmt = delete(models.UserAuthorizations).where(
            models.UserAuthorizations.user_id == user_id,
            models.UserAuthorizations.authorization_id == authorization_id,
        )
        session.execute(stmt)
        session.flush()
        return True
    except DatabaseError:
        raise DatabaseError("Failed to delete user authorization")
