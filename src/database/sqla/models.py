import uuid
from datetime import datetime
from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    PrimaryKeyConstraint,
    SMALLINT,
    String,
    TIMESTAMP,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import (
    Mapped,
    Session,
    declarative_base,
    mapped_column,
    object_mapper,
)
from typing import Any, Optional

from src.common.utils import generate_uuid
from src.database.sqla.types import GUID

BASE = declarative_base()


class ModelBase:
    """
    Base class for models. Provides created_at, updated_at columns and basic
    operations (save, delete, update, to_dict, keys, values, items).
    """

    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    def save(self, flush: bool = True, session: Optional[Session] = None) -> None:
        """
        Save the current object to the database session.
        """
        session.add(self)
        if flush:
            session.flush()

    def delete(self, flush: bool = True, session: Optional[Session] = None) -> None:
        """
        Delete the current object from the database session.
        """
        session.delete(self)
        if flush:
            session.flush()

    def update(
        self,
        values: dict[str, Any],
        flush: bool = True,
        session: Optional[Session] = None,
    ) -> None:
        """
        Update the current object with new values.
        """
        for k, v in values.items():
            setattr(self, k, v)
        if flush:
            session.flush()

    def to_dict(self):
        """
        Convert the object to a dictionary, excluding SQLAlchemy internal state.
        """
        dictionary = self.__dict__.copy()
        dictionary.pop("_sa_instance_state", None)
        return dictionary

    def keys(self):
        return list(self.__dict__.keys())

    def values(self):
        return list(self.__dict__.values())

    def items(self):
        return list(self.__dict__.items())

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __getitem__(self, key):
        return getattr(self, key)

    def __iter__(self):
        self._i = iter(object_mapper(self).columns)
        return self

    def __next__(self):
        n = next(self._i).name
        return n, getattr(self, n)

    next = __next__


class Users(BASE, ModelBase):
    __tablename__ = "users"
    id: Mapped[uuid.UUID] = mapped_column(
        GUID(), primary_key=True, default=generate_uuid
    )
    username: Mapped[str] = mapped_column(String(50), nullable=False, unique=True)
    email: Mapped[str] = mapped_column(String(255), nullable=True, unique=True)
    uid: Mapped[str] = mapped_column(Integer, nullable=True, unique=True)

    __table_args__ = (
        CheckConstraint("username IS NOT NULL", name="USERS_USERNAME_NN"),
        Index("USER_ID_IDX", "id"),
        Index("USERS_USERNAME_IDX", "username"),
    )


class Permissions(BASE, ModelBase):
    __tablename__ = "permissions"
    id: Mapped[uuid.UUID] = mapped_column(
        GUID(), primary_key=True, default=generate_uuid
    )
    permission: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)

    __table_args__ = (
        CheckConstraint("permission IS NOT NULL", name="PERMISSIONS_PERMISSION_NN"),
        Index("PERMISSION_ID_IDX", "id"),
        Index("PERMISSIONS_PERMISSION_IDX", "permission"),
    )


class Apps(BASE, ModelBase):
    __tablename__ = "apps"
    id: Mapped[uuid.UUID] = mapped_column(
        GUID(), primary_key=True, default=generate_uuid
    )
    app: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)

    __table_args__ = (
        CheckConstraint("app IS NOT NULL", name="APPS_APP_NN"),
        Index("APPS_ID_IDX", "id"),
        Index("APPS_APP_IDX", "app"),
    )


class Authorizations(BASE, ModelBase):
    __tablename__ = "authorizations"
    id: Mapped[uuid.UUID] = mapped_column(
        GUID(), primary_key=True, default=generate_uuid
    )
    authorization: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)

    __table_args__ = (
        CheckConstraint(
            "authorization IS NOT NULL", name="AUTHORIZATIONS_AUTHORIZATION_NN"
        ),
        Index("AUTHORIZATIONS_ID_IDX", "id"),
        Index("AUTHORIZATIONS_AUTHORIZATION_IDX", "authorization"),
    )


class UserPermissions(BASE, ModelBase):
    __tablename__ = "user_permissions"
    user_id: Mapped[uuid.UUID] = mapped_column(GUID(), nullable=False)
    permission_id: Mapped[uuid.UUID] = mapped_column(GUID(), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("user_id", "permission_id", name="USER_PERMISSIONS_PK"),
        ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name="USER_PERMISSIONS_USER_FK",
        ),
        ForeignKeyConstraint(
            ["permission_id"],
            ["permissions.id"],
            name="USER_PERMISSIONS_PERMISSION_FK",
        ),
        Index("USER_PERMISSIONS_USER_IDX", "user_id"),
        Index("USER_PERMISSIONS_PERMISSION_IDX", "permission_id"),
    )


class UserAuthorizations(BASE, ModelBase):
    __tablename__ = "user_authorizations"
    user_id: Mapped[uuid.UUID] = mapped_column(GUID(), nullable=False)
    authorization_id: Mapped[uuid.UUID] = mapped_column(GUID(), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint(
            "user_id", "authorization_id", name="USER_AUTHORIZATIONS_PK"
        ),
        ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name="USER_AUTHORIZATIONS_USER_FK",
        ),
        ForeignKeyConstraint(
            ["authorization_id"],
            ["authorizations.id"],
            name="USER_AUTHORIZATIONS_AUTHORIZATION_FK",
        ),
        Index("USER_AUTHORIZATIONS_USER_IDX", "user_id"),
        Index("USER_AUTHORIZATIONS_AUTHORIZATION_IDX", "authorization_id"),
    )


class UserApps(BASE, ModelBase):
    __tablename__ = "user_apps"
    user_id: Mapped[uuid.UUID] = mapped_column(GUID(), nullable=False)
    app_id: Mapped[uuid.UUID] = mapped_column(GUID(), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("user_id", "app_id", name="USER_APPS_PK"),
        ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name="USER_APPS_USER_FK",
        ),
        ForeignKeyConstraint(
            ["app_id"],
            ["apps.id"],
            name="USER_APPS_APP_FK",
        ),
        Index("USER_APPS_USER_IDX", "user_id"),
        Index("USER_APPS_APP_IDX", "app_id"),
    )


class KerberosTokens(BASE, ModelBase):
    __tablename__ = "kerberos_tokens"
    user_id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, nullable=False)
    token: Mapped[str] = mapped_column(String(2000), nullable=False)
    generated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    expired_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(["user_id"], ["users.id"], name="KERBEROS_TOKENS_USER_FK"),
        Index("KERBEROS_TOKENS_USER_IDX", "user_id"),
    )


class Authentications(BASE, ModelBase):
    __tablename__ = "authentications"
    id: Mapped[uuid.UUID] = mapped_column(
        GUID(), primary_key=True, default=generate_uuid
    )
    authentication: Mapped[str] = mapped_column(
        String(100), nullable=False, unique=True
    )

    __table_args__ = (
        CheckConstraint(
            "authentication IS NOT NULL", name="AUTHENTICATIONS_AUTHENTICATION_NN"
        ),
        Index("AUTHENTICATIONS_ID_IDX", "id"),
        Index("AUTHENTICATIONS_AUTHENTICATION_IDX", "authentication"),
    )


class Tokens(BASE, ModelBase):
    __tablename__ = "tokens"
    user_id: Mapped[uuid.UUID] = mapped_column(GUID(), nullable=False)
    authentication_id: Mapped[uuid.UUID] = mapped_column(GUID(), nullable=False)
    token: Mapped[str] = mapped_column(String(2000), nullable=False)
    generated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    expired_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("user_id", "authentication_id", name="TOKENS_PK"),
        ForeignKeyConstraint(["user_id"], ["users.id"], name="TOKENS_USER_FK"),
        ForeignKeyConstraint(
            ["authentication_id"],
            ["authentications.id"],
            name="TOKENS_AUTHENTICATION_FK",
        ),
        Index("TOKENS_USER_IDX", "user_id"),
        Index("TOKENS_AUTHENTICATION_IDX", "authentication_id"),
    )


class JobInfo(BASE, ModelBase):
    __tablename__ = "job_info_table"

    id: Mapped[Integer] = mapped_column(Integer, primary_key=True, autoincrement=True)
    uid: Mapped[str] = mapped_column(Integer, nullable=False)
    jobid: Mapped[str] = mapped_column(Integer, nullable=False)
    outpath: Mapped[str] = mapped_column(String(200), nullable=False)
    errpath: Mapped[str] = mapped_column(String(200), nullable=False)
    job_type: Mapped[str] = mapped_column(String(20))
    job_path: Mapped[str] = mapped_column(String(255))
    clusterid: Mapped[str] = mapped_column(String(32), nullable=False)
    job_status: Mapped[str] = mapped_column(String(20), nullable=False, server_default=text("'SUBMITTED'"))
    iptable_clean: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    iptable_status: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    connect_sign: Mapped[str] = mapped_column(String(20), nullable=False, server_default=text("'False'"))
    job_start_time: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    job_end_time:   Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)


    __table_args__ = (
        CheckConstraint("uid IS NOT NULL", name="JOBINFO_UID_NN"),
        CheckConstraint("jobid IS NOT NULL", name="JOBINFO_JOBID_NN"),
        UniqueConstraint("jobid", "clusterid", name="JOBID_CLUSTERID_UK"),
        Index("USER_ID_IDX", "uid"),
        Index("CLUSTER_ID_IDX", "clusterid"),
        Index("JOB_ID_IDX", "jobid"),
    )


class ShareLink(BASE, ModelBase):
    __tablename__ = "share_link"

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, nullable=False)
    file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    owner: Mapped[str] = mapped_column(String(255), nullable=False)
    file_path: Mapped[str] = mapped_column(String(255), nullable=False)
