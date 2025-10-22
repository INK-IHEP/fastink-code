from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlalchemy.orm import Session
from typing import Any, Optional, Type, TypeVar, Union

from src.common.logger import logger
from src.database.sqla.models import ModelBase
from src.database.sqla.session import read_session, transactional_session

logger.debug("DatabaseService initialized with DEBUG level.")

T = TypeVar("T", bound=ModelBase)


@transactional_session
def g_create(obj: Union[T, list[T]], *, session: Session) -> None:
    """
    Create new record(s) and save it(them) to the database.

    :param obj: The object to be created.
    :param session: SQLAlchemy session (injected by the decorator).
    :return: The created object.
    """
    try:
        if isinstance(obj, list):
            for item in obj:
                item.save(session=session, flush=False)
                logger.debug(f"Created object: {item}")
            session.flush()
        else:
            obj.save(session=session, flush=True)
        logger.debug(f"Created object: {obj}")
    except IntegrityError as error:
        logger.error(f"Object already exists: {error}")
        raise IntegrityError(error)


@transactional_session
def g_update(
    obj: Union[T, list[T]],
    values: Union[dict, list[dict[str, Any]]],
    *,
    session: Session,
) -> None:
    """
    Update existing record(s).

    :param obj: The object to be updated.
    :param values: The fields and values to update.
    :param session: SQLAlchemy session (injected by the decorator).
    :return: The updated object.
    """
    try:
        if isinstance(obj, list) and isinstance(values, list):
            for item, value in zip(obj, values):
                item.update(value, session=session, flush=False)
                logger.debug(f"Updated object: {item}, with value: {value}")
            session.flush()
        else:
            obj.update(values, session=session, flush=True)
        logger.debug(f"Updated object: {obj}, with value: {values}")
    except IntegrityError as error:
        logger.error(f"Object should be unique: {error}")
        raise IntegrityError(error)


@transactional_session
def g_delete(obj: Union[T, list[T]], *, session: Session) -> None:
    """
    Delete record(s) from the database.

    :param obj: The object to be deleted.
    :param session: SQLAlchemy session (injected by the decorator).
    """
    try:
        if isinstance(obj, list):
            for item in obj:
                item.delete(session=session, flush=False)
                logger.debug(f"Deleted object: {item}")
                session.flush()
        else:
            obj.delete(session=session, flush=True)
            logger.info(f"Deleted object: {obj}")
    except NoResultFound as error:
        logger.error(f"Object cannot be found: {error}")
        raise NoResultFound(error)


@read_session
def g_get_all(
    model: Type[T], *, limit: int = 100, offset: int = 0, session: Session
) -> list[T]:
    """
    Retrieve all records, with optional pagination.

    :param model: The model class to query.
    :param limit: The maximum number of records to retrieve.
    :param offset: The offset for pagination.
    :param session: SQLAlchemy session (injected by the decorator).
    :return: A list of retrieved objects.
    """
    objs = session.query(model).offset(offset).limit(limit).all()
    logger.info(f"Retrieved all objects: {objs}")
    return objs


@read_session
def g_find(model: Type[T], *, session: Session, **filters) -> Optional[list[T]]:
    """
    Find records matching the specified filters.

    :param model: The model class to query.
    :param session: SQLAlchemy session (injected by the decorator).
    :param filters: The filters to apply (e.g., field=value).
    :return: A list of objects matching the filters.
    """
    try:
        objs = session.query(model).filter_by(**filters).all()
        logger.info(f"Retrieved objects with filters {filters}: {objs}")
        return objs
    except NoResultFound:
        logger.warning(f"No objects found with filters: {filters}")
        return None


@read_session
def g_find_one(model: Type[T], *, session: Session, **filters) -> Optional[T]:
    """
    Find a single record matching the specified filters.

    :param model: The model class to query.
    :param session: SQLAlchemy session (injected by the decorator).
    :param filters: The filters to apply (e.g., field=value).
    :return: The object matching the filters, or None if not found.
    """
    try:
        obj = session.query(model).filter_by(**filters).one()
        logger.info(f"Retrieved single object with filters {filters}: {obj}")
        return obj
    except NoResultFound:
        logger.warning(f"No object found with filters: {filters}")
        return None
