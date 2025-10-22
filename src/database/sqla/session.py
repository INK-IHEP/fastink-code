import time

from functools import wraps
from sqlalchemy import create_engine, exc, text
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
from typing import Optional

from src.common.config import get_config
from src.common.logger import logger

# Global session factory and engine
_ENGINE = None
_SESSION = None


# Get or initialize the database engine
def get_engine(
    pool_size: int = 20,
    max_overflow: int = 20,
    pool_timeout: int = 30,
    sql_connection: Optional[str] = None,
) -> Engine:
    """
    Get or initialize the database engine, supports only MySQL.

    :param sql_connection: Database connection string, e.g., "mysql+pymysql://user:password@host:port/database"
    :param pool_size: Connection pool size.
    :param max_overflow: Maximum overflow connections.
    :param pool_timeout: Timeout for retrieving connections from the pool.
    :return: SQLAlchemy Engine instance.
    """
    global _ENGINE, _SESSION
    host = get_config("database", "host")
    port = get_config("database", "port")
    user = get_config("database", "user")
    password = get_config("database", "password")
    database = get_config("database", "dbname", fallback="inkdb")
    if not sql_connection:
        sql_connection = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    if _ENGINE is None:
        _ENGINE = create_engine(
            sql_connection,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_pre_ping=True,  # Enable MySQL connection keep-alive
        )

        # Configure thread-safe sessions
        _SESSION = scoped_session(
            sessionmaker(
                bind=_ENGINE,
                autocommit=False,
                autoflush=True,
                expire_on_commit=True,
                future=True,
            )
        )
        logger.info("MySQL database engine initialized.")
    return _ENGINE


# Get a session
def get_session() -> scoped_session:
    """
    Retrieve the current scoped session, creating a new one if necessary.

    :return: A scoped session instance.
    """
    global _SESSION
    if not _SESSION:
        logger.warning("Session is not initialized. Initializing engine...")
        get_engine()  # Initialize engine and Session if not already initialized
    return _SESSION


# Decorator: Read-only session
def read_session(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        session_scoped = get_session()
        session = session_scoped()
        session.begin()
        try:
            result = func(*args, session=session, **kwargs)
            return result
        finally:
            session_scoped.remove()  # Release the thread-bound session

    return wrapper


# Decorator: Transactional session
def transactional_session(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        session_scoped = get_session()
        session = session_scoped()
        session.begin()
        try:
            result = func(*args, session=session, **kwargs)
            session.commit()
            return result
        except Exception as e:
            session.rollback()
            logger.error(f"Transaction rolled back due to error: {str(e)}")
            raise
        finally:
            session_scoped.remove()  # Release the thread-bound session

    return wrapper


# Retry mechanism
def retrying(retries: int = 3, delay: int = 2):
    """
    Decorator for automatic retry on connection errors.

    :param retries: Maximum retry attempts.
    :param delay: Delay between retries (seconds).
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(retries):
                try:
                    return func(*args, **kwargs)
                except exc.DBAPIError as e:
                    if e.connection_invalidated:
                        logger.warning(
                            "Database connection invalidated, retrying (%d/%d)..."
                            % (attempt + 1, retries),
                        )
                        time.sleep(delay)
                    else:
                        raise
            logger.error("Exhausted all retry attempts.")
            raise RuntimeError("Failed to connect to the database.")

        return wrapper

    return decorator
