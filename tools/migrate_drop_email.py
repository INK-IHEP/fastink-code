#!/usr/bin/env python3
"""Idempotent migration: drop the ``email`` column from ``users``.

Safe to run multiple times — checks whether the column exists before
attempting the DROP.  Intended to be called from CI/CD after deploying
a version of fastink-code that no longer reads the column.

NOTE: MySQL DDL statements commit implicitly, so the index drops and the
column drop are NOT atomic.  A crash between steps is still safe: the
next run re-inspects and finishes the remaining steps.

Usage:
    python3.12 tools/migrate_drop_email.py
"""
from sqlalchemy import create_engine, inspect, text

from fastink.common.config import get_config
from fastink.common.logger import logger


def _email_index_names(indexes: list[dict]) -> list[str]:
    """Names of indexes that cover exactly the ``email`` column."""
    return [idx["name"] for idx in indexes if idx.get("column_names") == ["email"]]


def migrate():
    db_config = get_config("database")
    db_url = (
        f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    )
    engine = create_engine(db_url)

    with engine.begin() as conn:
        inspector = inspect(conn)
        columns = [col["name"] for col in inspector.get_columns("users")]
        if "email" not in columns:
            logger.info("Column 'email' does not exist in 'users' — nothing to do.")
            return

        logger.info("Dropping column 'email' from 'users' ...")
        preparer = engine.dialect.identifier_preparer
        for idx_name in _email_index_names(inspector.get_indexes("users")):
            conn.execute(text(f"DROP INDEX {preparer.quote_identifier(idx_name)} ON `users`"))
            logger.info("Dropped index %s", idx_name)
        conn.execute(text("ALTER TABLE `users` DROP COLUMN `email`"))
    logger.info("Column 'email' dropped successfully.")


if __name__ == "__main__":
    migrate()
