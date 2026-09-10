#!/usr/bin/env python3
"""Initialize FastINK database: create tables, seed permissions and users.

User and group-permission seeding are driven by an optional JSON file.
The default (no file) creates only the ``root`` user with admin permission.

Seed file location (checked in order):
  1. ``FASTINK_SEED_USERS_FILE`` environment variable
  2. ``/opt/fastink/seed-users.json`` (conventional mount point)

Seed file format — either a JSON list of user objects (users only)::

    [{"username": "alice", "uid": 1001, "permissions": ["admin"]}, ...]

or an object with both users and group-permission mappings::

    {
      "users": [{"username": "alice", "permissions": ["admin"]}, ...],
      "group_permissions": [{"group_name": "staff", "permission": "gpu"}, ...]
    }

Each user entry must have a non-blank ``username``; ``uid`` (non-negative
integer) and ``permissions`` are optional.  ``permissions`` defaults to
["admin", "ink_special"].

Failure policy: when no seed file is configured, only ``root`` is seeded.
When a seed file IS configured (env var or the default path exists) but is
missing, unreadable, or malformed, init fails loudly — silently falling
back to root-only would hide a broken deployment.

NOTE: seed users are inserted directly into the database and intentionally
bypass the external identity-directory validation hook
(``verify_user_identity``).  The seed file is operator-controlled site
configuration, so directory validation is neither required nor desired here.
"""
import json
import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError, NoResultFound

from fastink.common.config import get_config
from fastink.common.logger import logger
from fastink.database.sqla.models import BASE
from fastink.auth import permission, common

DEFAULT_SEED_FILE = Path("/opt/fastink/seed-users.json")
DEFAULT_SEED_PERMISSIONS = ["admin", "ink_special"]


def _seed_file_path() -> Path | None:
    """Return the configured seed file path, or None when unconfigured."""
    env_path = os.environ.get("FASTINK_SEED_USERS_FILE", "").strip()
    if env_path:
        return Path(env_path)
    return DEFAULT_SEED_FILE if DEFAULT_SEED_FILE.exists() else None


def _validate_seed_users(users: list[dict], path: Path) -> None:
    for entry in users:
        if not isinstance(entry, dict):
            raise RuntimeError(f"Invalid seed user entry in {path}: {entry!r}")
        username = entry.get("username")
        if not isinstance(username, str) or not username.strip():
            raise RuntimeError(f"Invalid seed user entry in {path}: {entry!r}")
        uid = entry.get("uid")
        if uid is not None and (
            not isinstance(uid, int) or isinstance(uid, bool) or uid < 0
        ):
            raise RuntimeError(
                f"uid must be a non-negative integer for seed user {username!r}: {uid!r}"
            )
        perms = entry.get("permissions", DEFAULT_SEED_PERMISSIONS)
        if (
            not isinstance(perms, list)
            or not all(isinstance(p, str) and p.strip() for p in perms)
        ):
            raise RuntimeError(
                f"Invalid permissions for seed user {username!r} in {path}: {perms!r}"
            )


def _validate_group_permissions(mappings: list[dict], path: Path) -> None:
    for mapping in mappings:
        if (
            not isinstance(mapping, dict)
            or not isinstance(mapping.get("group_name"), str)
            or not mapping["group_name"].strip()
            or not isinstance(mapping.get("permission"), str)
            or not mapping["permission"].strip()
        ):
            raise RuntimeError(f"Invalid group_permission entry in {path}: {mapping!r}")


def _load_seed() -> dict[str, list]:
    """Load the seed configuration.  Falls back to root-only when unconfigured."""
    path = _seed_file_path()
    if path is None:
        logger.info("No seed file configured; seeding root user only.")
        return {"users": [{"username": "root"}], "group_permissions": []}
    if not path.is_file():
        raise RuntimeError(f"Seed users path is not a readable file: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        users, group_perms = data, []
    elif isinstance(data, dict):
        if "users" not in data or not isinstance(data["users"], list):
            raise RuntimeError(
                f"Seed file {path}: object form requires a 'users' list"
            )
        users = data["users"]
        group_perms = data.get("group_permissions", [])
        if not isinstance(group_perms, list):
            raise RuntimeError(
                f"Seed file {path}: 'group_permissions' must be a list"
            )
    else:
        raise RuntimeError(
            f"Seed file {path} must contain a JSON list or object with 'users'"
        )
    _validate_seed_users(users, path)
    _validate_group_permissions(group_perms, path)
    logger.info(
        "Loaded %d seed user(s) and %d group permission mapping(s) from %s",
        len(users), len(group_perms), path,
    )
    return {"users": users, "group_permissions": group_perms}


def init_db():
    db_config = get_config("database")

    host = db_config["host"]
    port = db_config["port"]
    user = db_config["user"]
    password = db_config["password"]
    database = db_config["dbname"]

    db_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    # Never log db_url: it embeds the database password.
    logger.debug("Connecting to database: %s:%s/%s (user %s)", host, port, database, user)

    engine = create_engine(db_url)
    BASE.metadata.create_all(engine)

    logger.info("Database and tables initialized successfully.")

    perm_list = [
        "cpu",
        "gpu",
        "CentOS7",
        "AlmaLinux9",
        "admin",
        "ink_special",
        "elk",
        "compile",
        "lhaasogpu",
        "juno_daq",
        "job_lens"
    ]
    for perm in perm_list:
        try:
            common.get_permission(perm)
            continue
        except NoResultFound:
            pass
        try:
            permission.add_permission(perm)
        except IntegrityError:
            pass

    seed = _load_seed()

    for useritem in seed["users"]:
        username = useritem["username"].strip()
        uid = useritem.get("uid")
        perms = useritem.get("permissions", DEFAULT_SEED_PERMISSIONS)
        try:
            common.get_user(username=username)
        except NoResultFound:
            common.add_user(username=username, uid=uid)
        # Reconcile permissions on every run so seed grants stay idempotent
        # for both fresh and pre-existing users.
        for perm in perms:
            try:
                common.get_permission(perm)
            except NoResultFound:
                raise RuntimeError(
                    f"Seed user {username!r} requests unknown permission {perm!r}"
                )
            permission.add_user_permission(username, perm)
            # False here means the grant already existed (idempotent re-run).

    authenticationlist = ["password", "krb5"]
    for authentication in authenticationlist:
        try:
            common.get_authentication(authentication)
            continue
        except NoResultFound:
            pass
        try:
            common.add_authentication(authentication)
        except IntegrityError:
            pass

    for mapping in seed["group_permissions"]:
        group_name = mapping["group_name"].strip()
        perm_name = mapping["permission"].strip()
        try:
            perm_id = common.get_permission(permission=perm_name)["id"]
        except NoResultFound:
            raise RuntimeError(
                f"Group mapping {group_name!r} requests unknown permission {perm_name!r}"
            )
        try:
            common.add_group_permission(
                group_name=group_name,
                permission_id=perm_id,
            )
            logger.info(
                "Seeded group_permission: %s -> %s",
                group_name,
                perm_name,
            )
        except IntegrityError:
            pass  # Mapping already exists, skip


if __name__ == "__main__":
    init_db()
