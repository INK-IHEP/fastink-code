#! /usr/bin/python3
# code_datadir.py — ensure opencode/openchamber data dir is not on AFS.
#
# opencode's SQLite database lives under ~/.local/share/opencode. AFS does
# not handle SQLite file locking well, so when the resolved path lands on
# /afs we move (or create) the directory under a non-AFS scratch root and
# replace the user path with a symlink back to it.

from __future__ import annotations

import subprocess
from pathlib import Path
from shlex import quote

from fastink.common.config import get_config
from fastink.common.logger import logger
from fastink.computing.tools.common.utils import (
    change_username_to_uid,
    get_user_exp_group,
)

AFS_PREFIX = "/afs"
CODE_DATADIR_RELPATH = ".local/share/opencode"

# Resolution states for a candidate path.
RESOLVED_ABSENT = "absent"
RESOLVED_AFS = "afs"
RESOLVED_OK = "ok"


def _get_scratch_root(username: str, group_dir: str) -> str:
    template = get_config(
        "service",
        "code_datadir_root",
        fallback="/home/{username}",
    )
    return template.format(
        username=username,
        experiment_group_lower=group_dir,
        group_dir=group_dir,
    )


def _resolve_user_experiment_group(username: str) -> str:
    uid = change_username_to_uid(username)
    experiment_group, raw_group = get_user_exp_group(uid)
    group_dir = (experiment_group or raw_group or "").lower()
    if not group_dir:
        raise ValueError(f"Failed to resolve scratchfs experiment group for {username}")
    return group_dir


def _get_user_home(username: str) -> str:
    result = subprocess.run(
        ["su", "-s", "/bin/bash", username, "-c", "printf %s \"$HOME\""],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            result.stderr.strip() or result.stdout.strip() or f"Failed to resolve home for {username}"
        )
    home = result.stdout.strip()
    if not home:
        raise RuntimeError(f"Failed to resolve home for {username}")
    return home


def _run_as_user(username: str, command: str) -> str:
    result = subprocess.run(
        ["su", "-s", "/bin/bash", username, "-c", command],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "unknown error")
    return result.stdout


def _get_krb5ccname(username: str) -> str:
    from fastink.common.utils import get_krb5cc

    _, _, krb5ccname = get_krb5cc(name=username, krb5=True)
    if not krb5ccname:
        raise RuntimeError(f"No krb5 credential cache available for {username}")
    return krb5ccname


def _path_exists(username: str, path: str) -> bool:
    """Return True if the path exists (following symlinks)."""
    result = subprocess.run(
        ["su", "-s", "/bin/bash", username, "-c", f"test -e {quote(path)}"],
        text=True,
        capture_output=True,
        check=False,
    )
    return result.returncode == 0


def _resolve_path(username: str, path: str) -> str:
    """Resolve a path through symlinks. Returns "" when the path does not
    exist (readlink -f resolves missing final components to a full path, so
    existence must be checked first)."""
    if not _path_exists(username, path):
        return ""
    result = subprocess.run(
        ["su", "-s", "/bin/bash", username, "-c", f"readlink -f {quote(path)}"],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def _classify_path(username: str, path: str) -> str:
    """Classify a candidate path: absent / on AFS / non-AFS ok.

    Existence is judged on the symlink-followed path: if the path is absent
    (or its symlink target is absent) the candidate is treated as absent and
    the caller climbs up to the parent directory.
    """
    resolved = _resolve_path(username, path)
    if not resolved:
        return RESOLVED_ABSENT
    if resolved.startswith(AFS_PREFIX):
        return RESOLVED_AFS
    return RESOLVED_OK


def _build_aklog_prefix(username: str) -> str:
    krb5ccname = _get_krb5ccname(username)
    return f"export KRB5CCNAME={quote(krb5ccname)} && aklog"


def _migrate_from_afs(username: str, src: str, target: str) -> None:
    _run_as_user(
        username,
        f"{_build_aklog_prefix(username)} && "
        f"mkdir -p {quote(str(Path(target).parent))} && "
        f"chmod 700 {quote(str(Path(target).parent))} && "
        f"mv {quote(src)} {quote(target)} && "
        f"chmod 700 {quote(target)}",
    )


def _create_target(username: str, target: str) -> None:
    _run_as_user(
        username,
        f"{_build_aklog_prefix(username)} && "
        f"mkdir -p {quote(target)} && "
        f"chmod 700 {quote(str(Path(target).parent))} && "
        f"chmod 700 {quote(target)}",
    )


def _symlink_back(username: str, target: str, user_path: str) -> None:
    _run_as_user(
        username,
        f"{_build_aklog_prefix(username)} && "
        f"mkdir -p {quote(str(Path(user_path).parent))} && "
        f"ln -sfn {quote(target)} {quote(user_path)}",
    )


async def ensure_code_datadir(username: str) -> dict:
    """Ensure ~/.local/share/opencode resolves to a non-AFS directory.

    Logic:

      1. classify ~/.local/share/opencode
         - ok (non-AFS)          -> nothing to do
         - afs (exists on AFS)   -> mv target, symlink back
         - absent                -> climb parents in order
                                    (~/.local/share, ~/.local, ~):
           - parent ok (exists, non-AFS)  -> nothing to do (opencode will
                                             create its dir there naturally)
           - parent afs (exists on AFS)   -> create target + symlink back
           - parent absent                -> keep climbing
         - all absent / nothing on AFS    -> nothing to do
    """
    home = _get_user_home(username)
    user_path = f"{home}/{CODE_DATADIR_RELPATH}"

    group_dir = _resolve_user_experiment_group(username)
    scratch_root = _get_scratch_root(username, group_dir)
    target = f"{scratch_root}/.ink/opencode"

    state = _classify_path(username, user_path)
    if state == RESOLVED_OK:
        logger.info("opencode datadir already non-AFS: user=%s path=%s", username, user_path)
        return {
            "username": username,
            "group_dir": group_dir,
            "target": target,
            "migrated": False,
            "created": False,
            "detail": "not_on_afs",
        }
    if state == RESOLVED_AFS:
        src = _resolve_path(username, user_path)
        _migrate_from_afs(username, src, target)
        _symlink_back(username, target, user_path)
        logger.info("opencode datadir moved off AFS: user=%s %s -> %s", username, src, target)
        return {
            "username": username,
            "group_dir": group_dir,
            "target": target,
            "migrated": True,
            "created": False,
            "detail": "migrated_from_afs",
        }

    # user_path absent -> climb parents
    for parent_rel in ("~/.local/share", "~/.local", "~"):
        parent_path = f"{home}/{parent_rel[2:]}" if parent_rel != "~" else home
        parent_state = _classify_path(username, parent_path)
        if parent_state == RESOLVED_OK:
            logger.info(
                "opencode parent non-AFS, nothing to do: user=%s parent=%s",
                username,
                parent_path,
            )
            return {
                "username": username,
                "group_dir": group_dir,
                "target": target,
                "migrated": False,
                "created": False,
                "detail": "parents_not_on_afs",
            }
        if parent_state == RESOLVED_AFS:
            _create_target(username, target)
            _symlink_back(username, target, user_path)
            logger.info(
                "opencode datadir created off AFS: user=%s anchor=%s target=%s",
                username,
                parent_path,
                target,
            )
            return {
                "username": username,
                "group_dir": group_dir,
                "target": target,
                "migrated": False,
                "created": True,
                "detail": "created_off_afs",
            }

    logger.info("no AFS anchor for opencode datadir: user=%s", username)
    return {
        "username": username,
        "group_dir": group_dir,
        "target": target,
        "migrated": False,
        "created": False,
        "detail": "no_afs_anchor",
    }
