"""Small shared helpers used by every ``apps.<name>`` connect() function.

Historically these lived directly inside ``computing/tools/common/utils.py``
alongside a dozen ``connect_<type>_job`` implementations.  The helpers are
now consolidated here so that each app module can import a stable, minimal
surface.

Scope: read the login-info file the job wrote when it started, translate
that into a ``ConnectResult``, and (for VNC-family apps) ask the worker
node to mint a one-time password.  Talking to the DB is intentionally
kept to the single ``get_job_path`` call inside :func:`resolve_job_paths`
-- callers that need both the ``job_path`` and the ``login_info`` should
use :func:`read_login_info` so the DB is not hit twice.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
import uuid as _uuid
from typing import Optional, Tuple

from fastapi import HTTPException

from fastink.common.config import get_config
from fastink.common.logger import logger
from fastink.computing.tools.db.db_tools import get_job_path
from fastink.computing.tools.common.utils import (
    change_uid_to_username,
    get_user_jobs_dir,
    parse_info,
    read_file,
)
from fastink.storage import common as _storage_common


APP_LOGIN_INFO = "app_login.info"
SSH_LOGIN_INFO = "ssh_login.info"


def get_nginx_node() -> str:
    """Public URL prefix for the reverse proxy (``computing.nginx_node``)."""
    return get_config("computing", "nginx_node")


def get_gateway_node() -> str:
    return get_config("computing", "gateway_node")


async def resolve_job_paths(
    job_id: int, uid: int, cluster_id: str, info_name: str = APP_LOGIN_INFO
) -> Tuple[str, str]:
    """Locate the job dir and the requested login-info file for a job.

    This is the single DB touch point; callers that need both the job
    directory and its login-info file content should prefer
    :func:`read_login_info` which returns both in one call.
    """
    (job_path,) = get_job_path(uid, job_id, cluster_id)
    return job_path, f"{job_path}/{info_name}"


async def read_login_info(
    job_id: int,
    uid: int,
    cluster_id: str,
    info_name: str = APP_LOGIN_INFO,
) -> Tuple[str, str]:
    """Read the job's ``app_login.info`` (or friends).

    Returns ``(job_path, login_info_text)``.  Callers that only care
    about the login-info body can discard the first element; callers
    that also need the job directory (e.g. the VNC apps, which pass
    it to :func:`generate_userotp`) get it here for free instead of
    calling :func:`resolve_job_paths` twice.
    """
    job_path, info_file = await resolve_job_paths(job_id, uid, cluster_id, info_name)
    logger.debug(f"reading login info: {info_file}")
    return job_path, await read_file(uid, info_file)


async def read_latest_job_login_info(
    *,
    username: str,
    uid: int,
    job_type: str,
    info_name: str = APP_LOGIN_INFO,
) -> Optional[Tuple[str, str]]:
    """Read login info from the newest timestamped directory for a job type."""
    jobs_dir = get_user_jobs_dir(username, uid)
    xrootd_path = get_config("storage", "xrd_host")
    entries = await _storage_common.list_path(
        dname=jobs_dir,
        username=username,
        mgm=xrootd_path,
    )
    pattern = re.compile(rf"^{re.escape(job_type)}-\d{{8}}-\d{{6}}$")
    candidates = [
        entry["path"].rstrip("/")
        for entry in entries
        if entry.get("type") == "directory"
        and pattern.fullmatch(os.path.basename(entry.get("path", "").rstrip("/")))
    ]
    if not candidates:
        return None

    latest_dir = max(candidates, key=lambda path: os.path.basename(path))
    info_file = f"{latest_dir}/{info_name}"
    exists, _ = await _storage_common.path_exist(
        name=info_file,
        username=username,
        mgm=xrootd_path,
    )
    if not exists:
        return None

    logger.debug("reading latest %s login info: %s", job_type, info_file)
    return latest_dir, await read_file(uid, info_file)


def parse_hostport(login_info: str) -> Tuple[str, str]:
    """Common HOST/PORT extraction that every connect() needs."""
    return parse_info(login_info, "HOST"), parse_info(login_info, "PORT")


def http_500(detail: str) -> HTTPException:
    return HTTPException(status_code=500, detail=detail)


def http_404(detail: str) -> HTTPException:
    return HTTPException(status_code=404, detail=detail)


# ---------------------------------------------------------------------------
# VNC OTP helper -- FS-based RPC to the running job.
#
# Fastink-server (this process) writes a request file into the job's
# ``otp/`` subdirectory on the shared xrootd namespace; the job's
# ``otp_start_listener`` loop (see apps/shell.sh) picks it up, mints a
# fresh OTP with ``vncpasswd -o`` inside its own VNC session, and writes
# a response file back.  No SSH, no container-side ssh key, no site-
# provided ``vnc_otp_script`` needed.
#
# Callers (vnc / asic / asicbm / ink_special) MUST await this coroutine
# and pass the ``job_path`` returned by :func:`read_login_info`.
# ---------------------------------------------------------------------------

_OTP_DEFAULT_TIMEOUT = 8.0
_OTP_POLL_INTERVAL = 0.2


async def generate_userotp(
    uid: int,
    hostname: str,
    job_path: str | None = None,
    *,
    timeout: float = _OTP_DEFAULT_TIMEOUT,
    poll_interval: float = _OTP_POLL_INTERVAL,
) -> str:
    """Ask the running VNC job to mint a fresh one-time password.

    Parameters
    ----------
    uid, hostname, job_path
        ``hostname`` is retained for logging and back-compat; routing is
        done via the shared filesystem, not the network.  ``job_path``
        MUST be provided (all four in-tree callers already pass it via
        :func:`read_login_info`).
    timeout
        Total wall-clock budget for waiting on the response file.
    poll_interval
        How often to stat the response path.

    Raises
    ------
    HTTPException(500)
        Missing ``.ready`` marker (job not yet at the listener step),
        malformed response body, worker-side mint failure, or timeout.
    """
    if not job_path:
        raise http_500(
            "generate_userotp requires job_path; callers must pass the "
            "value returned by read_login_info()."
        )

    username = change_uid_to_username(uid)
    otp_dir = f"{job_path}/otp"
    req_id = _uuid.uuid4().hex
    req_path = f"{otp_dir}/req_{req_id}"
    resp_path = f"{otp_dir}/resp_{req_id}"
    err_path = f"{otp_dir}/resp_{req_id}.err"
    ready_path = f"{otp_dir}/.ready"

    mgm = get_config("storage", "xrd_host")
    krb5_enabled = get_config("common", "krb5_enabled")

    ready_exists, _ = await _storage_common.path_exist(
        name=ready_path, username=username, mgm=mgm,
    )
    if not ready_exists:
        raise http_500(
            f"VNC OTP listener not ready for host={hostname}; the job "
            f"may still be starting. Retry in a few seconds."
        )

    logger.debug(
        "OTP RPC: uid=%s user=%s host=%s job_path=%s req_id=%s",
        uid, username, hostname, job_path, req_id,
    )
    await _storage_common.upload_file(
        src_data=b"", dst=req_path, username=username, mgm=mgm, mode="600",
    )

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp_exists, _ = await _storage_common.path_exist(
            name=resp_path, username=username, mgm=mgm,
        )
        if resp_exists:
            body = await _storage_common.cat_file(
                fname=resp_path, username=username, mgm=mgm,
                krb5_enabled=krb5_enabled,
            )
            # Best-effort cleanup; the listener's gc loop is the safety net.
            try:
                await _storage_common.delete_path(
                    name=resp_path, username=username, mgm=mgm,
                    krb5_enabled=krb5_enabled,
                )
            except Exception as e:
                logger.debug("OTP RPC: resp cleanup failed (%s): %s", resp_path, e)
            try:
                data = json.loads(body)
                if not isinstance(data, dict):
                    raise ValueError(f"expected JSON object, got {type(data).__name__}")
                raw = data.get("otp")
                if raw is not None and not isinstance(raw, str):
                    raise ValueError(
                        f"otp field must be a string, got {type(raw).__name__}"
                    )
                otp = (raw or "").strip()
            except Exception as e:
                raise http_500(f"Malformed OTP response body: {e}") from e
            if not otp:
                raise http_500("OTP mint returned empty result.")
            return otp

        err_exists, _ = await _storage_common.path_exist(
            name=err_path, username=username, mgm=mgm,
        )
        if err_exists:
            body = await _storage_common.cat_file(
                fname=err_path, username=username, mgm=mgm,
                krb5_enabled=krb5_enabled,
            )
            try:
                await _storage_common.delete_path(
                    name=err_path, username=username, mgm=mgm,
                    krb5_enabled=krb5_enabled,
                )
            except Exception as e:
                logger.debug("OTP RPC: err cleanup failed (%s): %s", err_path, e)
            raise http_500(
                f"OTP mint failed on worker {hostname}: {(body or '').strip() or '(no detail)'}"
            )

        await asyncio.sleep(poll_interval)

    raise http_500(
        f"Timeout waiting for OTP from {hostname} after {timeout:.1f}s. "
        f"The job's OTP listener may be stuck; check job stderr."
    )


__all__ = [
    "APP_LOGIN_INFO",
    "SSH_LOGIN_INFO",
    "get_nginx_node",
    "get_gateway_node",
    "resolve_job_paths",
    "read_login_info",
    "parse_hostport",
    "http_500",
    "http_404",
    "generate_userotp",
    "change_uid_to_username",
    "parse_info",
]
