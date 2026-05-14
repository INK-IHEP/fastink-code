#! /usr/bin/python3
# FileName      : service_manager.py
# Author        : HAN Xiao
# Email         : hanx@ihep.ac.cn
# Date          : Fri Jun 27 10:06:54 2025 CST
# Last modified : Fri Nov 14 09:37:01 2025 CST
# Description   : version 2 of service manager

import traceback

from fastapi import APIRouter, Body, Depends

from fastink.common.logger import logger
from fastink.routers.headers import get_username
from fastink.routers.status import InkStatus
from fastink.service.monitor import get_job_monitor_url, get_monitor_url
from fastink.service.openclaw import (
    get_openclaw_template,
    sync_openclaw_models,
)
from fastink.service.openclaw_schema import OpenClawSyncRequest
from fastink.service.rootbrowse import access_rootfile

router = APIRouter()


@router.post("/service/check")
def service_check():
    return {"message": "Services Checked"}


async def _handle_access_rootfile(
    username: str, workdir: str, filename: str, is_private: bool
) -> dict:
    try:
        url = await access_rootfile(username, workdir, filename, is_private=is_private)
        msg_user = f"{username}" if is_private else f"Someone"
        msg_owner = f" (owned by {username})" if not is_private else ""
        return {
            "status": InkStatus.SUCCESS,
            "msg": f"{msg_user} created URL for {workdir}/{filename}{msg_owner} successfully",
            "data": {"url": url},
        }
    except Exception as e:
        logger.error(
            f"An unexpected error occurred: {str(e)}\n{traceback.format_exc()}"
        )
        return {
            "status": InkStatus.ACCESS_ROOTFILE_FAILURE,
            "msg": f"Failed to access File {filename}: {str(e)}",
            "data": None,
        }


@router.post("/service/access_rootfile")
async def post_access_rootfile(
    username: str = Depends(get_username),
    workdir: str = Body(..., description="Work Directory"),
    filename: str = Body(..., description="Root File Name"),
) -> dict:
    return await _handle_access_rootfile(username, workdir, filename, is_private=True)


# same as post_access_rootfile, but without authentication (for shared files)
# limited by ip whitelist
@router.post("/service/access_shared_rootfile")
async def post_access_shared_rootfile(
    username: str = Body(..., description="File Owner Name"),
    workdir: str = Body(..., description="Work Directory"),
    filename: str = Body(..., description="Root File Name"),
) -> dict:
    return await _handle_access_rootfile(username, workdir, filename, is_private=False)


@router.get("/service/get_monitorurl")
async def get_monitorurl() -> dict:
    return {
        "status": InkStatus.SUCCESS,
        "msg": "Get monitor url successfully",
        "data": {"url": get_monitor_url()},
    }


@router.post("/service/query_jobsmonitor")
async def query_jobsmonitor(job_id: int = Body(..., embed=True)) -> dict:
    return {
        "status": InkStatus.SUCCESS,
        "msg": "Get jobs monitor url successfully",
        "data": {"url": get_job_monitor_url(job_id)},
    }


@router.post("/service/openclaw/sync_models")
async def post_sync_openclaw_models(
    payload: OpenClawSyncRequest,
    username: str = Depends(get_username),
) -> dict:
    try:
        result = await sync_openclaw_models(username, payload)
        return {
            "status": InkStatus.SUCCESS,
            "msg": "Sync OpenClaw template successfully",
            "data": result,
        }
    except Exception as e:
        logger.error(
            f"Failed to sync OpenClaw models for user {username}: {str(e)}\n{traceback.format_exc()}"
        )
        return {
            "status": InkStatus.INTERNAL_ERROR,
            "msg": f"Failed to sync OpenClaw models: {str(e)}",
            "data": None,
        }


@router.get("/service/openclaw/template")
async def get_openclaw_template_config(
    username: str = Depends(get_username),
) -> dict:
    try:
        result = await get_openclaw_template(username)
        return {
            "status": InkStatus.SUCCESS,
            "msg": "Get OpenClaw template successfully",
            "data": result,
        }
    except Exception as e:
        logger.error(
            f"Failed to get OpenClaw template for user {username}: {str(e)}\n{traceback.format_exc()}"
        )
        return {
            "status": InkStatus.INTERNAL_ERROR,
            "msg": f"Failed to get OpenClaw template: {str(e)}",
            "data": None,
        }
