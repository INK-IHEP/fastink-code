#! /usr/bin/python3
# FileName      : service_manager.py
# Author        : HAN Xiao
# Email         : hanx@ihep.ac.cn
# Date          : Fri Jun 27 10:06:54 2025 CST
# Last modified : Fri Sep 19 23:27:44 2025 CST
# Description   : version 2 of service manager

import traceback

from fastapi import APIRouter, Body, Depends

from src.common.logger import logger
from src.routers.headers import get_username
from src.routers.status import InkStatus
from src.service.rootbrowse import access_rootfile

router = APIRouter()


@router.post("/service/check")
def service_check():
    return {"message": "Services Checked"}


async def _handle_access_rootfile(
    username: str, workdir: str, filename: str, is_private: bool
):
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
):
    return await _handle_access_rootfile(username, workdir, filename, is_private=True)


# same as post_access_rootfile, but without authentication (for shared files)
# limited by ip whitelist
@router.post("/service/access_shared_rootfile")
async def post_access_shared_rootfile(
    username: str = Body(..., description="File Owner Name"),
    workdir: str = Body(..., description="Work Directory"),
    filename: str = Body(..., description="Root File Name"),
):
    return await _handle_access_rootfile(username, workdir, filename, is_private=False)
