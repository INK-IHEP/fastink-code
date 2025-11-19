#! /usr/bin/python3
# FileName      : service_manager.py
# Author        : HAN Xiao
# Email         : hanx@ihep.ac.cn
# Date          : Fri Jun 13 14:14:21 2025 CST
# Last modified : Fri Jun 27 10:26:54 2025 CST
# Description   :
from fastapi import APIRouter, Header, Body
import traceback

from src.service.rootbrowse import access_rootfile
from src.common.logger import logger

router = APIRouter()


@router.post("/service/check")
def service_check():
    return {"message": "Services Checked"}


@router.post("/service/open_root")
async def post_access_rootfile(
    # body: OpenRootRequest,
    username: str = Header(..., description="User Name"),
    token: str = Header(..., description="Token"),
    workdir: str = Body(...),
    filename: str = Body(...),
):
    try:
        url = await access_rootfile(username, workdir, filename)
        return {
            "status": 200,
            "msg": f"{username} created URL for {workdir}/{filename} successfully",
            "data": {"url": url},
        }
    except Exception as e:
        logger.error(
            f"An unexpected error occurred: {str(e)}\n{traceback.format_exc()}"
        )
        return {
            "status": 500,
            "msg": "Error",
            "data": {"error": f"Failed to open File {filename}: {str(e)}"},
        }
