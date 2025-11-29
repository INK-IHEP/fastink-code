from src.computing.tools.resources_utils import *
from src.apps.statistic.htc_statistics import *
from fastapi.responses import HTMLResponse
from src.inkdb.inkredis import *
from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel
from async_timeout import timeout


router = APIRouter()
@router.get("/apps/get-job-statistics")
async def get_job_sta(
    cluster_id: str = Query(None, description="cluster name"),
    query_type: str = Query(None, description="query type")
):
    try:
        async with timeout(15):  # 精确控制整个异步块
            job_info = await ink_sta_job(cluster_id, query_type)
            
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail=f"Job status query timed out after 15s"
        )
    except Exception as err:
        # 细化异常处理逻辑
        error_msg = f"Failed to get HTC Job Info: {str(err)}"
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=error_msg
        ) from err  # 保留原始异常堆栈

    return {
        "status": 200,
        "msg": "success",
        "data": job_info
    }


@router.get("/apps/token")
async def get_token(
    username: str = Query(None), email: str = Query(None), uid: int = Query(None)
):
    try:
        token = get_krb5(username, email, uid)
        result = {"token": token}
    except Exception as err:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Token retrieval failed: {err}",
        )
    return result
