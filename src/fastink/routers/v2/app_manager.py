from fastapi.responses import HTMLResponse
from fastink.inkdb.inkredis import *
from fastapi import APIRouter, HTTPException, Query, status, Depends
from pydantic import BaseModel
from async_timeout import timeout
from fastink.routers.headers import get_username, get_token
import importlib
import asyncio
from fastink.common.config import get_config
from fastink.common.logger import logger
from fastink.routers.status import *
from fastink.apps.drawio import drawio

router = APIRouter()
@router.get("/get_job_statistics")
async def get_job_sta(
    username: str = Depends(get_username),
    token: str = Depends(get_token),
    cluster_id: str = Query(None, description="cluster name"),
    query_type: str = Query(None, description="query type")
):
    
    project = get_config("app", "plugins")
    
    try:
        async with timeout(30): 
            plugin = importlib.import_module(f"fastink.apps.plugins.{project}")
            job_info = await plugin.get_cluster_stack_data(cluster_id, query_type)
            
    except asyncio.TimeoutError as err:
        return {
            "status": InkStatus.MONITOR_QUERY_TIMEOUT,
            "msg": f"Get HTC job info timeout in func(get_job_sta), {err}",
            "data": ""
        }
    except Exception as err:
        return {
            "status": InkStatus.MONITOR_QUERY_FAILED,
            "msg": f"Failed to get HTC job info, and the err details: {err}",
            "data": ""
        }

    return {
        "status": InkStatus.SUCCESS,
        "msg": "success",
        "data": job_info
    }


@router.get("/get_stackjobs")
async def get_omat_stack_jobs_info():
    project = get_config("app", "plugins")
    try:
        plugin = importlib.import_module(f"fastink.apps.plugins.{project}")
        data_by_jobtype = await plugin.get_job_stack_data()
        return {
            "status": InkStatus.SUCCESS,
            "msg": "请求成功",
            "data": data_by_jobtype
        }
    
    except Exception as err:
        return {
            "status": InkStatus.MONITOR_QUERY_FAILED,
            "msg": f"Failed to get omat stack jobs, and the err details: {err}",
            "data": ""
        }

@router.get("/drawio")
async def app_drawio(
    username: str = Depends(get_username),
    TargetPath: str = Query(..., description="File Path"),
    Type:str = Query("svg", description="File Type"),
    create:bool = Query(False, description="Create new file")):

    try:
        response = await drawio.draw(username = username, TargetPath = TargetPath, Type = Type, create = create)
    except Exception as e:
        logger.error(f"Failed to load drawio app. Err:{str(e)}")
        return {"status": InkStatus.APP_UNKNOWN, "msg": f"Failed to load drawio app. Err:{str(e)}", "data": None}
    return response