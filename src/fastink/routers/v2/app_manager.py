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


def _resolve_app_plugin_module():
    """Resolve the app data-source module from config.

    ``app.plugins`` accepts either a short name (loaded from
    ``fastink.apps.plugins.<name>``) or a fully qualified dotted module
    path (e.g. ``ihep_plugin.apps.omat``), allowing site-specific data
    sources to live in external plugin packages.
    """
    project = get_config("app", "plugins")
    if not project:
        raise LookupError("app.plugins is not configured")
    module_path = project if "." in project else f"fastink.apps.plugins.{project}"
    return importlib.import_module(module_path)


@router.get("/get_job_statistics")
async def get_job_sta(
    username: str = Depends(get_username),
    token: str = Depends(get_token),
    cluster_id: str = Query(None, description="cluster name"),
    query_type: str = Query(None, description="query type")
):
    
    try:
        async with timeout(30): 
            plugin = _resolve_app_plugin_module()
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
    try:
        plugin = _resolve_app_plugin_module()
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