from fastapi.responses import HTMLResponse
from src.inkdb.inkredis import *
from fastapi import APIRouter, HTTPException, Query, status, Depends
from pydantic import BaseModel
from async_timeout import timeout
from src.routers.headers import get_username, get_token
import importlib
import asyncio
from src.common.config import get_config
from src.common.logger import logger
from src.routers.status import *


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
            plugin = importlib.import_module(f"src.apps.plugins.{project}")
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
        plugin = importlib.import_module(f"src.apps.plugins.{project}")
        data_by_jobtype = await plugin.get_job_stack_data()
        return {
            "status": InkStatus.SUCCESS,
            "msg": "请求成功",
            "data": data_by_jobtype
        }

        # jupyter_list, vscode_list, vnc_list, rootbrowse_list, enode_list, gpu_list, npu_list = await plugin.get_job_stack_data()
        
        # return {
        #     "status": InkStatus.SUCCESS,
        #     "msg": "请求成功",
        #     "data":{
        #         "Jupyter": jupyter_list,
        #         "Vscode": vscode_list,
        #         "Vnc": vnc_list,
        #         "Rootbrowse": rootbrowse_list,
        #         "Enode": enode_list,
        #         "GPU": gpu_list,
        #         "NPU": npu_list
        #     }
        # }
    
    except Exception as err:
        return {
            "status": InkStatus.MONITOR_QUERY_FAILED,
            "msg": f"Failed to get omat stack jobs, and the err details: {err}",
            "data": ""
        }
        

    
    
    