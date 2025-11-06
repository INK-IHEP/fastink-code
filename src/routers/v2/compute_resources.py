from typing import Union
from fastapi import APIRouter, Query, Request, Body, Depends
from src.computing.hpc.v2 import hpc_create_jobs
from src.computing.htc import htc_create_jobs
from src.computing.hpc.v2 import hpc_check_job
from src.computing.htc import htc_check_job
from src.computing.hpc.v2 import hpc_delete_job
from src.computing.htc import htc_delete_job
from src.computing.hpc.v2 import hpc_job_details
from src.computing.htc import htc_job_details
from src.computing.hpc.v2 import hpc_query_jobs
from src.computing.htc import htc_query_jobs
from src.computing.hpc.v2 import hpc_get_user_assoc
from src.computing.htc import htc_system_jobs
from src.computing.tools.resources_utils import *
from src.apps.statistic.job_statistic import *
from src.routers.headers import get_username, get_token
from src.inkdb.inkredis import *
from src.routers.status import InkStatus
from src.common.logger import logger
from src.computing.adapter.strategy import get_scheduler
from src.computing.cluster.cluster import HTC_JOB, SLURM_JOB

router = APIRouter()

@router.get("/get_joboutput")
async def check_common_job(
    username: str = Depends(get_username),
    token: str = Depends(get_token),
    job_id: int = Query(..., description="Job ID"),
    cluster_id: str = Query(..., description="Cluster ID")
):
    try:
        uid = change_username_to_uid(username)
        if cluster_id == "slurm":
            output_content, err_content = await hpc_check_job.get_job_output(uid=uid, job_id=job_id, cluster_id=cluster_id)
        else:
            output_content, err_content = await htc_check_job.get_job_output(uid=uid, job_id=job_id, clusterid=cluster_id)

        return {
            "status": InkStatus.SUCCESS,
            "msg": "Request Success",
            "data": {
                "job_id": job_id,
                "output": output_content,
                "error": err_content
            }
        }

    except Exception as e:
        logger.error(f"Get job output error, username={username}, job_id={job_id}, cluster_id={cluster_id}")
        return {
            "status": InkStatus.RESOURCE_NOT_FOUND,
            "msg": f"Get job output error: {e}",
            "data": {"job_id" : job_id, "output": "", "error": ""}
        }


@router.get("/connect_job")
async def connect_common_job(    
    username: str = Depends(get_username),
    token: str = Depends(get_token),
    job_id: int = Query(..., description="Job ID"),
    job_type: str = Query(..., description="Job type"),
    cluster_id: str = Query(..., description="Cluster ID")
):
    try:
        uid = change_username_to_uid(username)
        if job_type == "jupyter":
            host, port, jupyter_token, jupyter_url = await connect_jupyter_job(job_id=job_id, uid=uid, clusterid=cluster_id)
            return {
                "status": InkStatus.SUCCESS,
                "msg": "Request Success",
                "data": {
                    "host": host,
                    "port": port,
                    "token": jupyter_token,
                    "url": jupyter_url,
                    "jobId": job_id,
                    "connect_type": "jupyter"
                }
            }
        
        elif job_type == "enode":
            gateway_node, gateway_port = await connect_sshd(job_id=job_id, uid=uid, clusterid=cluster_id)
            return {
                "status": InkStatus.SUCCESS,
                "msg": "Request Success",
                "data": {
                    "host": gateway_node,
                    "gateway_port": gateway_port,
                    "jobId": job_id,
                    "connect_type": "enode"
                }
            }
        
        elif job_type == "rootbrowse":
            host, port, root_token, rootbrowse_url = await connect_rootbrowse_job(job_id=job_id, uid=uid, clusterid=cluster_id)
            return {
                "status": InkStatus.SUCCESS,
                "msg": "Request Success",
                "data": {
                    "host": host,
                    "port": port,
                    "token": root_token,
                    "url": rootbrowse_url,
                    "jobId": job_id,
                    "connect_type": "rootbrowse"
                }
            }
        
        elif job_type == "vscode" or job_type == "npu" or job_type == "compile":
            hostname, port, passwd = await connect_vscode_job(job_id=job_id, uid=uid, clusterid=cluster_id)
            return {
                "status": InkStatus.SUCCESS,
                "msg": "Request Success",
                "data": {
                    "host": hostname,
                    "port": port,
                    "passwd": passwd,
                    "jobId": job_id,
                    "connect_type": "vscode"
                }
            }
        
        elif job_type == "vnc" or job_type == "ink_special":
            host, port, vnc_url = await connect_vnc_job(job_id=job_id, uid=uid, clusterid=cluster_id)
            return {
                "status": InkStatus.SUCCESS,
                "msg": "Request Success",
                "data": {
                    "host": host,
                    "port": port,
                    "url": vnc_url,
                    "jobId": job_id,
                    "connect_type": "vnc"
                }
            }
                
        else:
            return {
                "status": InkStatus.RESOURCE_NOT_SUPPORT,
                "msg": f"Connect job failed with wrong jobtype: {job_type}",
                "data": {}
            } 
            
        
    except Exception as e:
        logger.error(f"Get job output error, username={username}, job_id={job_id}, cluster_id={cluster_id}")
        return {
            "status": InkStatus.SERVER_INTERNAL_ERROR,
            "msg": f"Get job output error: {e}",
            "data": {}
        }


@router.post("/create_job_with_path")
async def create_job_with_path(
    req: Request,
    username: str = Depends(get_username),
    token: str = Depends(get_token),
    job_type: str = Body(..., description="Job type", embed=True),
    cluster_id: str = Body(..., description="Cluster ID",embed=True)
):
    try: 
        logger.info(f"Get the create job with path request: {req}")
        uid = change_username_to_uid(username)

        data = await req.json()
        job_request =  hpc_create_jobs.JobCreateRequestWithPath(**data)
        job_id, job_type, job_path = await hpc_create_jobs.create_job_with_path(request=job_request, uid=uid, cluster_id=cluster_id, job_type=job_type)
        logger.info(f"Create job with path finished, the jobid: {job_id}, jobpath: {job_path}, job_type: {job_type}")

        return {
            "status": InkStatus.SUCCESS,
            "msg": f"Create job with path successfully, jobid: {job_id}",
            "data": {
                "jobId": job_id,
                "jobType": job_type,
                "jobPath": job_path
            }
        }
    
    except Exception as e:
        logger.error(f"Create job with path failed, username: {username}, details: {e}.")
        return {
            "status": InkStatus.SERVER_INTERNAL_ERROR,
            "msg": f"Create job failed: {e}",
            "data": {}
        }


@router.get("/get_jobdetails")
async def get_common_job_details(
    username: str = Depends(get_username),
    token: str = Depends(get_token),
    job_id: str = Query(..., description="Job ID"),
    cluster_id: str = Query(..., description="Cluster ID")
):
    try:
        uid = change_username_to_uid(username)
        if cluster_id == "slurm":
            jobStartTime, jobStatus, jobNodeList, jobSubmitTime, jobType, connect_sign = await hpc_job_details.get_user_jobs(uid, job_id, cluster_id)
            response_data = {
                "status": InkStatus.SUCCESS,
                "msg": "Request Success",
                "data": {
                    "clusterId": "slurm",
                    "jobId": int(job_id),
                    "jobStartTime": jobStartTime,
                    "jobStatus": jobStatus,
                    "jobNodeList": jobNodeList,
                    "jobSubmitTime":jobSubmitTime,
                    "jobType": jobType,
                    "connect_sign": connect_sign
                }
            }
            
        elif cluster_id == "htcondor":
            response_data = await htc_job_details.get_user_job_details(uid, job_id, cluster_id)
        else:
            return {
                "status": InkStatus.RESOURCE_NOT_SUPPORT,
                "msg": "Get job details failed with wrong cluster_id.",
                "data": {}
            }
            
        return response_data
        
    except Exception as e:
        logger.error(f"Get job details failed, username: {username}, cluster_id: {cluster_id}, details: {e}.")
        return {
            "status": InkStatus.SERVER_INTERNAL_ERROR,
            "msg": f"Get job details failed: {e}",
            "data": {}
        }
    

@router.get("/get_userassoc")
async def get_common_user_assoc(
    username: str = Depends(get_username),
    token: str = Depends(get_token),
    cluster_id: str = Query(..., description="Cluster ID")
):
    try:
        uid = change_username_to_uid(username)
        
        if cluster_id == "slurm":
            account, qos, partition = await hpc_get_user_assoc.get_user_assoc(uid)
        else:
            return {
                "status": InkStatus.RESOURCE_NOT_SUPPORT,
                "msg": f"Cluster ID not support: {cluster_id}",
                "data": {}
            }
        return {
            "status": InkStatus.SUCCESS,
            "msg": "Request Success",
            "data": {
                "account" : account,
                "qos": qos,
                "partition": partition
            }
        }
    
    except Exception as e :
        return {
            "status": InkStatus.SERVER_INTERNAL_ERROR,
            "msg": f"Request to get user association Failed with error : {e}",
            "data": {}
        }
    


@router.get("/get_systemjobs")
async def query_system_jobs():
    try: 
        system_statistic_list = await htc_system_jobs.get_system_jobs()

        return {
            "status": InkStatus.SUCCESS,
            "msg": "Request Success",
            "data":{
                "job_total_list": system_statistic_list
            }
        }
            
    except Exception as e:
        logger.error(f"Get system job statistic info failed, details: {e}.")
        return {
            "status": InkStatus.SERVER_INTERNAL_ERROR,
            "msg": f"Get system job statistic info failed.: {e}.",
            "data": {}
        }


@router.post("/create_job")
async def create_common_job(
    username: str = Depends(get_username),
    token: str = Depends(get_token),
    jobclass: Union[SLURM_JOB, HTC_JOB] = Body(..., discriminator="cluster_id"),
):
    try:
        adapter = get_scheduler(jobclass.cluster_id, username)
        job_id, job_dir = await adapter.submit_job(jobclass)
        return {
            "status": InkStatus.SUCCESS,
            "msg": f"Create job successfully, jobid: {job_id}, cluster: {jobclass.cluster_id}.",
            "data": {
                "jobId": job_id,
                "jobType": jobclass.job_type,
                "jobPath": job_dir
            }
        }        
    
    except Exception as e:
        logger.error(f"Create job failed, username: {username}, cluster_id: {jobclass.cluster_id}, details: {e}.")
        return {
            "status": InkStatus.SERVER_INTERNAL_ERROR,
            "msg": f"Create job failed: {e}",
            "data": {}
        }


@router.get("/query_jobs")
async def query_common_job(
    username: str = Depends(get_username),
    token: str = Depends(get_token),
    cluster_id: str = Query(..., description="Cluster ID"),
    job_type: str = Query(None, description="Job type"),
    page: int = Query(1, description="Pangination page"),
    limit: int = Query(5000, description="lines of each page")
):
    try:
        joblist = []
        if cluster_id == "all":
            try:
                cluster_list = get_config("computing", "cluster_list")
                if not cluster_list:
                    raise ValueError("Get user jobs failed, Empty cluster_list from config.yaml")
                if isinstance(cluster_list, str):
                    cluster_ids = [c.strip() for c in cluster_list.split(",") if c.strip()]
                else:
                    cluster_ids = list(cluster_list)
            except Exception as e:
                logger.error(f"Get user jobs failed (load cluster_list), username: {username}, details: {e}.")
                return {
                    "status": InkStatus.SERVER_INTERNAL_ERROR,
                    "msg": f"Get user {username} jobs failed: {e}.",
                    "data": {}
                }

            for cid in cluster_ids:
                try:
                    adapter = get_scheduler(cid, username)
                    jobs = await adapter.query_job(job_type)
                    if jobs:
                        joblist.extend(jobs)
                except Exception as e:
                    logger.error(f"Get user jobs failed for cluster {cid}, username: {username}, details: {e}.")
        else:
            adapter = get_scheduler(cluster_id, username)
            joblist = await adapter.query_job(job_type) or []

        start_idx = max(0, (page - 1) * limit)
        end_idx = start_idx + max(0, limit)
        paged = joblist[start_idx:end_idx]

        return {
            "status": InkStatus.SUCCESS,
            "msg": f"Query user {username} jobs successfully.",
            "data": {
                "job_list": paged
            }
        }

    except Exception as e:
        logger.error(f"Get user jobs failed, username: {username}, cluster_id: {cluster_id}, details: {e}.")
        return {
            "status": InkStatus.SERVER_INTERNAL_ERROR,
            "msg": f"Get user {username} jobs failed: {e}.",
            "data": {}
        }

@router.post("/delete_job")
async def delete_common_job(
    username: str = Depends(get_username),
    token: str = Depends(get_token),
    job_id: str = Body(..., description="Job ID",embed=True),
    cluster_id: str = Body(..., description="Cluster ID",embed=True)
):
    try:
        adapter = get_scheduler(cluster_id, username)
        await adapter.cancel_job(job_id)
        return {
            "status": InkStatus.SUCCESS,
            "msg": f"Delete job {job_id} successfully.",
            "data": {}
        }

    except Exception as e:
        logger.error(f"Delete job failed, username: {username}, cluster_id: {cluster_id}, details: {e}.")
        return {
            "status": InkStatus.SERVER_INTERNAL_ERROR,
            "msg": f"Delete job failed: {e}.",
            "data": {}
        }