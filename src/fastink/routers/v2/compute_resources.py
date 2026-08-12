import re
from typing import Union
from fastapi import APIRouter, Query, Request, Body, Depends, Header, Response
from fastapi.responses import JSONResponse
from fastink.computing.hpc.v2 import hpc_create_jobs
from fastink.computing.hpc.v2 import hpc_check_job
from fastink.computing.htc import htc_check_job
from fastink.computing.hpc.v2 import hpc_get_user_assoc
from fastink.computing.htc import htc_system_jobs
from fastink.apps.statistic.job_statistic import *
from fastink.routers.headers import get_username, get_token
from fastink.routers import headers
from fastink.inkdb.inkredis import *
from fastink.routers.status import InkStatus
from fastink.common.logger import logger
from fastink.computing.adapter.strategy import get_scheduler
from fastink.computing.cluster.cluster import HTC_JOB, SLURM_JOB, SubmitMode
from fastink.computing.tools.common.utils import change_username_to_uid
from fastink.computing.apps import registry as computing_registry

router = APIRouter()


@router.get("/get_job_credentials")
async def get_job_credentials(
    username: str = Header(None, alias="Ink-Username"),
    token: str = Header(None, alias="Ink-Token"),
    x_original_uri: str = Header(None, alias="X-Original-URI"),
):
    """Return app-specific credentials for an authenticated proxy request."""
    if not username or not token or not headers.validate_token(username, token):
        return Response(status_code=401)
    if not x_original_uri:
        return Response(status_code=400)

    match = re.match(r"^/(?P<job_type>[^/]+)/[^/]+/(?P<encoded>\d+)/[^/]+", x_original_uri)
    if not match:
        return Response(status_code=400)

    encoded = match.group("encoded")
    if len(encoded) <= 5:
        return Response(status_code=400)

    try:
        job_id = int(encoded[5:])
        uid = change_username_to_uid(username)
        app = computing_registry.try_get(match.group("job_type"))
        if app is None:
            return Response(status_code=404)

        result = await app.connect(job_id=job_id, uid=uid, cluster_id="htcondor")
        credentials = app.get_proxy_credentials(result)
        if not credentials:
            return Response(status_code=500)

        return Response(status_code=204, headers={"X-Job-Credentials": credentials})
    except Exception:
        logger.exception(f"Job proxy credentials failed for {username}, job_id={job_id}")
        return Response(status_code=500)


@router.get("/resolve_job_proxy")
async def resolve_job_proxy(
    username: str = Header(None, alias="Ink-Username"),
    token: str = Header(None, alias="Ink-Token"),
    job_type: str = Query("openchamber"),
):
    """Called by OpenResty internal subrequest.

    Look up the user's running openchamber (or other session-proxied)
    job and return {host, port, passwd}. Apps may provide a storage-backed
    fallback when the scheduler has no usable job. The nginx layer caches
    the response for 600 s.
    """
    if not username or not token or not headers.validate_token(username, token):
        return Response(status_code=401)

    uid = change_username_to_uid(username)
    scheduler = get_scheduler("htcondor", username)
    jobs = await scheduler.query_job(job_type) or []
    app = computing_registry.try_get(job_type)
    if app is None:
        logger.warning("resolve_job_proxy: unknown job_type=%s", job_type)
        return Response(status_code=404)

    running = [
        j for j in jobs
        if j.get("jobStatus") == "RUNNING" and j.get("connect_sign") == "True"
    ]

    job_id = None
    try:
        if running:
            job_id = int(running[0]["jobId"])
            result = await app.connect(
                job_id=job_id,
                uid=uid,
                cluster_id="htcondor",
            )
        else:
            result = await app.resolve_proxy_fallback(
                username=username,
                uid=uid,
                cluster_id="htcondor",
            )
            if result is None:
                return Response(status_code=404)

        return JSONResponse(content={
            "host": result.host,
            "port": result.port,
            "passwd": result.passwd,
        })
    except Exception:
        logger.exception(
            "resolve_job_proxy failed: user=%s job_id=%s", username, job_id
        )
        return Response(status_code=500)


def _normalize_and_filter_jobs(joblist: list[dict]) -> list[dict]:
    normalized = []

    for job in joblist:
        raw_status = str(job.get("jobStatus", "") or "")
        if raw_status.startswith("CANCELLED"):
            # Frontend requirement: do not return cancelled jobs.
            continue

        normalized.append(job)

    return normalized

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
        app = computing_registry.try_get(job_type)
        if app is None:
            return {
                "status": InkStatus.RESOURCE_NOT_SUPPORT,
                "msg": f"Connect job failed with wrong jobtype: {job_type}",
                "data": {},
            }

        result = await app.connect(job_id=job_id, uid=uid, cluster_id=cluster_id)
        return {
            "status": InkStatus.SUCCESS,
            "msg": "Request Success",
            "data": result.to_response(job_id),
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
async def create_normal_job(
    username: str = Depends(get_username),
    jobclass: Union[SLURM_JOB, HTC_JOB] = Body(..., discriminator="cluster_id"),
):
    try:
        adapter = get_scheduler(jobclass.cluster_id, username)
        jobid = await adapter.submit_job(jobclass)

        if isinstance(jobid, dict) and jobid.get("job_status") == "DUPLICATE":
            return {
                "status": InkStatus.DUPLICATE_JOB,
                "msg": jobid.get("error", "Duplicate interactive job type."),
                "data": {}
            }

        return {
            "status": InkStatus.SUCCESS,
            "msg": f"Create job successfully.",
            "data": {
                "jobid": jobid
            }
        }   
    
    except Exception as e:
        logger.exception(f"Create job failed, username: {username}, cluster_id: {jobclass.cluster_id}, details: {e}.")
        return {
            "status": InkStatus.SERVER_INTERNAL_ERROR,
            "msg": f"Create job failed: {e}",
            "data": {}
        }

@router.post("/create_common_job")
async def create_common_job(
    username: str = Depends(get_username),
    jobclass: Union[SLURM_JOB, HTC_JOB] = Body(..., discriminator="cluster_id")
):
    try:
        adapter = get_scheduler(jobclass.cluster_id, username)
        result = await adapter.submit_job(jobclass)

        if isinstance(result, dict) and result.get("job_status") == "DUPLICATE":
            return {
                "status": InkStatus.DUPLICATE_JOB,
                "msg": result.get("error", "Duplicate interactive job type."),
                "data": {}
            }

        if jobclass.submit_mode == SubmitMode.SYNC:
            return {
                "status": InkStatus.SUCCESS,
                "msg": "Submit job synchronously succeeded.",
                "data": result      # contains job_id
            }
        else:
            return {
                "status": InkStatus.SUCCESS,
                "msg": "Submit job asynchronously accepted.",
                "data": result      # async has no job_id yet, but submit_uuid
            }

    except Exception as e:
        logger.exception(f"Failed to create jobs, user: {username}, cluster: {jobclass.cluster_id}, details: {e}.")
        return {
            "status": InkStatus.SERVER_INTERNAL_ERROR,
            "msg": f"Create common job failed: {e}",
            "data": {},
        }


@router.get("/query_jobs")
async def query_common_job(
    username: str = Depends(get_username),
    cluster_id: str = Query(None, description="Cluster ID"),
    job_type: str = Query(None, description="Job type"),
    page: int = Query(1, description="Pangination page"),
    limit: int = Query(5000, description="lines of each page")
):
    try:
        joblist = []
        if not cluster_id:
            try:
                cluster_list = get_config("computing", "cluster_list")
                if not cluster_list:
                    raise ValueError("Get user jobs failed, Empty cluster_list from config.yaml")
                if isinstance(cluster_list, str):
                    cluster_ids = [c.strip() for c in cluster_list.split(",") if c.strip()]
                else:
                    cluster_ids = list(cluster_list)
            except Exception as e:
                logger.exception(f"Get user jobs failed (load cluster_list), username: {username}, details: {e}.")
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
                    logger.exception(f"Get user jobs failed for cluster {cid}, username: {username}, details: {e}.")
        else:
            adapter = get_scheduler(cluster_id, username)
            joblist = await adapter.query_job(job_type) or []

        joblist = _normalize_and_filter_jobs(joblist)

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
        logger.exception(f"Get user jobs failed, username: {username}, cluster_id: {cluster_id}, details: {e}.")
        return {
            "status": InkStatus.SERVER_INTERNAL_ERROR,
            "msg": f"Get user {username} jobs failed: {e}.",
            "data": {}
        }

@router.post("/delete_job")
async def delete_common_job(
    username: str = Depends(get_username),
    submit_uuid: str | None = Body(None, description="use submit_uuid first because of async submission", embed=True),
    job_id: str | None = Body(None, description="job id once submitted", embed=True),
    cluster_id: str = Body(..., description="Cluster ID",embed=True)
):
    try:
        if not job_id and not submit_uuid:
            return {
                "status": InkStatus.BAD_REQUEST,
                "msg": "Delete job failed: either job_id or submit_uuid is required.",
                "data": {}
            }

        adapter = get_scheduler(cluster_id, username)
        await adapter.cancel_job(
            job_id=job_id,
            submit_uuid=submit_uuid
        )

        if job_id:
            return {
                "status": InkStatus.SUCCESS,
                "msg": f"Delete job {job_id} successfully.",
                "data": {}
            }

        return {
            "status": InkStatus.SUCCESS,
            "msg": f"Delete job {submit_uuid} successfully.",
            "data": {}
        }

    except Exception as e:
        logger.error(f"Delete job failed, username: {username}, cluster_id: {cluster_id}, details: {e}.")
        return {
            "status": InkStatus.SERVER_INTERNAL_ERROR,
            "msg": f"Delete job failed: {e}.",
            "data": {}
        }
