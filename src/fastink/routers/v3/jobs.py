import re
from typing import Union

from fastapi import APIRouter, Body, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.exc import NoResultFound
from starlette.concurrency import run_in_threadpool

from fastink.auth.principal import Principal
from fastink.auth.scopes import require_scope
from fastink.common.config import get_config
from fastink.common.logger import logger
from fastink.computing.adapter.strategy import get_scheduler
from fastink.computing.cluster.cluster import HTC_JOB, SLURM_JOB
from fastink.computing.hpc.v2 import hpc_check_job
from fastink.computing.htc import htc_check_job
from fastink.computing.tools.common.failed_jobs import get_failed_job
from fastink.computing.tools.common.utils import change_username_to_uid
from fastink.computing.tools.db.db_tools import get_job_info
from fastink.inkdb.inkredis import redis_connect

router = APIRouter(tags=["jobs"])

_JOB_TYPE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.\-]{0,63}")
_CLUSTER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.\-]{0,31}")
_JOB_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.\-]{0,63}")
_SUBMIT_UUID_RE = re.compile(r"[A-Za-z0-9_\-]{1,128}")


def _validate_job_type(value: str) -> None:
    if not _JOB_TYPE_RE.fullmatch(value) or value in (".", "..") or value.startswith("."):
        raise HTTPException(status_code=400, detail="Invalid job_type")


def _validate_cluster_id(value: str) -> None:
    if not _CLUSTER_RE.fullmatch(value) or value in (".", ".."):
        raise HTTPException(status_code=400, detail="Invalid cluster_id")


def _validate_job_id(value: str) -> None:
    if not _JOB_ID_RE.fullmatch(value):
        raise HTTPException(status_code=400, detail="Invalid job_id")


def _validate_submit_uuid(value: str) -> None:
    if not _SUBMIT_UUID_RE.fullmatch(value):
        raise HTTPException(status_code=400, detail="Invalid submit_uuid")


def _scheduler_for(cluster_id: str, username: str):
    try:
        return get_scheduler(cluster_id, username)
    except (ValueError, KeyError) as error:
        logger.warning("v3 scheduler lookup failed for %s on %s: %s", username, cluster_id, error)
        raise HTTPException(status_code=400, detail=f"Invalid cluster or user: {error}")


@router.get("/jobs")
async def list_jobs(
    principal: Principal = require_scope("jobs:read", user_only=True),
    cluster_id: str = Query(None),
    job_type: str = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(5000, ge=1, le=5000),
):
    username = principal.username
    joblist = []
    if not cluster_id:
        cluster_list = get_config("computing", "cluster_list")
        if isinstance(cluster_list, str):
            cluster_ids = [c.strip() for c in cluster_list.split(",") if c.strip()]
        else:
            cluster_ids = list(cluster_list) if cluster_list else []
    else:
        cluster_ids = [cluster_id]

    for cid in cluster_ids:
        try:
            sched = get_scheduler(cid, username)
            jobs = await sched.query_job(job_type)
            if jobs:
                joblist.extend(jobs)
        except Exception as e:
            logger.error("Query jobs failed for %s on %s: %s", username, cid, e)

    start = (page - 1) * limit
    end = start + limit
    return {"jobs": joblist[start:end], "total": len(joblist), "page": page}


@router.post(
    "/jobs",
    status_code=202,
    responses={201: {"description": "Synchronous submission created"}},
)
async def submit_job(
    job: Union[SLURM_JOB, HTC_JOB] = Body(..., discriminator="cluster_id"),
    principal: Principal = require_scope("jobs:submit", user_only=True),
):
    """Submit a Slurm or HTCondor job.

    Async submission (the model default) returns 202 with ``submit_uuid`` and
    no ``job_id`` yet.  ``submit_mode: "sync"`` returns 201 with a real id.
    """
    _validate_job_type(job.job_type)
    if job.script_path:
        raise HTTPException(
            status_code=400,
            detail="script_path is not allowed in v3; send job_script instead",
        )
    sched = _scheduler_for(job.cluster_id, principal.username)

    try:
        result = await sched.submit_job(job)
    except PermissionError:
        raise HTTPException(status_code=403, detail="Permission denied")
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))
    except Exception:
        logger.exception("v3 job submit failed for %s on %s", principal.username, job.cluster_id)
        raise HTTPException(status_code=502, detail="Job submission failed")

    if isinstance(result, dict) and result.get("job_status") == "DUPLICATE":
        raise HTTPException(
            status_code=409,
            detail=result.get("error") or "Duplicate job",
        )

    if result is None:
        # HTCondor async submission only enqueues the request and returns no
        # handle; it also returns None when the adapter silently dedups, so
        # clients must poll GET /api/v3/jobs (jobStatus=SUBMITTING).
        return JSONResponse(
            {
                "cluster": job.cluster_id,
                "job_id": None,
                "submit_uuid": None,
                "status": "SUBMITTING",
            },
            status_code=202,
        )

    if isinstance(result, int):
        return JSONResponse(
            {
                "cluster": job.cluster_id,
                "job_id": str(result),
                "submit_uuid": None,
                "status": "SUBMITTED",
            },
            status_code=201,
        )

    payload = {
        "cluster": result.get("cluster", job.cluster_id),
        "job_id": result.get("job_id"),
        "submit_uuid": result.get("submit_uuid"),
        "status": result.get("job_status"),
    }
    if result.get("query_hint"):
        payload["query_hint"] = result["query_hint"]
    return JSONResponse(payload, status_code=201 if payload["job_id"] else 202)


class JobCancelRequest(BaseModel):
    cluster_id: str
    job_id: str | None = None
    submit_uuid: str | None = None


@router.post("/jobs/cancel")
async def cancel_job(
    body: JobCancelRequest,
    principal: Principal = require_scope("jobs:cancel", user_only=True),
):
    if not body.job_id and not body.submit_uuid:
        raise HTTPException(status_code=400, detail="job_id or submit_uuid is required")
    if body.job_id and body.submit_uuid:
        raise HTTPException(
            status_code=400, detail="provide either job_id or submit_uuid, not both"
        )
    _validate_cluster_id(body.cluster_id)
    if body.job_id:
        _validate_job_id(body.job_id)
    if body.submit_uuid:
        _validate_submit_uuid(body.submit_uuid)
    if body.cluster_id == "htcondor" and not body.job_id:
        raise HTTPException(
            status_code=400,
            detail="htcondor does not support cancelling a pending submit_uuid",
        )

    if body.job_id:
        try:
            uid = await run_in_threadpool(
                change_username_to_uid, principal.username
            )
        except (KeyError, ValueError):
            raise HTTPException(status_code=400, detail="User account is not available")
        try:
            await run_in_threadpool(
                get_job_info, uid, body.job_id, body.cluster_id
            )
        except NoResultFound:
            try:
                failed_record = await get_failed_job(
                    redis_connect(), principal.username, body.job_id
                )
            except Exception:
                logger.exception(
                    "v3 cancel job lookup failed for %s/%s",
                    principal.username,
                    body.job_id,
                )
                failed_record = None
            if not failed_record:
                raise HTTPException(status_code=404, detail="Job not found")
        except Exception:
            logger.exception(
                "v3 cancel ownership check failed for %s/%s",
                principal.username,
                body.job_id,
            )
            raise HTTPException(status_code=502, detail="Job lookup failed")

    sched = _scheduler_for(body.cluster_id, principal.username)

    try:
        result = await sched.cancel_job(
            job_id=body.job_id, submit_uuid=body.submit_uuid
        )
    except PermissionError:
        raise HTTPException(status_code=403, detail="Permission denied")
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))
    except Exception:
        logger.exception(
            "v3 job cancel failed for %s on %s", principal.username, body.cluster_id
        )
        raise HTTPException(status_code=502, detail="Job cancellation failed")

    if result is None:
        return {
            "cluster": body.cluster_id,
            "job_id": body.job_id,
            "submit_uuid": body.submit_uuid,
            "status": "CANCELLED",
        }

    payload = {
        "cluster": result.get("cluster", body.cluster_id),
        "job_id": result.get("job_id"),
        "submit_uuid": result.get("submit_uuid"),
        "status": result.get("job_status"),
    }
    if "hidden" in result:
        payload["hidden"] = result["hidden"]
    return payload


@router.get("/jobs/{job_id}/output")
async def get_job_output(
    job_id: str,
    principal: Principal = require_scope("jobs:read", user_only=True),
    cluster_id: str = Query(...),
):
    _validate_job_id(job_id)
    _validate_cluster_id(cluster_id)
    username = principal.username

    try:
        failed_record = await get_failed_job(
            redis_connect(), username, job_id
        )
    except Exception:
        logger.exception("v3 failed-job lookup failed for %s/%s", username, job_id)
        failed_record = None
    if failed_record:
        return {
            "job_id": job_id,
            "output": "",
            "error": failed_record.get("failReason", ""),
        }

    try:
        uid = change_username_to_uid(username)
    except (KeyError, ValueError):
        raise HTTPException(status_code=400, detail="User account is not available")

    try:
        if cluster_id == "slurm":
            content, error = await hpc_check_job.get_job_output(
                uid=uid, job_id=job_id, cluster_id=cluster_id
            )
        elif cluster_id == "htcondor":
            content, error = await htc_check_job.get_job_output(
                uid=uid, job_id=job_id, clusterid=cluster_id
            )
        else:
            raise HTTPException(status_code=400, detail="Unknown cluster_id")
    except HTTPException:
        raise
    except Exception:
        logger.exception(
            "v3 job output failed for %s/%s", username, job_id
        )
        raise HTTPException(status_code=502, detail="Job output unavailable")

    return {"job_id": job_id, "output": content, "error": error}
