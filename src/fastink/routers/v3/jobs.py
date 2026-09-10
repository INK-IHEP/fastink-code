from fastapi import APIRouter, Query

from fastink.auth.oidc.principal import Principal
from fastink.auth.scopes import require_scope
from fastink.common.config import get_config
from fastink.common.logger import logger
from fastink.computing.adapter.strategy import scheduler

router = APIRouter(tags=["jobs"])


@router.get("/jobs")
async def list_jobs(
    principal: Principal = require_scope("jobs:read"),
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
            sched = scheduler(username=username, cluster_id=cid)
            jobs = await sched.query_job(job_type)
            if jobs:
                joblist.extend(jobs)
        except Exception as e:
            logger.error("Query jobs failed for %s on %s: %s", username, cid, e)

    start = (page - 1) * limit
    end = start + limit
    return {"jobs": joblist[start:end], "total": len(joblist), "page": page}


@router.get("/jobs/{job_id}/output")
async def get_job_output(
    job_id: str,
    principal: Principal = require_scope("jobs:read"),
    cluster_id: str = Query(...),
):
    username = principal.username
    from fastink.computing.tools.common.utils import get_job_output as _get_output
    content, error = await _get_output(
        uid=None, job_id=job_id, clusterid=cluster_id, username=username
    )
    return {"job_id": job_id, "output": content, "error": error}
