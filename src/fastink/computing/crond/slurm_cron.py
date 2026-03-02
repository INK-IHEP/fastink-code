from fastink.common.config import get_config
import json
from fastink.inkdb.inkredis import redis_connect
from fastink.computing.adapter.strategy import get_scheduler
from functools import wraps
from fastink.computing.tools.common.utils import (
    sub_command, 
    change_username_to_uid, 
    parse_sbatch_out_err
)
from fastink.computing.tools.db.db_tools import (
    insert_job_info, 
    update_job_status, 
    get_active_cluster_jobs,
    get_starttime_info,
    get_endtime_info,
    update_start_time,
    update_end_time
)

import logging
logger = logging.getLogger("ink.hpcadapter.slurm_cron")

def is_cluster_enabled(cluster: str) -> bool:
    """
    Check whether submit worker for the given cluster is enabled via YAML config.
    """
    enabled_clusters = get_config("crond", "submit_workers")

    if not isinstance(enabled_clusters, (list, tuple)):
        logger.error("crond.submit_workers must be a list")
        return False

    return cluster in enabled_clusters

def cluster_enabled(cluster: str):
    """
    Decorator to enable/disable a crond worker based on YAML config.
    If the cluster is disabled, the wrapped function becomes a no-op.
    """

    def decorator(func):
        enabled = is_cluster_enabled(cluster)

        if not enabled:
            logger.info(
                f"Crond worker for cluster '{cluster}' is disabled by config"
            )

        @wraps(func)
        async def wrapper(*args, **kwargs):
            if not enabled:
                # Do nothing if this cluster is disabled
                return
            return await func(*args, **kwargs)

        return wrapper

    return decorator

async def submit_slurm_jobs():
    """
    Periodic worker for submitting Slurm async jobs.
    """
    await submit_from_queue("slurm")


async def submit_from_queue(cluster: str):
    """
    Submit jobs from redis queue for the given cluster.
    """
    try:
        r = redis_connect()
        queue_key = f"submitting_jobs:{cluster}"

        while True:
            raw_job = await r.rpop(queue_key)
            if not raw_job:
                break

            try:
                job = json.loads(raw_job)
                logger.info(f"[{cluster}] Pop async job: {job}")

                scheduler = get_scheduler(cluster, job["username"])
                job_id = await scheduler.submit_job_from_queue(job)

                logger.info(
                    f"[{cluster}] Job submitted successfully, job_id={job_id}"
                )

            except Exception as e:
                logger.error(
                    f"[{cluster}] Failed to submit job: ({raw_job}) with error({e})",
                    exc_info=True
                )
                # Optional: push to dead-letter queue
                await r.lpush(f"failed_jobs:{cluster}", raw_job)

    except TimeoutError:
        # Another worker instance is running
        logger.debug(f"[{cluster}] worker job submission is timeout.")
        
        
def _map_slurm_status_to_internal(db_status: str, slurm_state: str) -> str:

    if slurm_state == "PENDING":
        return "QUEUEING"

    if slurm_state == "RUNNING":
        return "RUNNING"

    if slurm_state in ("COMPLETED", "FAILED") or slurm_state.startswith("CANCELLED"):
        return slurm_state

    return db_status

async def slurm_update_job_state(cluster: str):
    """
    Cluster-level Slurm reconciliation using full sacct metadata.
    """

    r = redis_connect()

    sacct_cmd = (
        "sacct -S now-1day "
        "--format=JobID,User,Partition,State,Elapsed,"
        "NNodes,NodeList,WCkey,Submit,Start,End,"
        "WorkDir,Time,SubmitLine "
        "-P -X -n"
    )

    stdout = await sub_command(
        sacct_cmd,
        30,
        "Slurm state sync failed",
        "Slurm state sync timeout",
    )

    lines = stdout.decode().strip().split("\n")

    slurm_jobs = {}

    for line in lines:
        if not line:
            continue

        fields = line.split("|")

        job_id = fields[0].strip()

        slurm_jobs[job_id] = {
            "username": fields[1],
            "partition": fields[2],
            "state": fields[3],
            "job_type": fields[7],
            "submit": fields[8],
            "start": fields[9],
            "end": fields[10],
            "workdir": fields[11],
            "submit_line": fields[13],
        }

    if not slurm_jobs:
        return

    db_jobs = get_active_cluster_jobs(cluster)
    db_job_map = {j.job_id: j for j in db_jobs}

    # -----------------------------------------------------
    # Case A: Slurm has but DB missing -> insert full info
    # -----------------------------------------------------
    for job_id, info in slurm_jobs.items():

        if job_id not in db_job_map:

            uid = change_username_to_uid(info["username"])
            if not uid:
                continue

            # Parse out/err from submit line
            out_path, err_path = parse_sbatch_out_err(
                info["submit_line"],
                job_id,
            )

            insert_job_info(
                uid=uid,
                job_id=job_id,
                out_path=out_path,
                err_path=err_path,
                job_type=info["job_type"],
                workdir=info["workdir"],
                clusterid=cluster,
            )

    # -----------------------------------------------------
    # Case B/C: reconcile state
    # -----------------------------------------------------
    for job in db_jobs:

        job_id = job.job_id
        db_status = job.status

        slurm_info = slurm_jobs.get(job_id)
        if not slurm_info:
            continue

        new_status = _map_slurm_status_to_internal(
            db_status,
            slurm_info["state"],
        )

        if new_status != db_status:

            update_job_status(job.uid, job_id, new_status, cluster)

            uuid_val = await r.get(
                f"job_id_to_submit_uuid:{cluster}:{job_id}"
            )

            if uuid_val:
                submit_uuid = (
                    uuid_val.decode()
                    if isinstance(uuid_val, bytes)
                    else uuid_val
                )

                await r.hset(
                    f"job_status:{cluster}:{submit_uuid}",
                    "jobStatus",
                    new_status,
                )
                

async def slurm_update_job_time(cluster: str):
    """
    Synchronize start and end times from Slurm into DB.
    Uses single sacct call (last 1 day).
    """

    sacct_cmd = (
        "sacct -S now-1day "
        "--format=JobID,User,Start,End "
        "-P -X -n"
    )

    stdout = await sub_command(
        sacct_cmd,
        20,
        "Slurm time sync failed",
        "Slurm time sync timeout",
    )

    lines = stdout.decode().strip().split("\n")

    for line in lines:
        if not line:
            continue

        fields = line.split("|")
        job_id = fields[0].strip()
        username = fields[1].strip()
        start_time = fields[2]
        end_time = fields[3]

        uid = change_username_to_uid(username)
        if not uid:
            continue

        if start_time and start_time not in ("", "Unknown"):
            if not get_starttime_info(uid, job_id, cluster):

                update_start_time(
                    uid,
                    job_id,
                    start_time.replace("T", " "),
                    cluster,
                )

        if end_time and end_time not in ("", "Unknown"):
            if not get_endtime_info(uid, job_id, cluster):

                update_end_time(
                    uid,
                    job_id,
                    end_time.replace("T", " "),
                    cluster,
                )