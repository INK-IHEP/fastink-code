from fastink.common.config import get_config
import json
import asyncio
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
    update_end_time,
    job_exists
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

@cluster_enabled("slurm")
async def submit_slurm_jobs():
    """
    Periodic worker for submitting Slurm async jobs.
    """
    logger.info("Starting Slurm job submission worker(submit_from_queue)...")
    await submit_from_queue("slurm")


async def submit_from_queue(cluster: str):
    """
    Submit jobs from redis queue for the given cluster.
    Processes both global queue and per-user queues.
    
    Note: If duplicate check is enabled for certain job types,
    jobs may only be stored in per-user queues, not in the global queue.
    """
    try:
        r = redis_connect()
        logger.debug(f"[{cluster}] Connected to Redis")
        global_queue_key = f"submitting_jobs:{cluster}"
        logger.debug(f"[{cluster}] Global queue key: {global_queue_key}")

        # 检查全局队列中的作业数量
        try:
            global_queue_length = await r.llen(global_queue_key)
            logger.debug(f"[{cluster}] Global queue '{global_queue_key}' length: {global_queue_length}")
        except Exception as e:
            logger.warning(f"[{cluster}] Failed to check global queue length: {e}")
            global_queue_length = 0

        job_count = 0
        
        logger.debug(f"[{cluster}] Processing global queue...")
        while True:
            try:
                raw_job = await r.rpop(global_queue_key)
                if not raw_job:
                    if job_count == 0 and global_queue_length > 0:
                        logger.debug(f"[{cluster}] No more jobs in global queue (initial length was {global_queue_length})")
                    break
                
                job_count += 1
                await _process_single_job(r, cluster, raw_job, job_count)

            except Exception as e:
                logger.error(
                    f"[{cluster}] Error processing global queue: {e}",
                    exc_info=True
                )
                break

        logger.debug(f"[{cluster}] Processed {job_count} jobs from global queue")

    except TimeoutError:
        # Another worker instance is running
        logger.debug(f"[{cluster}] worker job submission is timeout.")
    
    except Exception as e:
        logger.error(
            f"[{cluster}] Failed to connect to Redis or initialize queue: {e}",
            exc_info=True
        )


async def _process_single_job(r, cluster: str, raw_job: str, job_number: int):
    """
    Process a single job from the queue.
    """
    try:
        logger.debug(f"[{cluster}] Processing job #{job_number}, raw data: {raw_job}")

        try:
            job = json.loads(raw_job)
            logger.info(f"[{cluster}] Pop async job #{job_number}: {job}")

            scheduler = get_scheduler(cluster, job["username"])
            job_id = await scheduler.submit_job_from_queue(job)

            logger.info(
                f"[{cluster}] Job #{job_number} submitted successfully, job_id={job_id}"
            )

        except json.JSONDecodeError as e:
            logger.error(
                f"[{cluster}] Failed to parse job #{job_number} JSON: ({raw_job}) with error({e})",
                exc_info=True
            )
            # Push unparseable job to dead-letter queue
            await r.lpush(f"failed_jobs:{cluster}", raw_job)

        except Exception as e:
            logger.error(
                f"[{cluster}] Failed to submit job #{job_number}: ({raw_job}) with error({e})",
                exc_info=True
            )

            # Retry policy: allow finite retries then fail
            if isinstance(job, dict):
                retry_count = int(job.get("retry_count", 0)) + 1
                max_retries = int(job.get("max_retries", 3))

                if retry_count <= max_retries:
                    job["retry_count"] = retry_count
                    retry_delay = float(job.get("retry_delay_seconds", 10))
                    await r.lpush(f"submitting_jobs:{cluster}", json.dumps(job, ensure_ascii=False))
                    logger.warning(
                        f"[{cluster}] Retry {retry_count}/{max_retries} for job {job.get('submit_uuid')}, delay {retry_delay}s"
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    await r.lpush(f"failed_jobs:{cluster}", json.dumps(job, ensure_ascii=False))
                    await r.lrem(f"{cluster}_submitting_jobs:{job.get('username')}", 0, json.dumps(job, ensure_ascii=False))
                    logger.error(
                        f"[{cluster}] Dropped job after {max_retries} retries: {job.get('submit_uuid')}"
                    )
            else:
                # Non-parsable job, push to failed queue
                await r.lpush(f"failed_jobs:{cluster}", raw_job)

    except Exception as e:
        logger.error(
            f"[{cluster}] Unexpected error processing job #{job_number}: {e}",
            exc_info=True
        )
        
        
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
    db_job_map = {j["jobid"]: j for j in db_jobs}

    # -----------------------------------------------------
    # Case A: Slurm has but DB missing -> insert full info
    # -----------------------------------------------------
    for job_id, info in slurm_jobs.items():

        if job_id not in db_job_map:

            if job_exists(job_id, cluster):
                continue  # Job already exists in DB

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
                jobid=job_id,
                outpath=out_path,
                errpath=err_path,
                job_type=info["job_type"],
                job_path=info["workdir"],
                clusterid=cluster,
            )

    # -----------------------------------------------------
    # Case B/C: reconcile state
    # -----------------------------------------------------
    for job in db_jobs:

        job_id = job["jobid"]
        db_status = job["status"]

        slurm_info = slurm_jobs.get(job_id)
        if not slurm_info:
            continue

        new_status = _map_slurm_status_to_internal(
            db_status,
            slurm_info["state"],
        )

        if new_status != db_status:

            update_job_status(job["uid"], job_id, new_status, cluster)

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