import json, shlex
import traceback
from shlex import quote
from pathlib import Path
from fastapi import APIRouter
from src.common.logger import logger
from src.common.config import get_config
from filelock import FileLock, Timeout
from fastapi_utils.tasks import repeat_every
from src.inkdb.inkredis import redis_connect
from src.computing.tools.db.db_tools import needto_change_status_jobs
from src.computing.tools.db.db_tools import update_end_time, update_job_status, update_start_time, get_jobs_with_null_times, delete_jobinfo_by_jobids, insert_job_info
from src.computing.tools.common.utils import safe_get, safe_int, ts_to_str, sub_command, delete_iptables, change_username_to_uid, init_job_dir, generate_condor_submit, generate_submit_command

router = APIRouter()

def query_cluster_jobs():

    SCHEDD_HOST = get_config("computing", "schedd_host")

    BASE_CMD = f"condor_q -name {quote(SCHEDD_HOST)}"
    BASE_ATTRS = [
        "Owner", "ClusterId", "ProcId", "HepJob_RealGroup", "Qdate",
        "JobStatus", "JobStartDate", "RemoteHost", "HepJob_JobType", "HepJob_RequestOS"
    ]
    EXTRA_ATTRS = ["Iwd", "Out", "Err", "holdreason"]
    attrs = BASE_ATTRS + EXTRA_ATTRS

    command = (
        f"{BASE_CMD} "
        f"-af:V {' '.join(attrs)}"
    )

    return command


def get_condor_history_command(job_id: str) -> str:
    SCHEDD_HOST = get_config("computing", "schedd_host")
    BASE_CMD = f"condor_history -name {quote(SCHEDD_HOST)} -limit 1"
    ATTRS = [
        'formatTime(EnteredCurrentStatus,"%Y-%m-%d %H:%M:%S")',
        'ifThenElse(isUndefined(JobStartDate),"NULL",formatTime(JobStartDate,"%Y-%m-%d"))',
        'ifThenElse(isUndefined(JobStartDate),"NULL",formatTime(JobStartDate,"%H:%M:%S"))',
        'formatTime(QDate,"%Y-%m-%d %H:%M:%S")',
        "HepJob_JobType",
        "Owner"
    ]
    attrs_quoted = " ".join(quote(a) for a in ATTRS)   # 关键：给每个字段加 shell 引号
    command = f"{BASE_CMD} {quote(str(job_id))} -af {attrs_quoted}"

    logger.info(f"The history command: {command}")

    return command


LOCK_PATH1 = Path("src") / "computing" / "crond" / "lock1"
@router.on_event("startup")
@repeat_every(seconds=5, wait_first=False, raise_exceptions=False, logger=logger)
async def update_completed_jobs():
    lock = FileLock(str(LOCK_PATH1), timeout=0.1)  
    cluster_jobs: dict[str, list[dict]] = {}
    try:
        with lock:
            
            iptables_jobtype = get_config("computing", "iptables_jobtype")
            need_change_status_jobs = needto_change_status_jobs()
            query_command = query_cluster_jobs()
            stdout = await sub_command(query_command, 10, "Query user jobs failed.", "Query user jobs timeout.")
            lines = stdout.decode().strip().split('\n')
            to_delete = []
            
            if lines != ['']:
                for line in lines:
                    job_param_list = shlex.split(line, posix=True)

                    job_owner = safe_get(job_param_list, 0)
                    job_owner = job_owner.strip().strip('"').strip("'") if job_owner else ""
                    job_clusterid = safe_int(safe_get(job_param_list, 1), default=None)
                    qdate_ts = safe_int(safe_get(job_param_list, 4), default=None)
                    start_ts = safe_int(safe_get(job_param_list, 6), default=None)

                    job_submit_time = ts_to_str(qdate_ts)
                    job_start_time  = ts_to_str(start_ts)

                    job_status = safe_get(job_param_list, 5)
                    job_remote_host = safe_get(job_param_list, 7)
                    job_type = safe_get(job_param_list, 8)
                    job_request_os = safe_get(job_param_list, 9)
                    job_iwd = safe_get(job_param_list, 10)
                    job_out_path = safe_get(job_param_list, 11)
                    job_err_path = safe_get(job_param_list, 12)
                    job_hold_reason = " ".join(job_param_list[13:]) if len(job_param_list) > 13 else ""

                    cluster_jobs.setdefault(job_owner, []).append(
                        {
                            "ClusterId": "HTCondor",
                            "jobId": job_clusterid,
                            "jobType": job_type,
                            "jobStatus": job_status,
                            "jobSubmitTime": job_submit_time,
                            "jobStartTime": job_start_time,
                            "jobNodeList": job_remote_host,
                            "jobrunos": job_request_os,
                            "jobiwd": job_iwd,
                            "joboutpath": job_out_path,
                            "joberrpath": job_err_path,
                            "hold_reason": job_hold_reason
                        }
                    )
                
                    if job_clusterid in need_change_status_jobs.keys():
                        del need_change_status_jobs[job_clusterid]

            r = redis_connect()
            await r.set("cluster_jobs", json.dumps(cluster_jobs, ensure_ascii=False))
            #logger.debug(f"HTCondor joblist insert to redis, the list: {cluster_jobs}")

            if need_change_status_jobs:
                
                for key in need_change_status_jobs:
                    query_history_command = get_condor_history_command(key)
                    stdout = await sub_command(query_history_command, 30, "Exec condorhistory func failed.", "Exec condorhistory func timeout.")
                    history_job_lines = stdout.decode().strip().split('\n')
                    #logger.debug(f"The history result: {history_job_lines}")

                    if history_job_lines != [""]:
                        job_param_list = history_job_lines[0].split()
                        job_end_time = f"{job_param_list[0]} {job_param_list[1]}" 
                        if job_param_list[2] != "NULL":
                            job_start_time = f"{job_param_list[2]} {job_param_list[3]}"
                        else:
                            job_start_time = f"{job_param_list[4]} {job_param_list[5]}"
                        job_type = job_param_list[6]
                        job_user = job_param_list[7]
                        job_uid = change_username_to_uid(job_user)

                        if job_type in iptables_jobtype:
                            gateway_port = need_change_status_jobs[key][1]
                            sshd_job_iptables_clean = need_change_status_jobs[key][2]
                            if gateway_port != 0 and sshd_job_iptables_clean == 0:
                                delete_iptables(job_uid, key, gateway_port, "htcondor")
                        update_job_status(job_uid, key, 'COMPLETED', "htcondor")
                        update_start_time(job_uid, key, job_start_time, "htcondor")
                        update_end_time(job_uid, key, job_end_time, "htcondor")
                        logger.debug(f"Update job {key} status to COMPLETED.")
                    
                    else:
                        to_delete.append(key)
                
                if to_delete:
                    logger.debug(f"Need to delete jobs: {to_delete}")
                    delete_jobinfo_by_jobids(to_delete)
                        
    except Timeout:
        logger.debug("update_completed_jobs: lock busy, skip this tick")
    
    except Exception:
        logger.exception("update_completed_jobs: failed")




def gen_history_list_command() -> str:
    SCHEDD_HOST = get_config("computing", "schedd_host")
    BASE_CMD = f"condor_history -name {quote(SCHEDD_HOST)} "
    ATTRS = [
        'formatTime(EnteredCurrentStatus,"%Y-%m-%d %H:%M:%S")',
        'ifThenElse(isUndefined(JobStartDate),"NULL",formatTime(JobStartDate,"%Y-%m-%d"))',
        'ifThenElse(isUndefined(JobStartDate),"NULL",formatTime(JobStartDate,"%H:%M:%S"))',
        'formatTime(QDate,"%Y-%m-%d %H:%M:%S")',
        "Owner",
        "ClusterId"
    ]
    attrs_quoted = " ".join(quote(a) for a in ATTRS)
    command = f"{BASE_CMD} -af {attrs_quoted}"

    logger.debug(f"The reset DB history command: {command}")

    return command


LOCK_PATH2 = Path("src") / "computing" / "crond" / "lock2"
@router.on_event("startup")
@repeat_every(seconds=3600, wait_first=False, raise_exceptions=False, logger=logger)
async def resert_start_end_time():
    lock = FileLock(str(LOCK_PATH2), timeout=0.1)  
    try:
        with lock:
            time_null_jobs = get_jobs_with_null_times()
            logger.info(f"The DB time null jobs: {time_null_jobs}")
            
            if not time_null_jobs:
                return

            time_null_set = set(map(str, time_null_jobs))
            
            query_command = gen_history_list_command()
            stdout = await sub_command(query_command, 20, "Query history jobs failed.", "Query history jobs timeout.")
            lines = stdout.decode(errors="ignore").splitlines()
            lines = [ln for ln in lines if ln.strip()]

            history_ids = set()
            history_map = {}

            for ln in lines:
                parts = ln.split()
                end_time = f"{parts[0]} {parts[1]}"

                if parts[2] != "NULL":
                    start_time = f"{parts[2]} {parts[3]}"
                else:
                    start_time = f"{parts[4]} {parts[5]}"

                user = parts[6]
                clusterid = parts[7]
                uid = change_username_to_uid(user)

                history_ids.add(clusterid)
                history_map[clusterid] = (uid, start_time, end_time, user)

            found = time_null_set & history_ids
            missing = time_null_set - history_ids
            
            for clusterid in found:
                uid, start_time, end_time, user = history_map[clusterid]
                update_start_time(uid, clusterid, start_time, "htcondor")
                update_end_time(uid, clusterid, end_time, "htcondor")
                logger.debug(f"Update {user} job {clusterid} start and end time in DB.")
                
            stdout_q = await sub_command(
                query_cluster_jobs(),
                10,
                "Query cluster jobs failed.",
                "Query cluster jobs timeout."
            )
            q_lines = [ln for ln in stdout_q.decode(errors="ignore").splitlines() if ln.strip()]
            active_jobs = set()
            for ln in q_lines:
                parts = ln.split()
                if len(parts) >= 2:
                    active_jobs.add(parts[1])

            delete_jobs = missing - active_jobs
            
            if delete_jobs:
                delete_jobinfo_by_jobids(list(delete_jobs))
            
    except Timeout:
        logger.debug("resert_start_end_time: lock busy, skip this tick")
    
    except Exception:
        logger.exception("resert_start_end_time: failed")


LOCK_PATH3 = Path("src") / "computing" / "crond" / "lock3"
@router.on_event("startup")
@repeat_every(seconds=5, wait_first=False, raise_exceptions=False, logger=logger)
async def submit_job_from_redis():
    lock = FileLock(str(LOCK_PATH3), timeout=0.1)  
    try:
        with lock:   
            r = redis_connect()
            while True:
                try:
                    raw_job = await r.rpop("submitting_jobs")
                    if not raw_job:
                        break

                    job = json.loads(raw_job)

                    job_owner = job.get("username")
                    job_type = job.get("jobType")
                    job_cpu = job.get("jobReqCPU")
                    job_mem = job.get("jobReqMEM")
                    job_os = job.get("jobReqOS")
                    job_wn = job.get("jobReqWN")
                    job_arch = job.get("jobReqARCH")
                    job_params = job.get("jobReqParam")
                    cluster_id = job.get("clusterId")

                    uid = change_username_to_uid(job_owner)
                    job_dir = await init_job_dir(job_owner, job_type)
                    logger.debug(f"HTC-LOG: Init user dir {job_dir} successfully.")

                    submit_file = await generate_condor_submit(job_owner, job_cpu, job_mem, job_type, job_dir, job_os, job_wn, job_arch, job_params)
                    submit_command = generate_submit_command(job_owner, job_dir, job_type, submit_file)
                    logger.debug(f"HTC-LOG: Generate User {job_owner} submit command {submit_command} finished.")

                    stdout = await sub_command(submit_command, 10, "submit job failed.", "submit job timeout.")
                    job_id_line = stdout.decode().strip()
                    job_id = job_id_line.split()[-1].rstrip('.')
                    output = f"{job_dir}/{job_id}.out"
                    errpath = f"{job_dir}/{job_id}.err"
                    logger.debug(f"HTC-LOG: Submit User {job_owner} job {job_type} {job_id} to cluster.")

                    insert_job_info(uid, job_id, output, errpath, job_type, job_dir, cluster_id)
                    logger.debug(f"HTC-LOG: Submit {job_owner} job {job_id} to queue.")
                    await r.lrem(f"{job_owner}_submitting_jobs", 1, raw_job)

                except Exception as e:
                    if job_owner:
                        await r.lrem(f"{job_owner}_submitting_jobs", 1, raw_job)
                    logger.exception(f"HTC-LOG: Submit job failed, {e}")
                    continue

    except Timeout:
        logger.debug("submit_job_from_redis: lock busy, skip this tick")
        
    except Exception as e:
        logger.error(f"HTC-LOG: Some Wrong in Submit job, the details: {e}")
        raise e
    

            