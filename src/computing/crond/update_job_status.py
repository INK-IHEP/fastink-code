from shlex import quote
from pathlib import Path
from fastapi import APIRouter
from src.common.logger import logger
from src.common.config import get_config
from filelock import FileLock, Timeout
from fastapi_utils.tasks import repeat_every
from src.computing.tools.db.db_tools import needto_change_status_jobs
from src.computing.tools.common.utils import sub_command, delete_iptables, change_username_to_uid
from src.computing.tools.db.db_tools import update_end_time, update_job_status, update_start_time, get_jobs_with_null_times


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
        f"-af {' '.join(attrs)}"
    )

    return command


def get_condor_history_command(job_id: str) -> str:
    SCHEDD_HOST = get_config("computing", "schedd_host")
    BASE_CMD = f"condor_history -name {quote(SCHEDD_HOST)} -limit 1"
    ATTRS = [
        'formatTime(EnteredCurrentStatus,"%Y-%m-%d %H:%M:%S")',
        "HepJob_JobType",
        "Owner",
        'formatTime(JobStartDate,"%Y-%m-%d %H:%M:%S")'
    ]
    attrs_quoted = " ".join(quote(a) for a in ATTRS)   # 关键：给每个字段加 shell 引号
    command = f"{BASE_CMD} {quote(str(job_id))} -af {attrs_quoted}"

    logger.info(f"The history command: {command}")

    return command


LOCK_PATH1 = Path("src") / "computing" / "crond" / "lock1"
@router.on_event("startup")
@repeat_every(seconds=300, wait_first=False, raise_exceptions=True, logger=logger)
async def update_completed_jobs():
    lock = FileLock(str(LOCK_PATH1), timeout=0.1)  
    try:
        with lock:
            iptables_jobtype = get_config("computing", "iptables_jobtype")
            need_change_status_jobs = needto_change_status_jobs()
            query_command = query_cluster_jobs()
            stdout = await sub_command(query_command, 10, "Query user jobs failed.", "Query user jobs timeout.")
            lines = stdout.decode().strip().split('\n')
            
            if lines != ['']:
                for line in lines:
                    job_param_list = line.split()
                    job_clusterid = int(job_param_list[1])
                
                    if job_clusterid in need_change_status_jobs.keys():
                        del need_change_status_jobs[job_clusterid]

            if need_change_status_jobs:
                for key in need_change_status_jobs:
                    query_history_command = get_condor_history_command(key)
                    stdout = await sub_command(query_history_command, 30, "Exec condorhistory func failed.", "Exec condorhistory func timeout.")
                    history_job_lines = stdout.decode().strip().split('\n')
                    logger.info(f"The history result: {history_job_lines}")

                    if history_job_lines != [""]:
                        job_param_list = history_job_lines[0].split()
                        job_end_time = f"{job_param_list[0]} {job_param_list[1]}" 
                        job_type = job_param_list[2]
                        job_user = job_param_list[3]
                        job_start_time = f"{job_param_list[4]} {job_param_list[5]}"
                        job_uid = change_username_to_uid(job_user)

                        if job_type in iptables_jobtype:
                            gateway_port = need_change_status_jobs[key][1]
                            sshd_job_iptables_clean = need_change_status_jobs[key][2]
                            if gateway_port != 0 and sshd_job_iptables_clean == 0:
                                delete_iptables(job_uid, key, gateway_port, "htcondor")
                        update_job_status(job_uid, key, 'COMPLETED', "htcondor")
                        update_start_time(job_uid, key, job_start_time, "htcondor")
                        update_end_time(job_uid, key, job_end_time, "htcondor")
                        logger.info(f"Update job {key} status to COMPLETED.")

    except Timeout:
        logger.info("update_completed_jobs: lock busy, skip this tick")
    
    except Exception:
        logger.exception("update_completed_jobs: failed")




def gen_history_list_command() -> str:
    SCHEDD_HOST = get_config("computing", "schedd_host")
    BASE_CMD = f"condor_history -name {quote(SCHEDD_HOST)} -limit 200"
    ATTRS = [
        'formatTime(EnteredCurrentStatus,"%Y-%m-%d %H:%M:%S")',
        'formatTime(JobStartDate,"%Y-%m-%d %H:%M:%S")',
        "Owner",
        "ClusterId"
    ]
    attrs_quoted = " ".join(quote(a) for a in ATTRS)
    command = f"{BASE_CMD} -af {attrs_quoted}"

    logger.info(f"The history command: {command}")

    return command



LOCK_PATH2 = Path("src") / "computing" / "crond" / "lock2"
@router.on_event("startup")
@repeat_every(seconds=1800, wait_first=False, raise_exceptions=True, logger=logger)
async def resert_start_end_time():
    lock = FileLock(str(LOCK_PATH2), timeout=0.1)  
    try:
        with lock:
            time_null_jobs = get_jobs_with_null_times()
            query_command = gen_history_list_command()
            stdout = await sub_command(query_command, 10, "Query history jobs failed.", "Query history jobs timeout.")
            history_jobs_lines = stdout.decode().strip().split('\n')

            if history_jobs_lines != [""]:
                for job_line in history_jobs_lines:
                    job_param_list = job_line.split()
                    if len(job_param_list) == 6: 
                        job_end_time = f"{job_param_list[0]} {job_param_list[1]}" 
                        job_start_time = f"{job_param_list[2]} {job_param_list[3]}"
                        job_user = job_param_list[4]
                        job_clusterid = job_param_list[5]
                        job_uid = change_username_to_uid(job_user)

                        if job_clusterid in time_null_jobs:
                            update_start_time(job_uid, job_clusterid, job_start_time, "htcondor")
                            update_end_time(job_uid, job_clusterid, job_end_time, "htcondor")
                            logger.info(f"Update {job_user} job {job_clusterid} start and end time in DB.")

    except Timeout:
            logger.info("update_completed_jobs: lock busy, skip this tick")
    
    except Exception:
        logger.exception("update_completed_jobs: failed")

    

