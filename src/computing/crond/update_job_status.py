from shlex import quote
from pathlib import Path
from fastapi import APIRouter
from src.common.logger import logger
from src.common.config import get_config
from filelock import FileLock, Timeout
from fastapi_utils.tasks import repeat_every
from src.computing.tools.db.db_tools import needto_change_status_jobs
from src.computing.tools.db.db_tools import update_end_time, update_job_status
from src.computing.tools.common.utils import sub_command, delete_iptables, change_username_to_uid


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
    ]
    attrs_quoted = " ".join(quote(a) for a in ATTRS)   # 关键：给每个字段加 shell 引号
    command = f"{BASE_CMD} {quote(str(job_id))} -af {attrs_quoted}"
    return command


LOCK_PATH = Path("src") / "computing" / "crond" / "lockfile"
@router.on_event("startup")
@repeat_every(seconds=60, wait_first=False, raise_exceptions=False, logger=logger)
async def update_completed_jobs():

    lock = FileLock(str(LOCK_PATH), timeout=0.1)  
    try:
        with lock:
            logger.info("update_completed_jobs: lock acquired")

            iptables_jobtype = get_config("computing", "iptables_jobtype")
            need_change_status_jobs = needto_change_status_jobs()
            query_command = query_cluster_jobs()
            stdout = await sub_command(
                query_command, 10,
                "Query user jobs failed.", "Query user jobs timeout."
            )
            lines = stdout.decode().strip().splitlines()

            if lines and lines != ['']:
                for line in lines:
                    parts = line.split()
                    if len(parts) >= 2:
                        job_clusterid = int(parts[1])
                        need_change_status_jobs.pop(job_clusterid, None)

            if need_change_status_jobs:
                for key, v in need_change_status_jobs.items():
                    query_history_command = get_condor_history_command(key)
                    stdout = await sub_command(
                        query_history_command, 30,
                        "Exec condorhistory func failed.", "Exec condorhistory func timeout."
                    )
                    history_job_lines = stdout.decode().strip().splitlines()
                    logger.info("history for %s: %s", key, history_job_lines)

                    if not history_job_lines or history_job_lines == ['']:
                        logger.warning("No history for job %s", key)
                        continue

                    cols = history_job_lines[0].split()
                    if len(cols) < 3:
                        logger.warning("Unexpected history format for job %s: %s", key, history_job_lines[0])
                        continue

                    job_end_time, job_type, job_user = cols[0], cols[1], cols[2]
                    job_uid = change_username_to_uid(job_user)

                    if job_type in iptables_jobtype:
                        gateway_port = v[1]
                        sshd_job_iptables_clean = v[2]
                        if gateway_port != 0 and sshd_job_iptables_clean == 0:
                            delete_iptables(job_uid, key, gateway_port, "htcondor")

                    update_job_status(job_uid, key, 'COMPLETED', "htcondor")
                    update_end_time(job_uid, key, job_end_time, "htcondor")
                    logger.info("Update job %s status to COMPLETED.", key)

            logger.info("update_completed_jobs: done")

    except Timeout:
        # 拿不到锁就直接跳过，避免把事件循环卡住
        logger.info("update_completed_jobs: lock busy, skip this tick")
    except Exception:
        # raise_exceptions=False + 打完整堆栈，不影响服务继续跑
        logger.exception("update_completed_jobs: failed")

# @router.on_event("startup")
# @repeat_every(seconds=60, wait_first=False, raise_exceptions=True, logger=logger)
# async def update_completed_jobs():
#     with FileLock(str(Path("src") / "computing" / "crond" / "lockfile")):
#         iptables_jobtype = get_config("computing", "iptables_jobtype")
#         need_change_status_jobs = needto_change_status_jobs()
#         query_command = query_cluster_jobs()
#         stdout = await sub_command(query_command, 10, "Query user jobs failed.", "Query user jobs timeout.")
#         lines = stdout.decode().strip().split('\n')
        
#         if lines != ['']:
#             for line in lines:
#                 job_param_list = line.split()
#                 job_clusterid = int(job_param_list[1])
            
#                 if job_clusterid in need_change_status_jobs.keys():
#                     del need_change_status_jobs[job_clusterid]


#         if need_change_status_jobs:
#             for key in need_change_status_jobs:
#                 query_history_command = get_condor_history_command(key)
#                 stdout = await sub_command(query_history_command, 30, "Exec condorhistory func failed.", "Exec condorhistory func timeout.")
#                 history_job_lines = stdout.decode().strip().split('\n')
#                 logger.info(f"The history command result: {history_job_lines}")

#                 job_end_time = history_job_lines[0][0]
#                 job_type = history_job_lines[0][1]
#                 job_user = history_job_lines[0][2]
#                 job_uid = change_username_to_uid(job_user)
#                 logger.info(f"The job_end_time: {job_end_time}, jobType: {job_type}, job_user: {job_user}")

#                 if job_type in iptables_jobtype:
#                     gateway_port = need_change_status_jobs[key][1]
#                     sshd_job_iptables_clean = need_change_status_jobs[key][2]
#                     if gateway_port != 0 and sshd_job_iptables_clean == 0:
#                         delete_iptables(job_uid, key, gateway_port, "htcondor")
#                 update_job_status(job_uid, key, 'COMPLETED', "htcondor")
#                 update_end_time(job_uid, key, job_end_time, "htcondor")
#                 logger.info(f"Update job {key} status to COMPLETED.")




@router.on_event("startup")
@repeat_every(seconds=60, wait_first=False, raise_exceptions=True, logger=logger)
async def test_task():
    logger.info("测试任务正在运行")