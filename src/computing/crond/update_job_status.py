import datetime
from shlex import quote
from fastapi import FastAPI
from filelock import FileLock
from src.common.logger import logger
from src.common.config import get_config
from fastapi_utils.tasks import repeat_every
from src.computing.tools.common.utils import sub_command
from src.computing.tools.db.db_tools import needto_change_status_jobs

app = FastAPI()

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

async def get_condor_history_command(job_id: str) -> str:

    SCHEDD_HOST = get_config("computing", "schedd_host")
    BASE_CMD = f"condor_history -name {quote(SCHEDD_HOST)} -limit 1"
    ATTRS = [
        'formatTime(EnteredCurrentStatus,"%Y-%m-%d %H:%M:%S")',
        "HepJob_JobType"
    ]
    
    command = (
        f"{BASE_CMD} "
        f"{job_id} "
        f"-af {' '.join(ATTRS)}"
    )

    return command



# @app.on_event("startup")
# @repeat_every(seconds=1800)
# async def update_completed_jobs():
#     with FileLock("src\computing\crond\lockfile"):
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

#                 if history_job_lines != ['']:
                    
                

#                 complete_job_type = user_completed_jobs[key][0]
#                 if complete_job_type == "enode" or complete_job_type == "compile":
#                     gateway_port = user_completed_jobs[key][1]
#                     sshd_job_iptables_clean = user_completed_jobs[key][2]
#                     if gateway_port != 0 and sshd_job_iptables_clean == 0:
#                         _ = delete_iptables(self.UID, key, gateway_port, self.CLUSTER_TYPE)
#                 update_job_status(self.UID, key, 'COMPLETED', self.CLUSTER_TYPE)
#                 logger.info(f"Update job {key} status to COMPLETED.")
