import datetime
from shlex import quote
from fastapi import FastAPI
from filelock import FileLock
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



# @app.on_event("startup")
# @repeat_every(seconds=1800)
# async def update_completed_jobs():
#     with FileLock("src\computing\crond\lockfile"):
#         need_change_status_jobs = needto_change_status_jobs()
#         query_command = query_cluster_jobs()
#         stdout = await sub_command(query_command, 10, "Query user jobs failed.", "Query user jobs timeout.")
#         lines = stdout.decode().strip().split('\n')
        