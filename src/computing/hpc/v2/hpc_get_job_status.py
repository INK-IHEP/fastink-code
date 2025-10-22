import subprocess
from fastapi import HTTPException
from src.computing.tools.resources_utils import change_uid_to_username
from src.computing.common import *
from src.computing.gateway_tools import *
import json
from src.computing.tools.resources_utils import *

async def get_job_status(jobId, uid, cluster_id):

    response_data = {
        "status": 0,
        "msg": "",
        "data": {
            "jobId": jobId,
            "jobStatus": "UNKNOWN"
            }
        }

    try:
        user_name = change_uid_to_username(uid)
        sacct_command = f"sudo -u {user_name} sacct -j {jobId} --format=JobID,state -P"
        result = await sub_command(sacct_command, timeoutsec=2, errinfo="sacct err", tminfo="sacct timeout")

        output_lines = result.strip().split('\n')
        if len(output_lines) < 2:
            return response_data

        job_info = output_lines[1].split('|')
        job_status = job_info[1]
        if job_status == "PENDING":
            job_status = "QUEUEING"

        job_type, db_job_status, job_iptables_status, job_iptables_clean = get_job_info(uid,jobId,cluster_id)
        
        if job_status != db_job_status:
            update_job_status(uid, jobId, job_status, cluster_id)
                    
        if job_type == "enode":
            if job_status == "RUNNING":
                create_iptables(uid, jobId,job_iptables_status, job_iptables_clean,cluster_id)

            if job_status != "QUEUEING" and job_status != "RUNNING" and job_status != "CONFIGURING":
                gateway_port, = get_job_iptables_status(uid, jobId, cluster_id)
                if gateway_port != 0 and job_iptables_clean != 1:
                    delete_iptables(uid, jobId, gateway_port, cluster_id)

        response_data = {
            "status": 200,
            "msg": "请求成功",
            "data": {
                "jobId": jobId,
                "jobStatus": job_status
            }
        }

        return response_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

