from fastapi import HTTPException
from src.computing.tools.resources_utils import change_uid_to_username, sub_command
from src.computing.hpc.hpc_job_details import *

# delete job
async def delete_job(jobid, uid):
    user_name = change_uid_to_username(uid)
    try:
        scancel_command = f"sudo -u {user_name} scancel {jobid}"
        _ = await sub_command(scancel_command, timeoutsec=5, errinfo="scancel err",tminfo="scancel timeout")
        
        job_type, _, job_iptables_status, job_iptables_clean = get_job_info(uid, jobid, "slurm")
        
        if job_type == "enode":
            if job_iptables_status != 0 and job_iptables_clean == 0:
                delete_iptables(uid, jobid, job_iptables_status, 'slurm')
        update_job_status(uid, jobid, 'COMPLETED', "slurm")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
