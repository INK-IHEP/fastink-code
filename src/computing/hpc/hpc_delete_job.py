from fastapi import HTTPException
from src.computing.tools.resources_utils import sub_command, change_uid_to_username
from src.computing.hpc.hpc_job_details import *



# 删除作业的API
async def delete_job(jobId,uid):
    user_name = change_uid_to_username(uid)
    try:
        scancel_command = f"sudo -u {user_name} scancel {jobId}"
        
        _ = await sub_command(scancel_command, timeoutsec=5, errinfo="scancel err",tminfo="scancel timeout")
        
        job_type, _, job_iptables_status, job_iptables_clean = get_job_info(uid, jobId, "slurm")
        if job_type == "enode":
            if job_iptables_status != 0 and job_iptables_clean == 0:
                delete_iptables(uid, jobId, job_iptables_status, 'slurm')
        update_job_status(uid, jobId, 'COMPLETED', "slurm")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    
