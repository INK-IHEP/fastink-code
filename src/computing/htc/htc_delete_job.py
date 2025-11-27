from src.computing.tools.resources_utils import *
from src.common.config import get_config

'''
Author:         guocq@ihep.ac.cn
Created:        2024-12-18
Last Modified:  2024-12-18
'''

async def delete_htc_job(jobid, uid):
    
    user_name = change_uid_to_username(uid)
    try:
        
        schedd_host = get_config("computing", "schedd_host")
        cm_host = get_config("computing", "cm_host")
        
        cancel_command = f"sudo -u {user_name} condor_rm -name {schedd_host} -pool {cm_host} {jobid}"

        _ = await sub_command(cancel_command, timeoutsec=5, errinfo="condor_rm job failed", tminfo="condor_rm job timeout")
        
        job_type, _, job_iptables_status, job_iptables_clean = get_job_info(uid, jobid, "htcondor")
        
        if job_type == "enode":
            if job_iptables_status != 0 and job_iptables_clean == 0:
                delete_iptables(uid, jobid, job_iptables_status, 'htcondor')
        update_job_status(uid, jobid, 'COMPLETED', "htcondor")
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
