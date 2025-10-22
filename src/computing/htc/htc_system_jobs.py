from src.computing.tools.resources_utils import *
from src.inkdb.inkredis import *

'''
Author:         guocq@ihep.ac.cn
Created:        2024-12-18
Last Modified:  2024-12-18
'''

async def get_system_jobs():
    
    try: 
        r = redis_connect()
        job_total_list = await r.get('system_job_list')

        if job_total_list:
            job_total_list = json.loads(job_total_list)
            formatted_job = {key.capitalize(): value for key, value in job_total_list.items()}
            
            return formatted_job
    
        else:
            job_total_list = {"enode": 0, "jupyter": 0, "vscode": 0, "rootbrowse": 0, "vnc": 0}
            job_queue_list = {"enode": 0, "jupyter": 0, "vscode": 0, "rootbrowse": 0, "vnc": 0}
            job_running_list = {"enode": 0, "jupyter": 0, "vscode": 0, "rootbrowse": 0, "vnc": 0}
            
            
            SCHEDD_HOST = get_config("computing", "schedd_host")
        
            command = (
                f"condor_q "
                f"-name {quote(SCHEDD_HOST)} "
                f"-const 'HepJob_JobType == \"enode\" || HepJob_JobType == \"jupyter\" || HepJob_JobType == \"vscode\" || HepJob_JobType == \"rootbrowse\" || HepJob_JobType == \"vnc\"' "
                "-af Owner ClusterId ProcId HepJob_RealGroup Qdate JobStatus JobStartDate RemoteHost HepJob_JobType"
            )
            
            stdout = await sub_command(command, 5, "Get queue jobs failed.", "Get queue jobs timeout.")
            
            lines = stdout.decode().strip().split('\n')
            if lines != ['']:
                for line in lines:
                    job_param_list = line.split()
                    HepJob_JobType = job_param_list[8]

                    if job_param_list[5] == '1':
                        job_queue_list[HepJob_JobType] += 1

                    elif job_param_list[5] == '2':
                        job_running_list[HepJob_JobType] += 1
              
            for key in job_total_list:
                job_total_list[key] = job_queue_list[key] + job_running_list[key]
                
            await r.setex('system_job_list', 600, json.dumps(job_total_list))    
                
            formatted_job = {key.capitalize(): value for key, value in job_total_list.items()}    
        
            return formatted_job
            
    except Exception as e:
        logger.error(f"Get system job statistic info failed in get_system_jobs func, details: {e}.")
        raise e

        
        


