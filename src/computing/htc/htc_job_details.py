from zoneinfo import ZoneInfo
from src.computing.tools.resources_utils import *
from shlex import quote

'''
Author:         guocq@ihep.ac.cn
Created:        2024-12-18
Last Modified:  2024-12-18
'''

async def get_user_job_details(uid, jobid, clusterid):
    try:

        job_detail = {
            "clusterId": clusterid,
            "jobId": jobid,
            "jobType": " ",
            "jobSubmitTime": " ",
            "jobStatus": " ",
            "jobStartTime": " ",
            "JobNodeList": " "
        }
        
        SCHEDD_HOST = get_config("computing", "schedd_host")
        
        command = (
            f"condor_q {quote(str(jobid))} "
            f"-name {quote(SCHEDD_HOST)} "
            "-af Owner ClusterId ProcId HepJob_RealGroup Qdate JobStatus JobStartDate RemoteHost HepJob_JobType"
        )

        stdout = await sub_command(command, 5, "Command condor_q execution failed", "Command condor_q execution timeout.")

        if stdout.decode() == "":
            
            history_command = (
                f"condor_history {quote(str(jobid))} "
                "-limit 1 "                     
                f"-name {quote(SCHEDD_HOST)} "
                "-af Owner ClusterId ProcId HepJob_RealGroup Qdate JobStatus JobStartDate RemoteHost HepJob_JobType LastRemoteHost"
            )
            stdout = await sub_command(history_command, 5, "Command condor_history execution failed", "Command condor_history execution timeout.")
            
        lines = stdout.decode().strip().split('\n')
        if lines != ['']:
            job_param_list = lines[0].split()
            job_clusterid = int(job_param_list[1])
            job_procid = job_param_list[2]
            job_detail["jobId"] = str(job_clusterid) + '.' + str(job_procid)
            job_detail["jobType"] = job_param_list[8]

            q_date_time = datetime.fromtimestamp(int(job_param_list[4]), ZoneInfo("Asia/Shanghai"))
            job_detail["jobSubmitTime"] = q_date_time.strftime('%Y-%m-%d %H:%M:%S')
            
            r_date_time = datetime.fromtimestamp(int(job_param_list[6]), ZoneInfo("Asia/Shanghai"))
            job_detail["jobStartTime"] = r_date_time.strftime('%Y-%m-%d %H:%M:%S')
            
            if job_param_list[5] == '1':
                job_detail["jobStatus"] = "QUEUEING"

            elif job_param_list[5] == '2':
                job_detail["jobStatus"] = "RUNNING"
                job_detail["JobNodeList"] = job_param_list[9]
            
            elif job_param_list[5] == '3':
                job_detail["jobStatus"] = "CANCELED"
                job_detail["JobNodeList"] = job_param_list[9]

            elif job_param_list[5] == '4':
                job_detail["jobStatus"] = "COMPLETED"
                job_detail["JobNodeList"] = job_param_list[9]

            elif job_param_list[5] == '5':
                job_detail["jobStatus"] = "HOLDING"
                
            else:
                job_detail["jobStatus"] = "OTHER" 

            ret, msg, _, db_job_status, _, _ = get_job_info(uid, job_clusterid, clusterid)
            if ret < 0:
                raise HTTPException(status_code=500, detail=f"Get job info from db error: {msg}")
                                
            if db_job_status != job_detail["jobStatus"]: 
                ret, msg = update_job_status(uid, job_clusterid, job_detail["jobStatus"], clusterid)
                if ret < 0:
                    raise HTTPException(status_code=500, detail=f"Update job info to db error: {msg}")
                
        return job_detail

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
        


