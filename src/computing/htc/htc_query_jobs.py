import re
from zoneinfo import ZoneInfo
from src.common.logger import logger
from sqlalchemy.exc import NoResultFound
from src.computing.tools.resources_utils import *

'''
Author:         guocq@ihep.ac.cn
Created:        2024-12-18
Last Modified:  2024-12-18
'''

async def get_user_jobs(uid, job_type, clusterid):
    try:
        username = change_uid_to_username(uid)
        job_list = []

        user_completed_jobs = find_completed_jobs(uid, job_type)
        logger.info(f"Get user completed jobs: {user_completed_jobs}")
        
        SCHEDD_HOST = get_config("computing", "schedd_host")
        command = (
            f"condor_q {quote(username)} "
            f"-name {quote(SCHEDD_HOST)} "
            f"-const 'HepJob_JobType == \"{job_type}\"' "
            "-af Owner ClusterId ProcId HepJob_RealGroup Qdate JobStatus JobStartDate RemoteHost HepJob_JobType HepJob_RequestOS"
        )
        
        if job_type == "all":    
            command = (
                f"condor_q {quote(username)} "
                f"-name {quote(SCHEDD_HOST)} "
                f"-const 'HepJob_JobType == \"enode\" || HepJob_JobType == \"ink_special\" || HepJob_JobType == \"npu\" || HepJob_JobType == \"jupyter\" || HepJob_JobType == \"vscode\" || HepJob_JobType == \"rootbrowse\" || HepJob_JobType == \"vnc\"' "
                "-af Owner ClusterId ProcId HepJob_RealGroup Qdate JobStatus JobStartDate RemoteHost HepJob_JobType HepJob_RequestOS Iwd Out Err holdreason"
            )
            
        stdout = await sub_command(command, 10, "Query user jobs failed.", "Query user jobs timeout.")
        lines = stdout.decode().strip().split('\n')
        logger.info(f"Get user {username} queue's jobs: {lines}")
        
        if lines != ['']:
            for line in lines:
                job_param_list = line.split()
                job_clusterid = int(job_param_list[1])
                job_procid = job_param_list[2]
                job_id = str(job_clusterid) + '.' + str(job_procid)
                HepJob_JobType = job_param_list[8]
                job_start_time = " "
                job_remote_host = " "
                hold_reason = ""
                job_requestos = job_param_list[9]
            
                if job_clusterid in user_completed_jobs.keys():
                    del user_completed_jobs[job_clusterid]
                try:
                    job_type, db_job_status, job_iptables_status, job_iptables_clean = get_job_info(uid, job_clusterid, clusterid)
                    logger.info(f"Find the job {job_id} in the DB, and the details are: {job_type}, {db_job_status}, {job_iptables_status}, {job_iptables_clean}") 
                            
                except NoResultFound:
                    job_path = job_param_list[10]
                    job_output_path = f"{job_path}/{job_param_list[11]}"
                    job_errput_path = f"{job_path}/{job_param_list[12]}"
                    insert_job_info(uid, job_id, job_output_path, job_errput_path, HepJob_JobType, job_path, clusterid)
                    job_type, db_job_status, job_iptables_status, job_iptables_clean = get_job_info(uid, job_clusterid, clusterid)
                connect_sign, = get_job_connect_info(uid, job_id, clusterid)

                date_time =  datetime.fromtimestamp(int(job_param_list[4]), ZoneInfo("Asia/Shanghai"))
                job_queue_time = date_time.strftime('%Y-%m-%d %H:%M:%S')
                if job_param_list[5] == '1':
                    job_status = "QUEUEING"

                elif job_param_list[5] == '2':
                    logger.debug(f"{job_id} is running")
                    job_status = "RUNNING"
                    date_time = datetime.fromtimestamp(int(job_param_list[6]), ZoneInfo("Asia/Shanghai"))
                    job_start_time = date_time.strftime('%Y-%m-%d %H:%M:%S')
                    job_remote_host = job_param_list[7]
                    if connect_sign == "False":
                        output_content, _ = await get_job_output(uid=uid, job_id=job_id, clusterid="htcondor")
                        if (
                            re.search(r"Jupyter Server [\w.+-]+ is running at", output_content) or
                            "HTTP server listening on" in output_content or
                            "Generating public/private rsa key pair" in output_content or
                            "Navigate to this URL" in output_content or
                            "Elapsed time" in output_content
                        ):
                            connect_sign = "True"
                            if HepJob_JobType == "enode":
                                create_iptables(uid, job_clusterid, job_iptables_status, job_iptables_clean, clusterid)
                            update_connect_status(uid, job_id, connect_sign, clusterid)

                elif job_param_list[5] == '4':
                    job_status = "COMPLETED"
                    date_time =  datetime.fromtimestamp(int(job_param_list[6]), ZoneInfo("Asia/Shanghai"))
                    job_start_time = date_time.strftime('%Y-%m-%d %H:%M:%S')
                    job_remote_host = job_param_list[7]

                elif job_param_list[5] == '5':
                    job_status = "HOLDING"
                    date_time =  datetime.fromtimestamp(int(job_param_list[6]), ZoneInfo("Asia/Shanghai"))
                    job_start_time = date_time.strftime('%Y-%m-%d %H:%M:%S')
                    hold_reason = ' '.join(map(str, job_param_list[13:]))
                else:
                    job_status = "OTHER" 
                
                if db_job_status != job_status: 
                    update_job_status(uid, job_clusterid, job_status, clusterid)
                    
                job_list.append({
                    "clusterId": clusterid,
                    "jobId": job_id,
                    "jobType": HepJob_JobType,
                    "jobSubmitTime": job_queue_time,
                    "jobStatus": job_status,
                    "jobStartTime": job_start_time,
                    "JobNodeList": job_remote_host,
                    "jobrunos": job_requestos,
                    "jobtimelimit": "24:00:00",
                    "connect_sign": connect_sign,
                    "hold_reason": hold_reason
                })

        if user_completed_jobs:
            for key in user_completed_jobs:
                complete_job_type = user_completed_jobs[key][0]
                if complete_job_type == "enode":
                    jobId = key
                    gateway_port = user_completed_jobs[key][1]
                    sshd_job_iptables_clean = user_completed_jobs[key][2]
                    if gateway_port != 0 and sshd_job_iptables_clean == 0:
                        delete_iptables(uid, jobId, gateway_port, clusterid)
                update_job_status(uid, key, 'COMPLETED', clusterid)
        logger.info(f"Query finished, User: {username} job_list is: {job_list}")
        return job_list

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
        


