# routers/jobs.py
from fastapi import HTTPException
import subprocess
import re
from src.computing.tools.resources_utils import change_uid_to_username, sub_command
from src.computing.common import get_job_info, insert_job_info
from src.computing.gateway_tools import *
from src.computing.tools.resources_utils import *
from src.computing.hpc.hpc_check_job import get_job_output

async def get_user_jobs(uid, jobId, cluster_id):
    try:
        user_name = change_uid_to_username(uid)
        
        # command = f"sacct -j {jobId} --format=JobID,Partition,JobName,User,State,Elapsed,NNodes,NodeList,AdminComment,Start,Submit -P"
        command = f"su {user_name} -c 'sacct -j {jobId} --format=JobID,Partition,JobName,User,State,Elapsed,NNodes,NodeList,AdminComment,Start,Submit -P'"
        result = await sub_command(command, timeoutsec=5, errinfo="sacct err", tminfo="sacct timeout")
        lines = result.decode().strip().split('\n')
        headers = lines[0].split('|')
        
        job_data = {}
        connect_sign = ""
        for line in lines[1:2]:
            fields = line.split('|')
            job_data = dict(zip(headers, fields))
            job_id = job_data["JobID"]
            
            if not job_id.isdigit():  
                continue
            
            jobId = job_data["JobID"]
            job_status = job_data["State"]
            if job_status == "PENDING":
                job_status = "QUEUEING"
                
            admin_comment = job_data["AdminComment"]
            db_job_type, db_job_status, job_iptables_status, job_iptables_clean = get_job_info(uid, jobId, cluster_id)

            try:
                db_job_type, db_job_status, job_iptables_status, job_iptables_clean = get_job_info(uid, jobId, cluster_id)
            except NoResultFound:                
                logger.info(f"create a missing job record in database! jobId: {jobId}")
                job_path = job_data["WorkDir"]
                job_type_insert = admin_comment
                if job_status in ("QUEUEING", "RUNNING", "CONFIGURING"):
                    sc_command = (f"scontrol show job {jobId}")
                    stdout = await sub_command(sc_command, 5, "Command scontrol execution failed", "Command scontrol execution timeout.")
                    sc_result = stdout.decode()
                    output_match = re.search(r"StdOut=(\S+)", sc_result)
                    error_match  = re.search(r"StdErr=(\S+)", sc_result)
                    
                    if output_match:
                        output_path = output_match.group(1)
                    else:
                        output_path = ""
                        
                    if error_match:
                        error_path = error_match.group(1)
                    else:
                        error_path = ""
                    insert_job_info(uid, jobId, f"{output_path}", f"{error_path}", job_type_insert, job_path, cluster_id)
                    db_job_type, db_job_status, job_iptables_status, job_iptables_clean = get_job_info(uid, jobId, cluster_id)
                else:
                    continue
            
            if job_status != db_job_status:
                update_job_status(uid, jobId, job_status, cluster_id)
            connect_sign, = get_job_connect_info(uid, jobId, cluster_id)

            if job_status == "RUNNING":
                if connect_sign == "False":
                    output_content, _ = await get_job_output(uid=uid, job_id=jobId, cluster_id=cluster_id)
                    if (
                        re.search(r"Jupyter Server [\w.+-]+ is running at", output_content) or 
                        "HTTP server listening on" in output_content or
                        "Generating public/private rsa key pair" in output_content or
                        "Navigate to this URL" in output_content or
                        "Elapsed time" in output_content
                        ):
                        connect_sign = "True"
                        update_connect_status(uid, jobId, connect_sign, cluster_id)                          

            if admin_comment  == "enode":
                if job_status == "RUNNING":
                    create_iptables(uid, jobId, job_iptables_status, job_iptables_clean, cluster_id)
                    
                elif job_status != "QUEUEING" and job_status != "RUNNING" and job_status != "CONFIGURING":
                    gateway_port, = get_job_iptables_status(uid, jobId, cluster_id)
                    if gateway_port != 0 and job_iptables_clean != 1:
                        delete_iptables(uid, jobId, gateway_port, cluster_id)
                        
        return job_data.get("Start"), job_data.get("State"), job_data.get("NodeList"), job_data.get("Submit"), job_data.get("AdminComment"), connect_sign

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

