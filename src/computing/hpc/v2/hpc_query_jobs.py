# routers/jobs.py
import re
from fastapi import HTTPException
from src.computing.gateway_tools import *
from datetime import datetime, timedelta
from sqlalchemy.exc import NoResultFound
from src.computing.tools.resources_utils import *
from src.computing.hpc.hpc_check_job import get_job_output
from src.computing.common import get_job_info, insert_job_info

async def get_user_jobs(uid, job_type, cluster_id):
    try:
        starttime = (datetime.now() - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%S")
        username = change_uid_to_username(uid)
        
        command = (f"sacct -u {username} --format=JobID,Partition,JobName,User,State,Elapsed,NNodes,NodeList,AdminComment,Start,submit,WorkDir -P")
        stdout = await sub_command(command, 5, "Command sacct execution failed", "Command sacct execution timeout.")
        lines = stdout.decode().strip().split('\n')
        headers = lines[0].split('|')
        job_list = []
        
        for line in lines[1:]:
            fields = line.split('|')
            job_data = dict(zip(headers, fields))
            
            if not job_data["JobID"].isdigit():
                continue
            
            admin_comment = job_data["AdminComment"].strip().lower()
            if admin_comment not in ["common", "jupyter", "enode", "rootbrowse", "vscode", "vnc"]:
                admin_comment = "other"
                continue

            jobId = job_data["JobID"]
            job_status = job_data["State"]
            if job_status == "PENDING":
                job_status = "QUEUEING"

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

            if job_type and job_type != "all" and admin_comment != job_type.lower():
                continue
            if job_type == "other" and admin_comment != "other":
                continue
            if job_type == "all" and admin_comment == "other":
                continue

            if admin_comment  == "enode":
                if job_status == "RUNNING":
                    create_iptables(uid, jobId, job_iptables_status, job_iptables_clean, cluster_id)
                elif job_status != "QUEUEING" and job_status != "RUNNING" and job_status != "CONFIGURING":
                    gateway_port, = get_job_iptables_status(uid, jobId, cluster_id)
                    if gateway_port != 0 and job_iptables_clean != 1:
                        delete_iptables(uid, jobId, gateway_port, cluster_id)
                        
            if job_status != "QUEUEING" and job_status != "RUNNING":
                continue
            
            if job_data["Start"] == "Unknown":
                job_data["Start"] = " "
            
            if job_data["NodeList"] == "None assigned":
                job_data["NodeList"] = " "
            else:
                job_data["NodeList"] = f"{job_data['NodeList']}.ihep.ac.cn"

            job_list.append({
                "clusterId": "slurm",
                "jobId": job_data["JobID"],
                "jobStartTime": job_data["Start"].replace("T", " "),
                "jobStatus": job_data["State"],
                "jobNodeList": job_data["NodeList"],
                "jobSubmitTime":job_data["Submit"].replace("T", " "),
                "jobType": admin_comment,
                "connect_sign": connect_sign
            })

        return job_list

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))