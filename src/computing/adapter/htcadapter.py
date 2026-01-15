import json
from shlex import quote
from typing import Optional
from datetime import datetime
from src.common.logger import logger
from src.common.config import get_config
from sqlalchemy.exc import NoResultFound
from src.computing.tools.db.db_tools import *
from src.computing.cluster.cluster import HTC_JOB
from src.computing.adapter.strategy import scheduler
from src.computing.adapter.baseadapter import SchedulerBase
from src.inkdb.inkredis import redis_connect
from src.computing.tools.common.utils import sub_command, get_job_output, create_iptables, delete_iptables

@scheduler("htcondor")
class HTC_Scheduler(SchedulerBase):
    def __init__(self, uid: int):
        super().__init__(uid)
        self.SCHEDD_HOST = get_config("computing", "schedd_host")
        self.CM_HOST = get_config("computing", "cm_host")
        self.CLUSTER_TYPE = "htcondor"


    def _generate_condor_query_command(self, job_type: str) -> str:
        
        BASE_CMD = f"condor_q {quote(self.USERNAME)} -name {quote(self.SCHEDD_HOST)} "
        BASE_ATTRS = [
            "Owner", "ClusterId", "ProcId", "HepJob_RealGroup", "Qdate",
            "JobStatus", "JobStartDate", "RemoteHost", "HepJob_JobType", "HepJob_RequestOS"
        ]
        EXTRA_ATTRS = ["Iwd", "Out", "Err", "holdreason"]
        ALL_JOB_TYPES = ["enode", "ink_special", "jupyter", "vscode", "rootbrowse", "vnc", "npu", "compile"]

        if job_type == "all":
            constraint = " || ".join([f'HepJob_JobType == "{jt}"' for jt in ALL_JOB_TYPES])
            attrs = BASE_ATTRS + EXTRA_ATTRS
        else:
            constraint = f'HepJob_JobType == "{job_type}"'
            attrs = BASE_ATTRS

        command = (
            f"{BASE_CMD} "
            f"-const '{constraint}' "
            f"-af {' '.join(attrs)}"
        )
        
        logger.debug(f"Generate {self.USERNAME} query command: {command}")

        return command
    

    def _change_completed_jobs_status(self, user_completed_jobs) -> None:
        iptables_jobtype = get_config("computing", "iptables_jobtype")
        if user_completed_jobs:
            for key in user_completed_jobs:
                complete_job_type = user_completed_jobs[key][0]
                if complete_job_type in iptables_jobtype:
                    gateway_port = user_completed_jobs[key][1]
                    sshd_job_iptables_clean = user_completed_jobs[key][2]
                    if gateway_port != 0 and sshd_job_iptables_clean == 0:
                        _ = delete_iptables(self.UID, key, gateway_port, self.CLUSTER_TYPE)
                update_job_status(self.UID, key, 'COMPLETED', self.CLUSTER_TYPE)
                logger.info(f"Update job {key} status to COMPLETED.")
    

    async def submit_job(self, htc_job_params: HTC_JOB):
        try:
            r = redis_connect()
            cluster_jobs = await r.get("cluster_jobs")
            
            if not cluster_jobs:
                cluster_jobs = {}
            else:
                if isinstance(cluster_jobs, (bytes, bytearray)):
                    cluster_jobs = cluster_jobs.decode("utf-8")
                cluster_jobs = json.loads(cluster_jobs)
            user_job_list = cluster_jobs.get(self.USERNAME, [])
            logger.debug(f"HTC-LOG: Get {self.USERNAME} cluster_jobs: {user_job_list}")

            for job in user_job_list:
                if job.get("jobType") == htc_job_params.job_type:
                    logger.debug(f"HTC-LOG: Submit job {htc_job_params.job_type} exsit in cluster_jobs: {job}")
                    return
            
            raw = await r.lrange(f"{self.USERNAME}_submitting_jobs", 0, -1)
            logger.debug(f"HTC-LOG: Get {self.USERNAME}_submitting_jobs keys value: {raw}")

            for job in raw:
                if isinstance(job, (bytes, bytearray)):
                    job = job.decode("utf-8")
                    job_param = json.loads(job)
                if job_param.get("jobType") == htc_job_params.job_type:
                    logger.debug(f"HTC-LOG: Submit job {htc_job_params.job_type} exsit in {self.USERNAME}_submitting_jobs: {job}")
                    return  
            
            submit_param = {
                "username": self.USERNAME,
                "jobType": htc_job_params.job_type,
                "jobReqCPU": htc_job_params.cpu,
                "jobReqMEM": htc_job_params.mem,
                "jobReqOS": htc_job_params.os,
                "jobReqWN": htc_job_params.wn,
                "jobReqARCH": htc_job_params.arch,
                "jobReqParam": htc_job_params.job_parameters,
                "clusterId": htc_job_params.cluster_id
            }
            
            await r.lpush(f"submitting_jobs", json.dumps(submit_param, ensure_ascii=False))
            items = await r.lrange("submitting_jobs", 0, -1)
            items = [x.decode("utf-8") if isinstance(x, (bytes, bytearray)) else x for x in items]
            logger.debug(f"HTC-LOG: PUSH successfully, and the submitting_jobs: {items}")

            await r.lpush(f"{self.USERNAME}_submitting_jobs", json.dumps(submit_param, ensure_ascii=False))
            items = await r.lrange(f"{self.USERNAME}_submitting_jobs", 0, -1)
            items = [x.decode("utf-8") if isinstance(x, (bytes, bytearray)) else x for x in items]
            logger.debug(f"HTC-LOG: PUSH successfully, and the {self.USERNAME}_submitting_jobs: {items}")
            logger.debug(f"HTC-LOG: {self.USERNAME} job {htc_job_params.job_type} add to redis queue.")

        except Exception as e:
            logger.error(f"HTC-LOG: Some Wrong in Submit job, the details: {e}")
            raise e
    

        
    async def query_job(self, req_job_type: Optional[str] = None):
        
        r = redis_connect()

        raw_cluster_jobs = await r.get("cluster_jobs")
        if not raw_cluster_jobs:
            cluster_jobs = {}
        else:
            if isinstance(raw_cluster_jobs, (bytes, bytearray)):
                raw_cluster_jobs = raw_cluster_jobs("utf-8")
            cluster_jobs = json.loads(raw_cluster_jobs)
        job_list = cluster_jobs.get(self.USERNAME, [])

        iptables_jobtype = get_config("computing", "iptables_jobtype")
        start_keywords = get_config("computing", "start_keywords")

        logger.info(f"Get {self.USERNAME} jobs: {job_list}")

        return_list = [] 

        for job in job_list:
            job_id = job.get("jobId")
            job_condor_type = job.get("jobType")
            job_status = job.get("jobStatus")
            job_output_path = job.get("joboutpath")
            job_err_path = job.get("joberrpath")
            job_iwd = job.get("jobiwd")
            job_submit_time = job.get("jobSubmitTime")
            job_start_time = job.get("jobStartTime")
            job_node_list = job.get("jobNodeList")
            job_runos = job.get("jobrunos")
            job_hold_reason = job.get("hold_reason")

            if req_job_type:
                req_job_types = req_job_type.split(',')
                if job_condor_type not in req_job_types:
                    continue

            try:
                job_type, db_job_status, job_iptables_status, job_iptables_clean = get_job_info(self.UID, job_id, self.CLUSTER_TYPE)
                logger.info(f"Find the job {job_id} in the DB, and the details are: {job_type}, {db_job_status}, {job_iptables_status}, {job_iptables_clean}")       
            except NoResultFound:
                job_path = job_iwd
                insert_job_info(self.UID, job_id, job_output_path, job_err_path, job_condor_type, job_path, self.CLUSTER_TYPE)
                job_type, db_job_status, job_iptables_status, job_iptables_clean = get_job_info(self.UID, job_id, self.CLUSTER_TYPE)
            connect_sign, = get_job_connect_info(self.UID, job_id, self.CLUSTER_TYPE)

            if job_status == '1':    
                job_status = "QUEUEING"

            elif job_status == '2':
                job_status = "RUNNING"
                if connect_sign == "False":
                    output_content, _ = await get_job_output(uid=self.UID, job_id=job_id, clusterid="htcondor")
                    if any(kw in output_content for kw in start_keywords):
                        connect_sign = "True"
                        if job_type in iptables_jobtype:
                            try:
                                await create_iptables(self.UID, job_id, job_iptables_status, job_iptables_clean, self.CLUSTER_TYPE)
                            except Exception as e:
                                connect_sign = "False"
                                logger.error(f"{job_id} iptables set failed, the details: {e}")
                        update_connect_status(self.UID, job_id, connect_sign, self.CLUSTER_TYPE)
                        update_start_time(self.UID, job_id, job_start_time, self.CLUSTER_TYPE)
                        
            elif job_status == '4':
                job_status = "COMPLETED"

            elif job_status == '5':
                job_status = "HOLDING"

            else:
                job_status = "OTHER" 
            
            if db_job_status != job_status: 
                update_job_status(self.UID, job_id, job_status, self.CLUSTER_TYPE)
            
            return_list.append({
                "clusterId": self.CLUSTER_TYPE,
                "jobId": str(job_id),
                "jobType": job_type,
                "jobSubmitTime": job_submit_time,
                "jobStatus": job_status,
                "jobStartTime": job_start_time,
                "JobNodeList": job_node_list,
                "jobrunos": job_runos,
                "connect_sign": connect_sign,
                "hold_reason": job_hold_reason
            })

        raw_jobs = await r.lrange(f"{self.USERNAME}_submitting_jobs", 0, -1)
        for raw_job in raw_jobs:
            job = json.loads(raw_job)

            job_redis_type = job.get("jobType")
            job_os = job.get("jobReqOS")
            job_status = "SUBMITTING"
            connect_sign = "False"

            if req_job_type:
                req_job_types = req_job_type.split(',')
                if job_redis_type not in req_job_types:
                    continue

            return_list.append({
                "clusterId": self.CLUSTER_TYPE,
                "jobId": "",
                "jobType": job_redis_type,
                "jobSubmitTime": "",
                "jobStatus": job_status,
                "jobStartTime": "",
                "JobNodeList": "",
                "jobrunos": job_os,
                "connect_sign": connect_sign,
                "hold_reason": ""
            })

        return return_list

    
    async def cancel_job(self, job_id):
        
        cancel_command = f"sudo -u {self.USERNAME} condor_rm -name {self.SCHEDD_HOST} -pool {self.CM_HOST} {job_id}"

        _ = await sub_command(cancel_command, timeoutsec=10, errinfo="condor_rm job failed", tminfo="condor_rm job timeout")
        
        job_type, _, job_iptables_status, job_iptables_clean = get_job_info(self.UID, job_id, self.CLUSTER_TYPE)
        
        iptables_jobtype = get_config("computing", "iptables_jobtype")

        if job_type in iptables_jobtype:
            if job_iptables_status != 0 and job_iptables_clean == 0:
                delete_iptables(self.UID, job_id, job_iptables_status, self.CLUSTER_TYPE)
        update_job_status(self.UID, job_id, 'COMPLETED', self.CLUSTER_TYPE)

        job_end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        update_end_time(self.UID, job_id, job_end_time, "htcondor")
        
        
