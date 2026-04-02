from shlex import quote
from datetime import datetime
from zoneinfo import ZoneInfo
from fastink.common.logger import logger
from fastink.common.config import get_config
from sqlalchemy.exc import NoResultFound
from fastink.computing.tools.db.db_tools import *
from fastink.computing.cluster.cluster import HTC_JOB
from fastink.computing.adapter.strategy import scheduler
from fastink.computing.adapter.baseadapter import SchedulerBase
from fastink.computing.tools.common.utils import sub_command, get_job_output, create_iptables, delete_iptables, generate_condor_submit, generate_submit_command, init_job_dir

class HTC_SYNC_Scheduler(SchedulerBase):
    def __init__(self, uid: int):
        super().__init__(uid)
        self.SCHEDD_HOST = get_config("computing", "schedd_host")
        self.CM_HOST = get_config("computing", "cm_host")
        self.CLUSTER_TYPE = "htcondor-sync"
    

    def _generate_condor_query_command(self, job_type: str) -> str:
        
        BASE_CMD = f"condor_q {quote(self.USERNAME)} -name {quote(self.SCHEDD_HOST)} "
        BASE_ATTRS = [
            "Owner", "ClusterId", "ProcId", "HepJob_RealGroup", "Qdate",
            "JobStatus", "JobStartDate", "RemoteHost", "HepJob_JobType", "HepJob_RequestOS"
        ]
        EXTRA_ATTRS = ["Iwd", "Out", "Err", "holdreason"]
        ALL_JOB_TYPES = ["enode", "ink_special", "jupyter", "vscode", "rootbrowse", "vnc", "npu", "compile", "openclaw"]

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


    async def submit_job(self, htc_job_params: HTC_JOB) -> str:
        try:
            job_dir = await init_job_dir(self.USERNAME, htc_job_params.job_type)
            logger.info(f"Init User {self.USERNAME} jobdir {job_dir} finished.")
            submit_file = await generate_condor_submit(self.USERNAME, htc_job_params.cpu, htc_job_params.mem, htc_job_params.job_type, job_dir, htc_job_params.os, htc_job_params.wn, htc_job_params.arch, htc_job_params.job_parameters)
            logger.info(f"Generate User {self.USERNAME} the condor submit file finished.")

            submit_command = generate_submit_command(self.USERNAME, job_dir, htc_job_params.job_type, submit_file)
            logger.info(f"Generate User {self.USERNAME} submit command {submit_command} finished.")
            stdout = await sub_command(submit_command, 10, "submit job failed.", "submit job timeout.")
            job_id_line = stdout.decode().strip()
            job_id = job_id_line.split()[-1].rstrip('.')
            output = f"{job_dir}/{job_id}.out"
            errpath = f"{job_dir}/{job_id}.err"

            logger.info(f"Submit User {self.USERNAME} job {job_id} to cluster.")
            insert_job_info(self.UID, job_id, output, errpath, htc_job_params.job_type, job_dir, htc_job_params.cluster_id)
            logger.info(f"Submit {self.USERNAME} job {job_id} to queue.")

            return int(job_id)
        
        except Exception as e:
            logger.error(f"Some Wrong in Submit job, the details: {e}")
            raise e
        
    
    async def query_job(self, job_type):
        job_list = []
        iptables_jobtype = get_config("computing", "iptables_jobtype")
        start_keywords = get_config("computing", "start_keywords")
        
        command = self._generate_condor_query_command(job_type)
        stdout = await sub_command(command, 10, "Query user jobs failed.", "Query user jobs timeout.")
        logger.info(f"Get user({self.USERNAME}) cluster jobs: {stdout}")
        lines = stdout.decode().strip().split('\n')

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

                try:
                    job_type, db_job_status, job_iptables_status, job_iptables_clean = get_job_info(self.UID, job_clusterid, self.CLUSTER_TYPE)
                    logger.info(f"Find the job {job_id} in the DB, and the details are: {job_type}, {db_job_status}, {job_iptables_status}, {job_iptables_clean}")       
                except NoResultFound:
                    job_path = job_param_list[10]
                    job_output_path = f"{job_param_list[11]}"
                    job_errput_path = f"{job_param_list[12]}"
                    insert_job_info(self.UID, job_id, job_output_path, job_errput_path, HepJob_JobType, job_path, self.CLUSTER_TYPE)
                    job_type, db_job_status, job_iptables_status, job_iptables_clean = get_job_info(self.UID, job_clusterid, self.CLUSTER_TYPE)
                connect_sign, = get_job_connect_info(self.UID, job_id, self.CLUSTER_TYPE)

                date_time =  datetime.fromtimestamp(int(job_param_list[4]), ZoneInfo("Asia/Shanghai"))
                job_queue_time = date_time.strftime('%Y-%m-%d %H:%M:%S')
                if job_param_list[5] == '1':    
                    job_status = "QUEUEING"

                elif job_param_list[5] == '2':
                    job_status = "RUNNING"
                    date_time = datetime.fromtimestamp(int(job_param_list[6]), ZoneInfo("Asia/Shanghai"))
                    job_start_time = date_time.strftime('%Y-%m-%d %H:%M:%S')
                    job_remote_host = job_param_list[7]
                    if connect_sign == "False":
                        output_content, _ = await get_job_output(uid=self.UID, job_id=job_id, clusterid="htcondor")
                        if any(kw in output_content for kw in start_keywords):
                            connect_sign = "True"
                            if HepJob_JobType in iptables_jobtype:
                                try:
                                    await create_iptables(self.UID, job_clusterid, job_iptables_status, job_iptables_clean, self.CLUSTER_TYPE)
                                except Exception as e:
                                    connect_sign = "False"
                                    logger.error(f"{job_clusterid} iptables set failed, the details: {e}")
                            update_connect_status(self.UID, job_id, connect_sign, self.CLUSTER_TYPE)
                            update_start_time(self.UID, job_id, job_start_time, self.CLUSTER_TYPE)
                            
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
                    update_job_status(self.UID, job_clusterid, job_status, self.CLUSTER_TYPE)
                
                job_list.append({
                    "clusterId": self.CLUSTER_TYPE,
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

        return job_list

    
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
