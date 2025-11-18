import os, re, importlib, pwd
from shlex import quote
from typing import Optional
from datetime import datetime
from zoneinfo import ZoneInfo
from src.storage import common
from src.common.logger import logger
from src.common.config import get_config
from sqlalchemy.exc import NoResultFound
from src.computing.tools.db.db_tools import *
from src.computing.cluster.cluster import HTC_JOB
from src.computing.adapter.strategy import scheduler
from src.computing.adapter.baseadapter import SchedulerBase
from src.computing.tools.common.utils import sub_command, get_job_output, create_iptables, delete_iptables, build_requirements

@scheduler("htcondor")
class HTC_Scheduler(SchedulerBase):
    def __init__(self, uid: int):
        super().__init__(uid)
        self.SCHEDD_HOST = get_config("computing", "schedd_host")
        self.CM_HOST = get_config("computing", "cm_host")
        self.CLUSTER_TYPE = "htcondor"


    async def _generate_condor_submit(
        self, 
        cpu: int, 
        mem: int, 
        jobtype: str, 
        jobdir: str, 
        request_os: Optional[str] = None, 
        request_wn: Optional[str] = None, 
        request_arch: Optional[str] = None, 
        arguments: Optional[str] = None
    ):

        default_job_config = get_config("jobtype", jobtype).get("htc")
        extra_param = default_job_config.get("extra_param")
        job_cpus = default_job_config.get("RequestCpus", cpu)
        job_mem = default_job_config.get("RequestMemory", mem)
        job_schedd = default_job_config.get("schedd_host")
        job_cm = default_job_config.get("cm_host")
        
        if job_schedd:
            self.SCHEDD_HOST = job_schedd
        if job_cm:
            self.CM_HOST = job_cm
            
        workernode = default_job_config.get("workernode", request_wn)
        arch = default_job_config.get("arch", request_arch)

        executable_dir = get_config("computing", "cluster_scripts")
        executable = f"{executable_dir}/{jobtype}/shell.sh"

        with open(executable, "rb") as file:
            submitfile_content = file.read()
        await common.upload_file(src_data=submitfile_content, dst=f"{jobdir}/shell.sh", username=self.USERNAME, mgm=self.XROOTD_PATH, mode="700")

        job_script = f"{executable_dir}/{jobtype}/run.sh"
        with open(job_script, "rb") as file:
            job_script_content = file.read()
        await common.upload_file(src_data=job_script_content, dst=f"{jobdir}/run.sh", username=self.USERNAME, mgm=self.XROOTD_PATH, mode="700")

        if jobtype == "npu":
            arguments += jobdir
        
        config = {
            "universe": "vanilla",
            "executable": "shell.sh",
            "arguments": arguments,
            "output": f"{jobdir}/$(ClusterId).out",
            "error": f"{jobdir}/$(ClusterId).err",
            "request_cpus": job_cpus,
            "request_memory": job_mem,
            "getenv": "True",
        }    

        for key, value in default_job_config.items():
            if key in {"schedd_host", "cm_host", "RequestCpus", "RequestMemory", "walltime", "workernode", "extra_param"}:
                continue
            config[f"{key}"] = value

        if extra_param:
            job_plugin = importlib.import_module(f"src.computing.scripts.plugins.set_extra_config")
            extra_job_config = job_plugin.get_extra_job_config(self.USERNAME, self.GROUPNAME, jobtype, request_os)
            for key, value in extra_job_config.items():
                logger.info(f"key: {key}, value: {value}")
                config[f"{key}"] = value

        
        requirement_expr = build_requirements(workernode, arch)
        config[f"requirements"] = requirement_expr

        lines = [f"{k} = {v}" for k, v in config.items()]
        lines.append("queue")
        submitfile_bytes = ("\n".join(lines) + "\n").encode("utf-8")
        submitfile_name = f"{self.USERNAME}_{jobtype}.sub"

        await common.upload_file(src_data=submitfile_bytes, dst=f"{jobdir}/{submitfile_name}", username=self.USERNAME, mgm=self.XROOTD_PATH, mode="600")

        return submitfile_name


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

    
    def _generate_submit_command(self, job_dir: str, job_type: str, token_filename: str, submitfile: str) -> str:
        
        krb5_enabled = self.KRB5_ENABLED
        if isinstance(krb5_enabled, str):
            krb5_enabled = krb5_enabled.strip().lower() in {"1", "true", "yes", "on"}

        user_shell = pwd.getpwuid(self.UID).pw_shell
        bash_like = user_shell in {"/bin/bash", "/bin/sh", "/bin/zsh"}
        special_job = job_type in {"jupyter", "npu", "vnc"}

        if special_job:
            if bash_like:
                su_prefix = f"su -s /bin/bash {quote(self.USERNAME)} -c "
            else:
                su_prefix = f"su -s /bin/tcsh {quote(self.USERNAME)} -c "
        else:
            su_prefix = f"su - {quote(self.USERNAME)} -c "


        def env_kv(k: str, v: str) -> str:
            if bash_like:
                return f"export {k}={v}"
            else:
                return f"setenv {k} {v}"

        env_parts = [
            f"cd {quote(job_dir)}",
            env_kv("PATH", "/usr/bin:$PATH"),
            env_kv("LD_LIBRARY_PATH", "/lib64:$LD_LIBRARY_PATH"),
        ]

        if not special_job:
            env_parts.append(env_kv("INKPATH", "$PATH"))
            env_parts.append(env_kv("INKLDPATH", "$LD_LIBRARY_PATH"))

        if krb5_enabled:
            env_parts.insert(1, env_kv("KRB5CCNAME", quote(token_filename)))  # 放在 PATH 前后都可

        submit_part = (
            "condor_submit "
            f"-name {quote(self.SCHEDD_HOST)} "
            f"-pool {quote(self.CM_HOST)} "
            f"{quote(submitfile)}"
        )

        command = su_prefix + '"' + " && ".join(env_parts + [submit_part]) + '"'
        logger.info(f"User {self.USERNAME} submit command: {command}")

        return command
    

    async def submit_job(self, htc_job_params: HTC_JOB) -> str:
        try:
            time_stamp = datetime.now().strftime('%Y%m%d-%H%M%S')
            token_filename, krb5_decoded_bytes = self._generate_token_file()
            logger.info(f"Generate User {self.USERNAME} the token file finished.")
            job_dir = await self._init_job_dir(htc_job_params.job_type, time_stamp, krb5_decoded_bytes)
            logger.info(f"Init User {self.USERNAME} jobdir {job_dir} finished.")
            submit_file = await self._generate_condor_submit(htc_job_params.cpu, htc_job_params.mem, htc_job_params.job_type, job_dir, htc_job_params.os, htc_job_params.wn, htc_job_params.arch, htc_job_params.job_parameters)
            logger.info(f"Generate User {self.USERNAME} the condor submit file finished.")

            # Submit the condor job.
            submit_command = self._generate_submit_command(job_dir, htc_job_params.job_type, token_filename, submit_file)
            logger.info(f"Generate User {self.USERNAME} submit command {submit_command} finished.")
            stdout = await sub_command(submit_command, 10, "submit job failed.", "submit job timeout.")
            job_id_line = stdout.decode().strip()
            job_id = job_id_line.split()[-1].rstrip('.')
            output = f"{job_dir}/{job_id}.out"
            errpath = f"{job_dir}/{job_id}.err"

            logger.info(f"Submit User {self.USERNAME} job {job_id} to cluster.")
            insert_job_info(self.UID, job_id, output, errpath, htc_job_params.job_type, job_dir, htc_job_params.cluster_id)
            logger.info(f"Submit {self.USERNAME} job {job_id} to queue.")

            return int(job_id), job_dir
        
        except Exception as e:
            logger.error(f"Some Wrong in Submit job, the details: {e}")
            raise e
        
        finally:
            if token_filename and os.path.exists(token_filename):
                os.remove(token_filename)
                

        
    async def query_job(self, job_type):
        job_list = []
        iptables_jobtype = get_config("computing", "iptables_jobtype")
        
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
                        if (
                            re.search(r"Jupyter Server [\w.+-]+ is running at", output_content) or
                            "HTTP server listening on" in output_content or
                            "SSH server starting" in output_content or
                            "Navigate to this URL" in output_content or
                            "Elapsed time" in output_content
                        ):
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
        
        
