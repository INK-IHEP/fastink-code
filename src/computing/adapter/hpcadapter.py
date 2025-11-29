import os, re, shlex
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from src.storage import common
from typing import List, Optional
from src.common.logger import logger
from src.common.config import get_config
from sqlalchemy.exc import NoResultFound
from src.computing.tools.db.db_tools import *
from src.computing.cluster.cluster import SLURM_JOB
from src.computing.adapter.strategy import scheduler
from src.computing.adapter.baseadapter import SchedulerBase
from src.computing.tools.common.utils import sub_command, get_job_output, create_iptables, delete_iptables, get_endtime_info


@scheduler("slurm")
class HPC_Scheduler(SchedulerBase):
    def __init__(self, uid: int):
        super().__init__(uid)
        self.CLUSTER_TYPE = "slurm"

    async def _generate_slurm_submit(
        self, 
        cpu: int, 
        mem: int, 
        jobtype: str, 
        jobdir: str, 
        partition: Optional[str],
        account: Optional[str],
        qos: Optional[str],
        ntasks: int = 1,
        nodes: int = 1,
        gpu_num: int = 0,
        gpu_name: Optional[str] = None,
        gpu_type: Optional[str] = None,
    ):
        if cpu < 1:
            raise ValueError("CPU must be >=1 .")
        
        if mem < 1:
            raise ValueError("MEM must be >= 1 (MB).")
        
        
        out_path = str(Path(jobdir) / f"%j.out")
        err_path = str(Path(jobdir) / f"%j.err")

        args: List[str] = [
            "sbatch",
            "--parsable",
            f"--output={out_path}",
            f"--error={err_path}",
            f"--nodes={nodes}",
            f"--ntasks={ntasks}",
            f"--cpus-per-task={cpu}",
            f"--mem={mem}M",
            f"--job-name={jobtype}",
            f"--wckey={jobtype}",
            f"--chdir={str(jobdir)}",
        ]

        if partition:
            args.append(f"--partition={partition}")
        if account:
            args.append(f"--account={account}")
        if qos:
            args.append(f"--qos={qos}")

        if gpu_num and gpu_num > 0 and gpu_name:
            if gpu_type:
                gres = f"{gpu_name}:{gpu_type}:{gpu_num}"
            else:
                gres = f"{gpu_name}:{gpu_num}"
            args.append(f"--gres={gres}")

        executable_dir = get_config("computing", "cluster_scripts")
        executable = f"{executable_dir}/{jobtype}/shell.sh"

        with open(executable, "rb") as file:
            submitfile_content = file.read()
        await common.upload_file(src_data=submitfile_content, dst=f"{jobdir}/shell.sh", username=self.USERNAME, mgm=self.XROOTD_PATH, mode="700")

        job_script = f"{executable_dir}/{jobtype}/run.sh"
        with open(job_script, "rb") as file:
            job_script_content = file.read()
        await common.upload_file(src_data=job_script_content, dst=f"{jobdir}/run.sh", username=self.USERNAME, mgm=self.XROOTD_PATH, mode="700")

        args.append(f"{jobdir}/shell.sh")
        submit_command = shlex.join(args)

        _inner = submit_command.replace("'", "'\"'\"'")
        submit_command = f"sudo -iu {shlex.quote(self.USERNAME)} bash -lc '{_inner}'"

        return submit_command

    async def submit_job(self, hpc_job_params: SLURM_JOB) -> str:
        try:
            time_stamp = datetime.now().strftime('%Y%m%d-%H%M%S')
            token_filename, krb5_decoded_bytes = self._generate_token_file()
            logger.info(f"Generate User {self.USERNAME} the token file finished.")
            job_dir = await self._init_job_dir(hpc_job_params.job_type, time_stamp, krb5_decoded_bytes)
            logger.info(f"Init User {self.USERNAME} jobdir {job_dir} finished.")

            submit_cmd = await self._generate_slurm_submit(cpu=hpc_job_params.cpu, 
                                                           mem=hpc_job_params.mem, 
                                                           jobtype=hpc_job_params.job_type,
                                                           jobdir=job_dir,
                                                           partition=hpc_job_params.partition,
                                                           account=hpc_job_params.account,
                                                           qos=hpc_job_params.qos,
                                                           ntasks=hpc_job_params.ntasks,
                                                           nodes=hpc_job_params.nodes,
                                                           gpu_num=hpc_job_params.gpu_num,
                                                           gpu_name=hpc_job_params.gpu_name,
                                                           gpu_type=hpc_job_params.gpu_type)
            logger.info(f"Generate User {self.USERNAME} the slurm submit command finished, cmd: {submit_cmd}")

            # Submit the slurm job.
            stdout = await sub_command(submit_cmd, 10, "submit job failed.", "submit job timeout.")
            logger.info(f"The slurm submit info: {stdout}")

            job_id_line = stdout.decode().strip()
            job_id = job_id_line.split(";", 1)[0]
            output = f"{job_dir}/{job_id}.out"
            errpath = f"{job_dir}/{job_id}.err"

            logger.info(f"Submit User {self.USERNAME} job {job_id} to cluster.")
            insert_job_info(self.UID, job_id, output, errpath, hpc_job_params.job_type, job_dir, self.CLUSTER_TYPE)
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
        
        command = (f"sacct -u {self.USERNAME} --format=JobID,Partition,State,Elapsed,NNodes,NodeList,WCkey,Submit,Start,End,WorkDir,Time -P -X -n")
        stdout = await sub_command(command, 10, "Query user jobs failed.", "Query user jobs timeout.")
        logger.info(f"Get user({self.USERNAME}) slurm cluster jobs: {stdout}")
        lines = stdout.decode().strip().split('\n')
        logger.info(f"{self.USERNAME} slurm jobs return: {lines}")

        if lines != ['']:
            for line in lines:
                job_param_list = line.split("|")
                job_clusterid = int(job_param_list[0])
                job_partition = job_param_list[1]
                job_status = job_param_list[2]
                job_nodelist = job_param_list[5]
                job_slurm_type = job_param_list[6]
                job_submit_time = job_param_list[7].replace("T", " "),
                job_start_time = job_param_list[8].replace("T", " "),
                job_end_time = job_param_list[9].replace("T", " "),
                job_time_limit = job_param_list[10]

                try:
                    job_type, db_job_status, job_iptables_status, job_iptables_clean = get_job_info(self.UID, job_clusterid, self.CLUSTER_TYPE)
                    logger.info(f"Find the job {job_clusterid} in the DB, and the details are: {job_type}, {db_job_status}, {job_iptables_status}, {job_iptables_clean}")       
                except NoResultFound:
                    job_path = job_param_list[10]
                    job_output_path = f"{job_path}/{job_clusterid}.out"
                    job_errput_path = f"{job_path}/{job_clusterid}.err"
                    insert_job_info(self.UID, job_clusterid, job_output_path, job_errput_path, job_slurm_type, job_path, self.CLUSTER_TYPE)
                    job_type, db_job_status, job_iptables_status, job_iptables_clean = get_job_info(self.UID, job_clusterid, self.CLUSTER_TYPE)
                connect_sign, = get_job_connect_info(self.UID, job_clusterid, self.CLUSTER_TYPE)

                if job_status == "PENDING":
                    job_status = "QUEUEING"

                elif job_status == "RUNNING":
                    if connect_sign == "False":
                        output_content, _ = await get_job_output(uid=self.UID, job_id=job_clusterid, clusterid=self.CLUSTER_TYPE)
                        if (
                            re.search(r"Jupyter Server [\w.+-]+ is running at", output_content) or
                            "HTTP server listening on" in output_content or
                            "SSH server starting" in output_content or
                            "Navigate to this URL" in output_content or
                            "Elapsed time" in output_content
                        ):
                            connect_sign = "True"
                            if job_type in iptables_jobtype:
                                try:
                                    await create_iptables(self.UID, job_clusterid, job_iptables_status, job_iptables_clean, self.CLUSTER_TYPE)
                                except Exception as e:
                                    connect_sign = "False"
                                    logger.error(f"{job_clusterid} iptables set failed, the details: {e}")
                            update_connect_status(self.UID, job_clusterid, connect_sign, self.CLUSTER_TYPE)
                            update_start_time(self.UID, job_clusterid, job_start_time, self.CLUSTER_TYPE)
                            
                elif job_status == "COMPLETED" or job_status.startswith("CANCELLED"):
                    job_status = "COMPLETED"
                    db_end_time = get_endtime_info(self.UID, job_clusterid, self.CLUSTER_TYPE)
                    if not db_end_time:
                        update_end_time(self.UID, job_clusterid, job_end_time, self.CLUSTER_TYPE)
                    continue

                else:
                    continue
                
                if db_job_status != job_status: 
                    update_job_status(self.UID, job_clusterid, job_status, self.CLUSTER_TYPE)
                
                job_list.append({
                    "clusterId": self.CLUSTER_TYPE,
                    "jobId": job_clusterid,
                    "jobPartition": job_partition,
                    "jobType": job_type,
                    "jobSubmitTime": job_submit_time,
                    "jobStatus": job_status,
                    "jobStartTime": job_start_time,
                    "JobNodeList": job_nodelist,
                    "jobtimelimit": job_time_limit,
                    "connect_sign": connect_sign,
                })

        return job_list

    
    async def cancel_job(self, job_id):

        cancel_command = f"sudo -u {self.USERNAME} scancel {job_id}"
        _ = await sub_command(cancel_command, timeoutsec=10, errinfo="scancel err",tminfo="scancel timeout")
        job_type, _, job_iptables_status, job_iptables_clean = get_job_info(self.UID, job_id, self.CLUSTER_TYPE)

        iptables_jobtype = get_config("computing", "iptables_jobtype")
        if job_type in iptables_jobtype:
            if job_iptables_status != 0 and job_iptables_clean == 0:
                delete_iptables(self.UID, job_id, job_iptables_status, self.CLUSTER_TYPE)
        update_job_status(self.UID, job_id, 'COMPLETED', self.CLUSTER_TYPE)

        job_end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        update_end_time(self.UID, job_id, job_end_time, self.CLUSTER_TYPE)





