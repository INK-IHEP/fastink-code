import os, re, shlex
from pathlib import Path
from datetime import datetime
from fastink.storage import common
from typing import List, Optional, Tuple
#from fastink.common.logger import logger
from fastink.common.config import get_config
from sqlalchemy.exc import NoResultFound
from fastink.computing.tools.db.db_tools import *
from fastink.computing.cluster.cluster import SLURM_JOB, SubmitMode
from fastink.computing.adapter.strategy import scheduler
from fastink.computing.adapter.baseadapter import SchedulerBase
from fastink.computing.tools.common.utils import (
    sub_command, 
    get_job_output, 
    create_iptables, 
    delete_iptables,
    jobid_sort_key,
    init_job_dir,
    PathChecker,
    parse_sbatch_out_err
)
from fastink.computing.hpc.v2.hpc_check_job import get_job_output
from fastink.computing.tools.db.db_tools import get_endtime_info

from time import time
import json
from fastink.inkdb.inkredis import redis_connect
from uuid import uuid4

import logging
logger = logging.getLogger("ink.hpcadapter")

@scheduler("slurm")
class HPC_Scheduler(SchedulerBase):
    def __init__(self, uid: int):
        super().__init__(uid)
        self.CLUSTER_TYPE = "slurm"
    
    def _get_interactive_job_types(self) -> List[str]:
        """Get the list of interactive job types from config."""
        return list(get_config("jobtype").keys())
    
    def _need_dedup(self, job_data: SLURM_JOB) -> bool:
        if job_data.submit_mode is SubmitMode.SYNC:
            return False

        # async mode
        interactive_job_types = self._get_interactive_job_types()
        return job_data.job_type in interactive_job_types

    async def _gen_slurm_submit_cmd(
        self, 
        cpu: int, 
        mem: int,
        jobname: str, 
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
        job_script_abs_path: Optional[str] = None,
        job_input_abs_path: Optional[str] = None,
        output_file: Optional[str]= None,
        error_file: Optional[str] = None,
        job_content: Optional[str] = None
    ):
        if cpu < 1:
            raise ValueError("CPU must be >=1 .")
        
        if mem < 1:
            raise ValueError("MEM must be >= 1 (MB).")
        
        iptables_jobtype = get_config("computing", "iptables_jobtype")
        
        out_path = str(Path(jobdir) / f"%j.out") 
        err_path = str(Path(jobdir) / f"%j.err") 
        
        if jobtype not in iptables_jobtype:
            pathchk = PathChecker()
            
            if output_file:
                if pathchk.is_directory(output_file):
                    raise ValueError(f"Output file cannot be a directory, current value is {output_file}.")
                elif pathchk.is_absolute_path(output_file):
                    out_path = output_file
                elif pathchk.is_filename_only(output_file) :
                    out_path = str(Path(jobdir) / output_file)
            
            if error_file:    
                if pathchk.is_directory(error_file):
                    raise ValueError(f"Error file cannot be a directory, current value is {error_file}.")
                elif pathchk.is_absolute_path(error_file):
                    err_path = error_file
                elif pathchk.is_filename_only(error_file) :
                    err_path = str(Path(jobdir) / error_file)
        
            if job_script_abs_path:
                if not pathchk.is_existed(job_script_abs_path):
                    raise ValueError(f"Job script path not existed, current value is {job_input_abs_path}.")
                if not pathchk.is_absolute_path(job_script_abs_path):
                    raise ValueError(f"Job script must be an absolute path, current value is {job_script_abs_path}.")
            
            if job_input_abs_path:
                if not pathchk.is_existed(job_input_abs_path):
                    raise ValueError(f"Job input file not existed, current value is {job_input_abs_path}.")
                if not pathchk.is_absolute_path(job_input_abs_path):
                    raise ValueError(f"Job input file must be an absolute path, current value is {job_input_abs_path}")
                if not pathchk.is_file(job_input_abs_path):
                    raise ValueError(f"Job input must be a file, current value is {job_input_abs_path}")
            
        args: List[str] = [
            "sbatch",
            "--parsable",
            f"--output={out_path}",
            f"--error={err_path}",
            f"--nodes={nodes}",
            f"--ntasks={ntasks}",
            f"--cpus-per-task={cpu}",
            f"--mem={mem}M",
            f"--job-name={jobname}",
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

        if job_input_abs_path:
            args.append(f"--input={job_input_abs_path}")
            
        # sbatch job_content or job_script_abs_path or prepared job script
        submit_abs_job_script = ""
        interactive_job_types = self._get_interactive_job_types()
        if jobtype in interactive_job_types:
            executable_dir = get_config("computing", "cluster_scripts")
            executable = f"{executable_dir}/{jobtype}/shell.sh"

            with open(executable, "rb") as file:
                submitfile_content = file.read()
            await common.upload_file(src_data=submitfile_content, dst=f"{jobdir}/shell.sh", username=self.USERNAME, mgm=self.XROOTD_PATH, mode="700")

            job_script = f"{executable_dir}/{jobtype}/run.sh"
            with open(job_script, "rb") as file:
                job_script_content = file.read()
            await common.upload_file(src_data=job_script_content, dst=f"{jobdir}/run.sh", username=self.USERNAME, mgm=self.XROOTD_PATH, mode="700")
            
            submit_abs_job_script = f"{jobdir}/shell.sh" 
        else:
            if job_content:
                timestamp = int(time())
                script_file_name = f'inkjob_{self.UID}_{timestamp}.sh'
                submit_abs_job_script = f"{jobdir}/{script_file_name}"
                await common.upload_file(src_data=job_content, dst=submit_abs_job_script, username=self.USERNAME, mgm=self.XROOTD_PATH, mode="700")
                
            elif job_script_abs_path:
                script_file_name = Path(job_script_abs_path).name
                with open(job_script_abs_path, "rb") as file:
                    script_content = file.read()
                submit_abs_job_script = f"{jobdir}/{script_file_name}"
                await common.upload_file(src_data=script_content, dst=submit_abs_job_script, username=self.USERNAME, mgm=self.XROOTD_PATH, mode="700")
            else:
                raise ValueError("Job content or job script cannot both be empty.")
        
        args.append(f"{submit_abs_job_script}")
        submit_command = shlex.join(args)

        _inner = submit_command.replace("'", "'\"'\"'")
        submit_command = f"sudo -iu {shlex.quote(self.USERNAME)} bash -lc '{_inner}'"

        return submit_command, out_path, err_path
                    
    async def submit_job_sync(self, hpc_job_params: SLURM_JOB) -> dict:
        try:
            job_dir = await init_job_dir(self.USERNAME, hpc_job_params.job_type)
            logger.info(f"Init User {self.USERNAME} jobdir {job_dir} finished.")

            submit_cmd, out_path, err_path = await self._gen_slurm_submit_cmd(cpu=hpc_job_params.cpu, 
                                                           mem=hpc_job_params.mem,
                                                           jobname=hpc_job_params.job_name, 
                                                           jobtype=hpc_job_params.job_type,
                                                           jobdir=job_dir,
                                                           partition=hpc_job_params.partition,
                                                           account=hpc_job_params.account,
                                                           qos=hpc_job_params.qos,
                                                           ntasks=hpc_job_params.ntasks,
                                                           nodes=hpc_job_params.nodes,
                                                           gpu_num=hpc_job_params.gpu_num,
                                                           gpu_name=hpc_job_params.gpu_name,
                                                           gpu_type=hpc_job_params.gpu_type,
                                                           job_script_abs_path=hpc_job_params.script_path,
                                                           job_input_abs_path=hpc_job_params.input_path,
                                                           output_file=hpc_job_params.output_file,
                                                           error_file=hpc_job_params.error_file,
                                                           job_content=hpc_job_params.job_script)
            logger.info(f"Generate the slurm submit command for  User({self.USERNAME}) finished, cmd: {submit_cmd}")

            # Submit the slurm job.
            stdout = await sub_command(submit_cmd, 30, "submit job failed.", "submit job timeout.")
            logger.info(f"The slurm submit info: {stdout}")

            job_id_line = stdout.decode().strip()
            job_id = job_id_line.split(";", 1)[0]

            logger.info(f"Submit User {self.USERNAME} job {job_id} to cluster.")
            insert_job_info(self.UID, job_id, out_path, err_path, hpc_job_params.job_type, job_dir, self.CLUSTER_TYPE)
            logger.info(f"Submit job({job_id}) for user({self.USERNAME}) to queue.")

            return {
                "cluster": self.CLUSTER_TYPE,
                "job_id": job_id,
                "job_status": "SUBMITTED"
            }
        
        except Exception as e:
            logger.error(f"Some Wrong in Submit job, the details: {e}")
            raise e

    async def submit_job_async(self, job_data: SLURM_JOB) -> dict:
        """
        Asynchronously submit a Slurm job:
        - Push job parameters into Redis queue
        - Do NOT interact with Slurm directly
        """

        r = redis_connect()
        submit_uuid = str(uuid4())           # submit_uuid for submitting jobs

        cluster = self.CLUSTER_TYPE          # "slurm"
        username = self.USERNAME

        global_queue = f"submitting_jobs:{cluster}"
        user_queue = f"{cluster}_submitting_jobs:{username}"

        # --------------------------------------------------
        # 1. Prevent duplicate submissions (same job_type)
        # --------------------------------------------------
        if self._need_dedup(job_data):
            raw_jobs = await r.lrange(user_queue, 0, -1)

            for raw in raw_jobs:
                job = json.loads(raw)
                if job.get("job_type") == job_data.job_type:
                    logger.debug(
                        f"SLURM-ASYNC: job {job_data.job_type} already in submitting queue"
                    )
                    return {
                        "cluster": cluster,
                        "submit_uuid": submit_uuid,
                        "job_status": "DUPLICATE",
                        "job_id": None,
                        "error": f"job_type '{job_data.job_type}' already in submitting queue"
                    }

        # --------------------------------------------------
        # 2. Build Redis job payload
        # --------------------------------------------------
        submit_payload = {
            "submit_uuid": submit_uuid,
            
            "cluster": cluster,
            "username": username,
            "uid": self.UID,

            # Job identity
            "job_type": job_data.job_type,
            "job_name": job_data.job_name,

            # Resource requirements
            "cpu": job_data.cpu,
            "mem": job_data.mem,
            "gpu_num": job_data.gpu_num,

            # Slurm-specific options
            "partition": job_data.partition,
            "account": job_data.account,
            "qos": job_data.qos,
            "nodes": job_data.nodes,
            "ntasks": job_data.ntasks,
            "gpu_name": job_data.gpu_name,
            "gpu_type": job_data.gpu_type,

            # Job content
            "job_script": job_data.job_script,
            "script_path": job_data.script_path,
            "input_path": job_data.input_path,
            "output_file": job_data.output_file,
            "error_file": job_data.error_file,

            # Status marker
            "job_status": "SUBMITTING",
            # Retry policy
            "retry_count": 0,
            "max_retries": int(get_config("crond", "async_submit_retries", fallback=3)),
            "retry_delay_seconds": int(get_config("crond", "retry_delay_seconds", fallback=10)),
        }

        # --------------------------------------------------
        # 3. Push into Redis (atomic)
        # --------------------------------------------------
        async with r.pipeline(transaction=True) as pipe:
            pipe.rpush(global_queue, json.dumps(submit_payload, ensure_ascii=False))
            pipe.rpush(user_queue, json.dumps(submit_payload, ensure_ascii=False))
            
            # NEW: reserve submit_uuid namespace (job_id unknown yet)
            pipe.set(
                f"submit_uuid:{cluster}:{submit_uuid}",
                "",
                ex=86400
            )
            
            await pipe.execute()

        logger.info(
            f"SLURM-ASYNC: user={username}, jobType={job_data.job_type} "
            f"pushed to redis queue"
        )
        
        return {
            "cluster": "slurm",
            "submit_uuid": submit_uuid,
            "job_status": "SUBMITTING",
            "job_id" : None,
            "query_hint": "use submit_uuid before job_id available"
        }
        
    async def _remove_job_from_queue_by_uuid(
        self,
        queue_key: str,
        submit_uuid: str,
    ) -> bool:
        """
        Safely remove a job from redis list by submit_uuid.
        """
        r = redis_connect()

        raw_jobs = await r.lrange(queue_key, 0, -1)
        for raw in raw_jobs:
            try:
                job = json.loads(raw)
            except Exception:
                continue

            if job.get("submit_uuid") == submit_uuid:
                await r.lrem(queue_key, 1, raw)
                return True

        return False
    
    async def submit_job_from_queue(self, job: dict) -> dict:
        """
        Submit a Slurm job from redis queue payload.
        Called ONLY by async worker.
        """
        r = redis_connect()

        cluster = self.CLUSTER_TYPE
        username = job.get("username")
        uid = job.get("uid")
        job_type = job.get("job_type")
        submit_uuid = job.get("submit_uuid")

        try:
            # --------------------------------------------------
            # 1. Prepare job runtime
            # --------------------------------------------------
            job_dir = await init_job_dir(username,job_type)

            # --------------------------------------------------
            # 2. Generate sbatch command
            # --------------------------------------------------
            submit_cmd, out_path, err_path = await self._gen_slurm_submit_cmd(
                cpu=job.get("cpu"),
                mem=job.get("mem"),
                jobname=job.get("job_name"),
                jobtype=job_type,
                jobdir=job_dir,
                partition=job.get("partition"),
                account=job.get("account"),
                qos=job.get("qos"),
                ntasks=job.get("ntasks", 1),
                nodes=job.get("nodes", 1),
                gpu_num=job.get("gpu_num", 0),
                gpu_name=job.get("gpu_name"),
                gpu_type=job.get("gpu_type"),
                job_script_abs_path=job.get("script_path"),
                job_input_abs_path=job.get("input_path"),
                output_file=job.get("output_file"),
                error_file=job.get("error_file"),
                job_content=job.get("job_script"),
            )

            # --------------------------------------------------
            # 3. Execute sbatch
            # --------------------------------------------------
            stdout = await sub_command(
                submit_cmd,
                10,
                "submit slurm job failed",
                "submit slurm job timeout",
            )

            job_id_line = stdout.decode().strip()
            job_id = job_id_line.split(";", 1)[0]

            # --------------------------------------------------
            # 4. Persist DB
            # --------------------------------------------------
            insert_job_info(
                uid,
                job_id,
                out_path,
                err_path,
                job_type,
                job_dir,
                cluster,
            )

            # --------------------------------------------------
            # 5. Persist Redis job index & detail (NEW)
            # --------------------------------------------------
            
            # Redis: 
            # establish uuid → job_id mapping
            await r.set(f"submit_uuid:{cluster}:{submit_uuid}", job_id, ex=86400)
            # establish reversed mapping for job query : job_id -> uuid
            await r.set(
                f"job_id_to_submit_uuid:{cluster}:{job_id}",
                submit_uuid,
                ex=86400
            )

            
            job_key = f"cluster_jobs:{cluster}:{username}:{job_id}"
            idx_key = f"cluster_jobs:{cluster}:{username}:job_ids"
            
            job_detail = {
                "cluster": cluster,
                "job_id": job_id,
                "submit_uuid": submit_uuid,

                "username": username,
                "uid": uid,

                "job_type": job_type,
                "job_name": job.get("job_name"),

                "job_dir": job_dir,
                "out_path": out_path,
                "err_path": err_path,

                "job_status": "SUBMITTED",
                "submit_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            for k in ("cpu", "mem", "gpu_num", "partition", "qos"):
                if k in job:
                    job_detail[k] = job[k] 
            
            async with r.pipeline(transaction=True) as pipe:
                pipe.hset(job_key, mapping=job_detail)
                pipe.sadd(idx_key, job_id)
                
                await pipe.execute()    
                
            # Remove from submitting queues using UUID-based helper
            removed_global = await self._remove_job_from_queue_by_uuid(
                f"submitting_jobs:{cluster}", submit_uuid
            )
            removed_user = await self._remove_job_from_queue_by_uuid(
                f"{cluster}_submitting_jobs:{username}", submit_uuid
            )

            if not removed_global:
                logger.debug(
                    f"submit_uuid={submit_uuid} not found in global submitting queue submitting_jobs:{cluster} "
                    "(maybe already handled by another worker)"
                )

            if not removed_user:
                logger.debug(
                    f"submit_uuid={submit_uuid} not found in user submitting queue {cluster}_submitting_jobs:{username} "
                    "(maybe already handled by another worker)"
                )
                
            logger.info(
                f"SLURM-WORKER: user={username}, jobType={job_type}, job_id={job_id}"
            )

            return {
                "cluster": cluster,
                "submit_uuid": submit_uuid,
                "job_status": "SUBMITTED",
                "job_id" : job_id
            }
            
        except Exception as e:
            logger.error(f"Failed to submit jobs from queue, the details: {e}")
            raise e    

    async def query_job(self, req_job_type: Optional[str] = None):
        """
        Query Slurm jobs using sacct as the source of truth.
        Redis is only used for:
        1) SUBMITTING async jobs
        2) submit_uuid <-> job_id mapping
        """

        job_list = []
        iptables_jobtype = get_config("computing", "iptables_jobtype")
        start_keywords = get_config("computing", "start_keywords")

        r = redis_connect()  # NEW: reuse redis connection

        # ======================================================
        # 1. Query Slurm jobs via sacct (authoritative source)
        # ======================================================
        sacct_cmd = (
            f"sacct -u {self.USERNAME} "
            "--format=JobID,Partition,State,Elapsed,NNodes,NodeList,"
            "WCkey,Submit,Start,End,WorkDir,Time,SubmitLine "
            "-P -X -n"
        )

        stdout = await sub_command(
            sacct_cmd,
            10,
            "Query user jobs failed.",
            "Query user jobs timeout."
        )

        lines = stdout.decode().strip().split("\n")

        for line in lines:
            if not line:
                continue

            fields = line.split("|")

            job_id = job_id = fields[0].strip()
            partition = fields[1]
            slurm_state = fields[2]
            node_list = fields[5]
            job_type = fields[6]
            submit_time = fields[7].replace("T", " ")
            start_time = fields[8].replace("T", " ")
            end_time = fields[9].replace("T", " ")
            workdir = fields[10]
            submit_line = fields[12]

            if req_job_type:
                if job_type not in req_job_type.split(","):
                    continue

            # --------------------------------------
            # DB sync (insert if not exists)
            # --------------------------------------
            try:
                (
                    db_job_type,
                    db_job_status,
                    ipt_status,
                    ipt_clean,
                ) = get_job_info(self.UID, job_id, self.CLUSTER_TYPE)
            except NoResultFound:
                out_path, err_path = parse_sbatch_out_err(submit_line, job_id)
                insert_job_info(
                    self.UID,
                    job_id,
                    out_path,
                    err_path,
                    job_type,
                    workdir,
                    self.CLUSTER_TYPE,
                )
                (
                    db_job_type,
                    db_job_status,
                    ipt_status,
                    ipt_clean,
                ) = get_job_info(self.UID, job_id, self.CLUSTER_TYPE)

            (connect_sign,) = get_job_connect_info(
                self.UID, job_id, self.CLUSTER_TYPE
            )

            # --------------------------------------
            # Normalize Slurm job state
            # --------------------------------------
            if slurm_state == "PENDING":
                job_status = "QUEUEING"

            elif slurm_state == "RUNNING":
                job_status = "RUNNING"

                if connect_sign == "False":
                    output_content, _ = await get_job_output(
                        uid=self.UID,
                        job_id=job_id,
                        cluster_id=self.CLUSTER_TYPE,
                    )

                    if any(kw in output_content for kw in start_keywords):
                        connect_sign = "True"

                        if job_type in iptables_jobtype:
                            try:
                                await create_iptables(
                                    self.UID,
                                    job_id,
                                    ipt_status,
                                    ipt_clean,
                                    self.CLUSTER_TYPE,
                                )
                            except Exception:
                                connect_sign = "False"

                        update_connect_status(
                            self.UID, job_id, connect_sign, self.CLUSTER_TYPE
                        )
                        update_start_time(
                            self.UID, job_id, start_time, self.CLUSTER_TYPE
                        )

            elif slurm_state in ("COMPLETED", "FAILED") or slurm_state.startswith(
                "CANCELLED"
            ):
                job_status = slurm_state
                if not get_endtime_info(self.UID, job_id, self.CLUSTER_TYPE):
                    update_end_time(
                        self.UID, job_id, end_time, self.CLUSTER_TYPE
                    )

            else:
                continue

            if db_job_status != job_status:
                update_job_status(
                    self.UID, job_id, job_status, self.CLUSTER_TYPE
                )

            # --------------------------------------
            # NEW: resolve submit_uuid via Redis mapping
            # --------------------------------------
            submit_uuid = ""
            uuid_val = await r.get(
                f"job_id_to_submit_uuid:{self.CLUSTER_TYPE}:{job_id}"
            )
            if uuid_val is None:
                submit_uuid = ""
            elif isinstance(uuid_val, bytes):
                submit_uuid = uuid_val.decode()
            else:
                submit_uuid = uuid_val

            job_list.append(
                {
                    "clusterId": self.CLUSTER_TYPE,
                    "jobId": job_id,
                    "submitUuid": submit_uuid,   # NEW
                    "jobPartition": partition,
                    "jobType": job_type,
                    "jobSubmitTime": submit_time,
                    "jobStatus": job_status,
                    "jobStartTime": start_time,
                    "JobNodeList": node_list,
                    "jobtimelimit": fields[11],
                    "connect_sign": connect_sign,
                }
            )

        # ======================================================
        # 2. Append SUBMITTING async jobs from Redis
        # ======================================================
        submitting_key = f"{self.CLUSTER_TYPE}_submitting_jobs:{self.USERNAME}"
        raw_jobs = await r.lrange(submitting_key, 0, -1)

        for raw in raw_jobs:
            try:
                job = json.loads(raw)
            except Exception:
                continue

            job_type = job.get("job_type")

            if req_job_type:
                if job_type not in req_job_type.split(","):
                    continue

            job_list.append(
                {
                    "clusterId": self.CLUSTER_TYPE,
                    "jobId": "",
                    "submitUuid": job.get("submit_uuid"),  # NEW
                    "jobType": job_type,
                    "jobSubmitTime": "",
                    "jobStatus": "SUBMITTING",
                    "jobStartTime": "",
                    "JobNodeList": "",
                    "jobtimelimit": "",
                    "connect_sign": "False",
                }
            )

        # ======================================================
        # 3. Sort and return
        # ======================================================
        job_list.sort(key=jobid_sort_key, reverse=True)
        return job_list


    async def cancel_job(self, job_id: Optional[int] = None, submit_uuid: Optional[str] = None):
        r = redis_connect()

        try:
            # ======================================================
            # Case 1: Job already submitted (job_id exists)
            # ======================================================
            if job_id:
                (
                    job_type,
                    job_status,
                    job_iptables_status,
                    job_iptables_clean,
                ) = get_job_info(self.UID, job_id, self.CLUSTER_TYPE)

                # ---- Slurm cancel ----
                if job_status not in ("COMPLETED", "FAILED", "CANCELLED"):
                    cancel_cmd = f"sudo -u {self.USERNAME} scancel {job_id}"
                    await sub_command(cancel_cmd, 10, "scancel err", "scancel timeout")

                # ---- iptables cleanup ----
                iptables_jobtype = get_config("computing", "iptables_jobtype")
                if job_type in iptables_jobtype:
                    if job_iptables_status != 0 and job_iptables_clean == 0:
                        delete_iptables(self.UID, job_id, job_iptables_status, self.CLUSTER_TYPE)

                # ---- DB update ----
                update_job_status(self.UID, job_id, "CANCELLED", self.CLUSTER_TYPE)
                end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                update_end_time(self.UID, job_id, end_time, self.CLUSTER_TYPE)

                # ---- Redis update (async submitted job!) ----
                job_key = f"cluster_jobs:{self.USERNAME}:{job_id}"
                if await r.exists(job_key):
                    await r.hset(job_key, mapping={
                        "jobStatus": "CANCELLED",
                        "jobEndTime": end_time,
                    })

                return {"status": "CANCELLED", "job_id": job_id}

            # ======================================================
            # Case 2: Async SUBMITTING (no job_id yet)
            # ======================================================
            if submit_uuid:
                queue_key = f"{self.CLUSTER_TYPE}_submitting_jobs:{self.USERNAME}"
                raw_jobs = await r.lrange(queue_key, 0, -1)

                for raw in raw_jobs:
                    job = json.loads(raw)
                    if job.get("submit_uuid") == submit_uuid:
                        await r.lrem(queue_key, 1, raw)
                        return {"status": "CANCELLED", "submit_uuid": submit_uuid}

                return {"status": "NOT_FOUND"}

            raise ValueError("job_id or submit_uuid required")

        except Exception:
            logger.exception("Cancel job failed")
            raise

    async def cancel_job(
        self,
        *,
        submit_uuid: str | None = None,
        job_id: str | None = None,
    ) -> dict:
        """
        Cancel a job.

        Supports:
        - Async jobs identified by submit_uuid
        - Sync jobs identified by job_id
        Operation is idempotent and safe for concurrent workers.
        """

        if not submit_uuid and not job_id:
            raise ValueError("Either submit_uuid or job_id must be provided")

        r = redis_connect()
        cluster = self.CLUSTER_TYPE
        username = self.USERNAME

        resolved_job_id = None

        # ==================================================
        # 1. Resolve job_id
        # ==================================================

        # 1.1 job_id explicitly provided (SYNC or ASYNC)
        if job_id:
            resolved_job_id = str(job_id)

        # 1.2 async path: resolve job_id from submit_uuid
        elif submit_uuid:
            job_id_from_redis = await r.get(
                f"submit_uuid:{cluster}:{submit_uuid}"
            )
            if job_id_from_redis:
                resolved_job_id = job_id_from_redis.decode()

        # ==================================================
        # 2. If job_id is known → cancel Slurm job
        # ==================================================
        if resolved_job_id:
            try:
                await sub_command(
                    f"scancel {resolved_job_id}",
                    5,
                    f"scancel job {resolved_job_id} failed",
                    f"scancel job {resolved_job_id} timeout",
                )
            except Exception as e:
                # Job may already be finished or cancelled
                logger.warning(
                    f"cancel_job: scancel job_id={resolved_job_id} failed "
                    f"or job already ended: {e}"
                )

            # Best-effort DB update (sync & async share this path)
            try:
                update_job_status(
                    self.UID,
                    resolved_job_id,
                    "CANCELLED",
                    cluster,
                )
            except Exception:
                pass

            return {
                "cluster": cluster,
                "submit_uuid": submit_uuid,
                "job_id": resolved_job_id,
                "job_status": "CANCELLED",
            }

        # ==================================================
        # 3. No job_id → treat as ASYNC SUBMITTING job
        # ==================================================
        # (Only possible if submit_uuid is provided)
        removed_global = await self._remove_job_from_queue_by_uuid(
            f"submitting_jobs:{cluster}", submit_uuid
        )
        removed_user = await self._remove_job_from_queue_by_uuid(
            f"{cluster}_submitting_jobs:{username}", submit_uuid
        )

        if removed_global or removed_user:
            logger.info(
                f"cancel_job: submit_uuid={submit_uuid} removed from submitting queue"
            )
        else:
            logger.debug(
                f"cancel_job: submit_uuid={submit_uuid} not found in submitting queues "
                "(already cancelled or already submitted)"
            )

        return {
            "cluster": cluster,
            "submit_uuid": submit_uuid,
            "job_id": None,
            "job_status": "CANCELLED",
        }


