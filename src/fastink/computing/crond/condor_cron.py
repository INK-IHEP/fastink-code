import htcondor
import json, shlex
from shlex import quote
from fastink.common.logger import logger
from fastink.common.config import get_config
from fastink.inkdb.inkredis import redis_connect
from fastink.computing.tools.db.db_tools import update_end_time, update_job_status, update_start_time, get_jobs_with_null_times, delete_jobinfo_by_jobids, insert_job_info, needto_change_status_jobs
from fastink.computing.tools.common.utils import safe_get, safe_int, ts_to_str, sub_command, delete_iptables, change_username_to_uid, init_job_dir, generate_condor_submit, generate_submit_command, clean_query_value, check_user_kerberos_ticket


def query_cluster_jobs():

    SCHEDD_HOST = get_config("computing", "schedd_host")

    BASE_CMD = f"condor_q -name {quote(SCHEDD_HOST)} --allusers "
    BASE_ATTRS = [
        "Owner", "ClusterId", "ProcId", "HepJob_RealGroup", "Qdate",
        "JobStatus", "JobStartDate", "RemoteHost", "HepJob_JobType", "HepJob_RequestOS"
    ]
    EXTRA_ATTRS = ["Iwd", "Out", "Err", "holdreason"]
    attrs = BASE_ATTRS + EXTRA_ATTRS

    command = (
        f"{BASE_CMD} "
        f"-af:V {' '.join(attrs)}"
    )

    return command


def get_history_records(jobid):
    schedd_host = get_config("computing", "schedd_host")
    coll = htcondor.Collector()
    schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd, schedd_host)
    schedd = htcondor.Schedd(schedd_ad)

    ads = schedd.history(
        constraint=f"'ClusterId == {jobid}'",
        projection=[
            "EnteredCurrentStatus",
            "JobStartDate",
            "QDate",
            "HepJob_JobType",
            "Owner",
            "ClusterId",
        ],
        match=-1,
    )

    results = []
    for ad in ads:
        entered_current_status = ad.get("EnteredCurrentStatus")
        job_start_date = ad.get("JobStartDate", "")
        job_type = ad.get("HepJob_JobType")
        
        job_info = {
            "job_end_time": ts_to_str(entered_current_status),
            "job_start_time": ts_to_str(job_start_date),
            "job_type": job_type
        }

        results.append(job_info)

    return results


def get_condor_history_command(job_id: str) -> str:
    SCHEDD_HOST = get_config("computing", "schedd_host")
    BASE_CMD = f"condor_history -name {quote(SCHEDD_HOST)} -limit 1"
    ATTRS = [
        'formatTime(EnteredCurrentStatus,"%Y-%m-%d %H:%M:%S")',
        'ifThenElse(isUndefined(JobStartDate),"NULL",formatTime(JobStartDate,"%Y-%m-%d"))',
        'ifThenElse(isUndefined(JobStartDate),"NULL",formatTime(JobStartDate,"%H:%M:%S"))',
        'formatTime(QDate,"%Y-%m-%d %H:%M:%S")',
        'ifThenElse(isUndefined(HepJob_JobType) || HepJob_JobType == "", "batch", HepJob_JobType)',
        "Owner"
    ]
    attrs_quoted = " ".join(quote(a) for a in ATTRS)   # 关键：给每个字段加 shell 引号
    command = f"{BASE_CMD} {quote(str(job_id))} -af {attrs_quoted}"

    logger.info(f"The history command: {command}")

    return command


async def get_redis_all_jobs():
    r = redis_connect()
    cursor = 0
    jobs = []

    while True:
        cursor, keys = await r.scan(cursor=cursor, match="cluster_jobs:*:*", count=1000)

        for key in keys:
            if isinstance(key, bytes):
                key = key.decode()

            parts = key.split(":")
            if len(parts) == 3 and parts[0] == "cluster_jobs" and parts[2] != "job_ids" and parts[2].isdigit():
                jobs.append({
                    "key": key,
                    "user": parts[1],
                    "job_id": parts[2],
                })

        if int(cursor) == 0:
            break

    return jobs


def condor_schedd_query():

    SCHEDD_HOST = get_config("computing", "schedd_host")
    try:
        coll = htcondor.Collector()
        schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd, SCHEDD_HOST)
        schedd = htcondor.Schedd(schedd_ad)
        
        jobs = schedd.query(
            projection=["ClusterId", "Owner", "Qdate", "JobStatus", "JobStartDate", "RemoteHost", "HepJob_JobType", "HepJob_RequestOS", "IWD", "Out", "Err", "Holdreason"]
        )

        job_list = []

        for job in jobs:
            cluster_id = job.get("ClusterId")
            owner = job.get("Owner", "Unknown")
            status_code = job.get("JobStatus")
            qdate = ts_to_str(job.get("Qdate"))
            starttime = ts_to_str(job.get("JobStartDate"))
            host = job.get("RemoteHost")
            jobtype = job.get("HepJob_JobType")
            job_request_os = job.get("HepJob_RequestOS")
            job_out_path = job.get("Out")
            job_err_path = job.get("Err")
            job_hold_reason = job.get("Holdreason")
            job_iwd = job.get("IWD")

            job_info = {
                "ClusterId": "HTCondor",
                "jobId": clean_query_value(f"{cluster_id}"),
                "jobType": clean_query_value(jobtype),
                "jobOwner": clean_query_value(owner),
                "jobStatus": clean_query_value(status_code),
                "jobSubmitTime": clean_query_value(qdate),
                "jobStartTime": clean_query_value(starttime),
                "jobNodeList": clean_query_value(host),
                "jobrunos": clean_query_value(job_request_os),
                "jobiwd": clean_query_value(job_iwd),
                "joboutpath": clean_query_value(job_out_path),
                "joberrpath": clean_query_value(job_err_path),
                "hold_reason": clean_query_value(job_hold_reason)
            }
            job_list.append(job_info)

        return job_list

    except Exception as e:
        logger.error(f"HTC-CROND-LOG: Condor API query job failed, and details: {e}")
        return []


async def update_completed_jobs():
    try:
        r = redis_connect()
        iptables_jobtype = get_config("computing", "iptables_jobtype")
        need_change_status_jobs = needto_change_status_jobs()
        query_command = query_cluster_jobs()
        logger.debug(f"HTC-CROND-QUEUE-LOG: The queue command: {query_command}")
        stdout = await sub_command(query_command, 10, "Query user jobs failed.", "Query user jobs timeout.")
        lines = stdout.decode().strip().split('\n')
        logger.debug(f"HTC-CROND-QUEUE-LOG: Queue jobs {lines}")
        to_delete = []
        
        if lines != ['']:
            for line in lines:
                job_param_list = shlex.split(line, posix=True)
                job_clusterid = safe_int(safe_get(job_param_list, 1))
                
                if job_clusterid in need_change_status_jobs.keys():
                    del need_change_status_jobs[job_clusterid]      

        if need_change_status_jobs:
            logger.debug(f"HTC-CROND-QUEUE-LOG: Need change status jobs: {need_change_status_jobs}")
            for key in need_change_status_jobs:
                query_history_command = get_condor_history_command(key)
                stdout = await sub_command(query_history_command, 30, "Exec condorhistory func failed.", "Exec condorhistory func timeout.")
                history_job_lines = stdout.decode().strip().split('\n')

                if history_job_lines != [""]:
                    job_param_list = history_job_lines[0].split()
                    job_end_time = f"{job_param_list[0]} {job_param_list[1]}" 
                    if job_param_list[2] != "NULL":
                        job_start_time = f"{job_param_list[2]} {job_param_list[3]}"
                    else:
                        job_start_time = f"{job_param_list[4]} {job_param_list[5]}"
                    job_type = job_param_list[6]
                    job_user = job_param_list[7]
                    job_uid = change_username_to_uid(job_user)

                    if job_type in iptables_jobtype:
                        gateway_port = need_change_status_jobs[key][1]
                        sshd_job_iptables_clean = need_change_status_jobs[key][2]
                        if gateway_port != 0 and sshd_job_iptables_clean == 0:
                            delete_iptables(job_uid, key, gateway_port, "htcondor")
                    
                    update_job_status(job_uid, key, 'COMPLETED', "htcondor")
                    update_start_time(job_uid, key, job_start_time, "htcondor")
                    update_end_time(job_uid, key, job_end_time, "htcondor")
                    logger.debug(f"HTC-CROND-QUEUE-LOG: Update job {key} status to COMPLETED.")
                    
                    JOB_KEY = f"cluster_jobs:{job_user}:{key}"
                    IDX_KEY = f"cluster_jobs:{job_user}:job_ids"
                    await r.delete(JOB_KEY)
                    await r.srem(IDX_KEY, str(key))
                else:
                    to_delete.append(key)
                    
            if to_delete:
                logger.debug(f"HTC-CROND-QUEUE-LOG: Need to delete jobs: {to_delete}")
                delete_jobinfo_by_jobids(to_delete)
                        
    except Exception as e:
        logger.exception(f"HTC-CROND-QUEUE-LOG: update_completed_jobs failed, the details: {e}")


async def refresh_redis_job_status():
    try:
        r = redis_connect()
        pipe = r.pipeline(transaction=False)

        condor_joblist = []
        condor_q_jobs = condor_schedd_query()
        for job in condor_q_jobs:
            job_owner = job.get("jobOwner")
            job_clusterid = job.get("jobId")
            condor_joblist.append(job_clusterid)

            tomb = await r.exists(f"cluster_jobs:deleted:{job_owner}:{job_clusterid}")
            if tomb:
                continue

            JOB_KEY = f"cluster_jobs:{job_owner}:{job_clusterid}"
            IDX_KEY = f"cluster_jobs:{job_owner}:job_ids"
            pipe.sadd(IDX_KEY, job_clusterid)
            pipe.hset(JOB_KEY, mapping=job)

        await pipe.execute()

        redis_jobs = await get_redis_all_jobs()

        for job in redis_jobs:
            job_id = job.get("job_id")
            job_user = job.get("user")
            if job_id not in condor_joblist:
                # delete redis record
                JOB_KEY = f"cluster_jobs:{job_user}:{job_id}"
                IDX_KEY = f"cluster_jobs:{job_user}:job_ids"
                await r.delete(JOB_KEY)
                await r.srem(IDX_KEY, str(job_id))
                logger.debug(f"HTC-CROND-QUEUE-LOG: delete the redis expire job {JOB_KEY}")

    except Exception as e:
        logger.exception(f"HTC-CROND-QUEUE-LOG: refresh_redis_job_status failed, the details: {e}")


def gen_history_list_command() -> str:
    SCHEDD_HOST = get_config("computing", "schedd_host")
    BASE_CMD = f"condor_history -name {quote(SCHEDD_HOST)}"
    ATTRS = [
        'formatTime(EnteredCurrentStatus,"%Y-%m-%d %H:%M:%S")',
        'ifThenElse(isUndefined(JobStartDate),"NULL",formatTime(JobStartDate,"%Y-%m-%d"))',
        'ifThenElse(isUndefined(JobStartDate),"NULL",formatTime(JobStartDate,"%H:%M:%S"))',
        'formatTime(QDate,"%Y-%m-%d %H:%M:%S")',
        "Owner",
        "ClusterId"
    ]
    attrs_quoted = " ".join(quote(a) for a in ATTRS)
    command = f"{BASE_CMD} -af {attrs_quoted}"

    logger.debug(f"The reset DB history command: {command}")

    return command


async def resert_start_end_time():
    try:

        time_null_jobs = get_jobs_with_null_times()
        logger.info(f"The DB time null jobs: {time_null_jobs}")
        
        if not time_null_jobs:
            return

        time_null_set = set(map(str, time_null_jobs))
        
        query_command = gen_history_list_command()
        stdout = await sub_command(query_command, 20, "Query history jobs failed.", "Query history jobs timeout.")
        lines = stdout.decode(errors="ignore").splitlines()
        lines = [ln for ln in lines if ln.strip()]

        history_ids = set()
        history_map = {}

        for ln in lines:
            parts = ln.split()
            end_time = f"{parts[0]} {parts[1]}"

            if parts[2] != "NULL":
                start_time = f"{parts[2]} {parts[3]}"
            else:
                start_time = f"{parts[4]} {parts[5]}"

            user = parts[6]
            clusterid = parts[7]
            uid = change_username_to_uid(user)

            history_ids.add(clusterid)
            history_map[clusterid] = (uid, start_time, end_time, user)

        found = time_null_set & history_ids
        missing = time_null_set - history_ids
        
        for clusterid in found:
            uid, start_time, end_time, user = history_map[clusterid]
            update_start_time(uid, clusterid, start_time, "htcondor")
            update_end_time(uid, clusterid, end_time, "htcondor")
            logger.debug(f"Update {user} job {clusterid} start and end time in DB.")
            
        stdout_q = await sub_command(
            query_cluster_jobs(),
            10,
            "Query cluster jobs failed.",
            "Query cluster jobs timeout."
        )
        q_lines = [ln for ln in stdout_q.decode(errors="ignore").splitlines() if ln.strip()]
        active_jobs = set()
        for ln in q_lines:
            parts = ln.split()
            if len(parts) >= 2:
                active_jobs.add(parts[1])

        delete_jobs = missing - active_jobs
        
        if delete_jobs:
            delete_jobinfo_by_jobids(list(delete_jobs))
    
    except Exception:
        logger.exception("resert_start_end_time: failed")


async def submit_job_from_redis():
    try:   
        r = redis_connect()
        while True:
            raw_job = None
            try:
                raw_job = await r.rpop("submitting_jobs")
                if not raw_job:
                    break
                logger.debug(f"HTC-ASYNC-LOG: Pop the raw info: {raw_job}")

                job = json.loads(raw_job)
                job_owner = job.get("username")
                job_type = job.get("jobType")
                job_cpu = job.get("jobReqCPU")
                job_mem = job.get("jobReqMEM")
                job_os = job.get("jobReqOS")
                job_wn = job.get("jobReqWN")
                job_arch = job.get("jobReqARCH")
                job_params = job.get("jobReqParam")
                cluster_id = job.get("clusterId")

                uid = change_username_to_uid(job_owner)
                job_dir = await init_job_dir(job_owner, job_type)
                logger.debug(f"HTC-ASYNC-LOG: Init user dir {job_dir} successfully.")

                submit_file = await generate_condor_submit(job_owner, job_cpu, job_mem, job_type, job_dir, job_os, job_wn, job_arch, job_params)
                await check_user_kerberos_ticket(job_owner, uid, job_dir)

                submit_command = generate_submit_command(job_owner, job_dir, job_type, submit_file)
                logger.debug(f"HTC-ASYNC-LOG: Generate User {job_owner} submit command {submit_command} finished.")
                stdout = await sub_command(submit_command, 120, "submit job failed.", "submit job timeout.")

                job_id_line = stdout.decode().strip()
                job_id = job_id_line.split()[-1].rstrip('.')
                output = f"{job_dir}/{job_id}.out"
                errpath = f"{job_dir}/{job_id}.err"
                logger.debug(f"HTC-ASYNC-LOG: Submit User {job_owner} job {job_type} {job_id} to cluster.")

                insert_job_info(uid, job_id, output, errpath, job_type, job_dir, cluster_id)
                logger.debug(f"HTC-ASYNC-LOG: Submit {job_owner} job {job_id} to queue.")
                
                job_record = {
                    "ClusterId": "HTCondor",
                    "jobId": job_id,
                    "jobType": job_type or "",
                    "jobStatus": "1",
                    "jobSubmitTime": "",
                    "jobStartTime": "",
                    "jobNodeList": "",
                    "jobrunos": job_os or "",
                    "jobiwd": job_dir or "",
                    "joboutpath": output or "",
                    "joberrpath": errpath or "",
                    "hold_reason": ""
                }
                
                JOB_KEY = f"cluster_jobs:{job_owner}:{job_id}"
                IDX_KEY = f"cluster_jobs:{job_owner}:job_ids"
                async with r.pipeline(transaction=True) as pipe:
                    pipe.sadd(IDX_KEY, str(job_id))
                    pipe.hset(JOB_KEY, mapping=job_record)
                    pipe.lrem(f"{job_owner}_submitting_jobs", 1, raw_job)
                    await pipe.execute()

            except Exception as e:
                if raw_job:
                    await r.lrem(f"{job_owner}_submitting_jobs", 1, raw_job)
                logger.error(f"HTC-ASYNC-LOG: Submit job failed, {e}")
                continue

    except Exception as e:
        logger.error(f"HTC-ASYNC-LOG: Some Wrong in Submit job, the details: {e}")
        raise e
    

            