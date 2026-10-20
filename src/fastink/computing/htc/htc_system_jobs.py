import json
import re
from shlex import quote
from fastink.computing.tools.common.utils import sub_command
from fastink.computing.apps import registry as computing_registry
from fastink.common.logger import logger
from fastink.inkdb.inkredis import *

'''
Author:         guocq@ihep.ac.cn
Created:        2024-12-18
Last Modified:  2024-12-18
'''


def _system_jobtypes() -> list:
    """Job types to count in the system totals, as a sorted identifier list.

    Baseline is the app registry -- ``computing_registry.names()`` is the same
    authoritative "which job types exist" source that ``list_job_type_capabilities``
    (the /list_job_types endpoint) and ``job_prepare`` already use.  The config
    ``jobtype`` section only carries *site overrides* and is empty in a
    generic/quickstart deploy, so it cannot be the sole source: keying off it
    alone made the totals collapse to nothing there even though the built-in apps
    (vscode, vnc, jupyter, ...) are registered and submittable.  Union the two so
    both registered apps and any site-configured extra types are counted.

    Values are interpolated into a shell ``condor_q -const`` string by the caller,
    so only plain identifiers are kept; anything else is dropped with a warning
    rather than silently -- silent loss of a job type is exactly the bug this
    function exists to prevent.  The isinstance guard mirrors
    ``hpc.v2.hpc_job_types.get_returnable_hpc_job_types``.
    """
    names = set(computing_registry.names())

    try:
        configured = get_config("jobtype") or {}
    except Exception:
        configured = {}
    if not isinstance(configured, dict):
        # Tolerate a jobtype section rendered as a list/str rather than a mapping.
        configured = list(configured) if isinstance(configured, (list, tuple, set)) else []
    for jt in configured:
        jt = str(jt).strip()
        if jt:
            names.add(jt)

    safe, dropped = [], []
    for jt in sorted(names):
        if re.fullmatch(r"[A-Za-z0-9_]+", jt):
            safe.append(jt)
        else:
            dropped.append(jt)
    if dropped:
        logger.warning(
            "get_system_jobs: dropping non-identifier job type(s) %s "
            "(cannot be safely interpolated into condor_q -const)", dropped,
        )
    return safe


async def get_system_jobs():
    
    try: 
        r = redis_connect()
        job_total_list = await r.get('system_job_list')

        if job_total_list:
            job_total_list = json.loads(job_total_list)
            formatted_job = {key.capitalize(): value for key, value in job_total_list.items()}
            
            return formatted_job
    
        else:
            # Registry (authoritative app list) unioned with any site-configured
            # jobtype keys -- see _system_jobtypes().  The old hardcoded 9-type
            # dict missed compile, npu, asic, asicbm and ink_special entirely, so
            # their jobs were never reflected in these totals.
            jobtypes = _system_jobtypes()
            if not jobtypes:
                # Defensive last resort: no registered apps AND no configured
                # job types (a broken/empty deployment).  Return empty totals
                # rather than build a `condor_q -const ''`, which condor rejects
                # as a parse error.  Unreachable once any app is registered,
                # since _system_jobtypes() baselines on the registry.
                await r.setex('system_job_list', 600, json.dumps({}))
                return {}
            job_total_list = {jt: 0 for jt in jobtypes}
            job_queue_list = {jt: 0 for jt in jobtypes}
            job_running_list = {jt: 0 for jt in jobtypes}
            
            
            SCHEDD_HOST = get_config("computing", "schedd_host")
        
            type_filter = " || ".join(
                f'HepJob_JobType == "{jt}"' for jt in jobtypes
            )
            command = (
                f"condor_q "
                f"-name {quote(SCHEDD_HOST)} "
                f"-const '{type_filter}' "
                "-af Owner ClusterId ProcId HepJob_RealGroup Qdate JobStatus JobStartDate RemoteHost HepJob_JobType"
            )
            
            stdout = await sub_command(command, 5, "Get queue jobs failed.", "Get queue jobs timeout.")
            
            lines = stdout.decode().strip().split('\n')
            if lines != ['']:
                for line in lines:
                    job_param_list = line.split()
                    HepJob_JobType = job_param_list[8]
                    # Defensive: -const already restricts to jobtypes, but never
                    # KeyError on an unexpected/blank value.
                    if HepJob_JobType not in job_total_list:
                        continue

                    if job_param_list[5] == '1':
                        job_queue_list[HepJob_JobType] += 1

                    elif job_param_list[5] == '2':
                        job_running_list[HepJob_JobType] += 1
              
            for key in job_total_list:
                job_total_list[key] = job_queue_list[key] + job_running_list[key]
                
            await r.setex('system_job_list', 600, json.dumps(job_total_list))    
                
            formatted_job = {key.capitalize(): value for key, value in job_total_list.items()}    
        
            return formatted_job
            
    except Exception as e:
        logger.error(f"Get system job statistic info failed in get_system_jobs func, details: {e}.")
        raise e

        
        
