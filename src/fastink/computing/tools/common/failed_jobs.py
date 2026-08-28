"""Per-user failed-submission records (Redis-backed, self-expiring).

When an async job submission fails permanently (condor cron worker throws,
or the slurm worker exhausts its retries), the job used to silently vanish
from the user's ``*_submitting_jobs`` queue -- the only trace was a server
log line.  This module gives each failure a short-lived, user-visible
record so that:

* ``query_job`` can list the entry with ``jobStatus: "FAILED"`` and a
  truncated reason in ``hold_reason`` (the field the frontend already
  renders for held jobs);
* ``get_joboutput`` can return the full failure reason as the job's
  "error" stream;
* ``cancel_job`` can dismiss the record on user request.

Schema
------
::

    failed_jobs:{username}:{fail_id}    hash, TTL = FAILED_JOB_TTL_SEC
        jobType     app name (vnc / vscode / ...)
        clusterId   htcondor | slurm | ...
        failReason  full exception text
        failTime    "YYYY-mm-dd HH:MM:SS"
        jobReqCPU / jobReqMEM / jobReqOS   original request params ("" ok)
    failed_jobs:{username}:ids          set of fail_ids (no TTL; pruned lazily)

``fail_id`` is the failure timestamp in epoch **milliseconds**.  Being
numeric it can travel through every existing ``job_id: int`` API parameter
(get_joboutput, delete_job) without any signature change, and it can never
collide with a real HTCondor/Slurm job id (those are < 10^9; epoch-ms is
> 1.7 * 10^12).
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastink.common.logger import logger

# Records self-expire after 24 h: the "show only recent failures" window
# and the storage cleanup are the same mechanism by design.
FAILED_JOB_TTL_SEC = 86400

# Truncation length for the reason copied into ``hold_reason`` (the full
# text stays in the hash and is returned by get_joboutput).
HOLD_REASON_MAX_LEN = 200


def _hash_key(username: str, fail_id: str) -> str:
    return f"failed_jobs:{username}:{fail_id}"


def _index_key(username: str) -> str:
    return f"failed_jobs:{username}:ids"


async def record_failed_job(
    r,
    username: str,
    cluster_id: str,
    job_type: str,
    reason: str,
    *,
    req_cpu: str = "",
    req_mem: str = "",
    req_os: str = "",
) -> str:
    """Persist one failed-submission record; returns the new fail_id."""
    fail_id = str(int(time.time() * 1000))
    mapping = {
        "jobType": job_type or "",
        "clusterId": cluster_id or "",
        "failReason": str(reason or "")[:10000],
        "failTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "jobReqCPU": str(req_cpu or ""),
        "jobReqMEM": str(req_mem or ""),
        "jobReqOS": str(req_os or ""),
    }
    async with r.pipeline(transaction=True) as pipe:
        pipe.hset(_hash_key(username, fail_id), mapping=mapping)
        pipe.expire(_hash_key(username, fail_id), FAILED_JOB_TTL_SEC)
        pipe.sadd(_index_key(username), fail_id)
        await pipe.execute()
    logger.info(
        "Recorded failed submission for user=%s type=%s cluster=%s fail_id=%s",
        username, job_type, cluster_id, fail_id,
    )
    return fail_id


async def list_failed_jobs(r, username: str) -> List[Dict[str, Any]]:
    """All live failed records for a user (hash contents + ``failId``).

    Index entries whose hash has already expired are pruned as a side
    effect, keeping the set from growing unboundedly.
    """
    fail_ids = await r.smembers(_index_key(username))
    if not fail_ids:
        return []

    results: List[Dict[str, Any]] = []
    stale: List[str] = []
    for fail_id in fail_ids:
        data = await r.hgetall(_hash_key(username, fail_id))
        if not data:
            stale.append(fail_id)
            continue
        data["failId"] = fail_id
        results.append(data)

    if stale:
        await r.srem(_index_key(username), *stale)

    return results


async def get_failed_job(r, username: str, fail_id: str) -> Optional[Dict[str, Any]]:
    """Single record lookup; None when absent/expired."""
    data = await r.hgetall(_hash_key(username, str(fail_id)))
    return data or None


async def delete_failed_job(r, username: str, fail_id: str) -> bool:
    """Dismiss one record (user clicked delete).  True if it existed."""
    fail_id = str(fail_id)
    async with r.pipeline(transaction=True) as pipe:
        pipe.delete(_hash_key(username, fail_id))
        pipe.srem(_index_key(username), fail_id)
        deleted, _ = await pipe.execute()
    return bool(deleted)


def failed_job_query_entry(cluster_type: str, record: Dict[str, Any]) -> Dict[str, Any]:
    """Shape one record like a query_job list entry.

    ``hold_reason`` carries the truncated reason because that is the
    field the frontend already displays; the full text is available via
    get_joboutput using ``failId`` as the job id.
    """
    reason = record.get("failReason", "")
    if len(reason) > HOLD_REASON_MAX_LEN:
        reason = reason[:HOLD_REASON_MAX_LEN] + "..."
    return {
        "clusterId": cluster_type,
        "jobId": record.get("failId", ""),
        "jobType": record.get("jobType", ""),
        "jobSubmitTime": record.get("failTime", ""),
        "jobStatus": "FAILED",
        "jobStartTime": "",
        "JobNodeList": "",
        "jobrunos": record.get("jobReqOS", ""),
        "connect_sign": "False",
        "hold_reason": reason,
        "jobtimelimit": "",
    }
