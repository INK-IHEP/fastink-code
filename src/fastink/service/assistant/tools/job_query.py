import re
from typing import Any

from fastink.common.config import get_config
from fastink.common.logger import logger
from fastink.computing.adapter.strategy import get_scheduler
from fastink.service.monitor import get_job_monitor_url


JOB_QUERY_KEYWORDS = (
    "job",
    "jobs",
    "queue",
    "running",
    "status",
    "monitor",
    "作业",
    "运行",
    "状态",
    "排队",
    "监控",
)

JOB_CANCEL_KEYWORDS = (
    "cancel",
    "stop",
    "terminate",
    "kill",
    "取消作业",
    "终止作业",
    "停止作业",
    "撤销作业",
    "删掉作业",
)

OUTPUT_KEYWORDS = ("output", "log", "stdout", "stderr", "输出", "日志", "报错")


def should_query_jobs(message: str) -> bool:
    text = message.lower()
    return any(keyword in text or keyword in message for keyword in JOB_QUERY_KEYWORDS)


def is_cancel_request(message: str) -> bool:
    text = message.lower()
    return any(keyword in text or keyword in message for keyword in JOB_CANCEL_KEYWORDS)


def wants_job_output(message: str) -> bool:
    text = message.lower()
    return any(keyword in text or keyword in message for keyword in OUTPUT_KEYWORDS)


def extract_job_id(message: str) -> str:
    match = re.search(r"(?<!\d)(\d+(?:\.\d+)?)(?!\d)", message)
    if not match:
        return ""
    return match.group(1)


def resolve_cluster_ids(cluster_id: str | None = None) -> list[str]:
    if cluster_id:
        return [cluster_id]

    cluster_list = get_config("computing", "cluster_list", fallback=[])
    if isinstance(cluster_list, str):
        return [item.strip() for item in cluster_list.split(",") if item.strip()]
    return [str(item).strip() for item in cluster_list if str(item).strip()]


def _status_rank(job_status: str) -> int:
    rank = {
        "RUNNING": 0,
        "QUEUEING": 1,
        "SUBMITTING": 2,
        "HOLDING": 3,
        "COMPLETED": 10,
        "FAILED": 11,
        "CANCELLED": 12,
    }
    return rank.get(job_status.upper(), 20)


def _normalize_job_card(raw_job: dict[str, Any]) -> dict[str, Any]:
    job_id = str(raw_job.get("jobId", "") or "")
    return {
        "cluster_id": str(raw_job.get("clusterId", "") or ""),
        "job_id": job_id,
        "submit_uuid": str(raw_job.get("submitUuid", "") or ""),
        "job_type": str(raw_job.get("jobType", "") or ""),
        "job_status": str(raw_job.get("jobStatus", "") or ""),
        "submit_time": str(raw_job.get("jobSubmitTime", "") or ""),
        "start_time": str(raw_job.get("jobStartTime", "") or ""),
        "node_list": str(raw_job.get("JobNodeList", "") or ""),
        "connect_sign": str(raw_job.get("connect_sign", "") or ""),
        "monitor_url": get_job_monitor_url(job_id) if job_id else "",
    }


async def collect_user_jobs(username: str, *, cluster_id: str | None = None) -> tuple[list[dict[str, Any]], list[str]]:
    job_cards: list[dict[str, Any]] = []
    errors: list[str] = []

    for resolved_cluster in resolve_cluster_ids(cluster_id):
        try:
            adapter = get_scheduler(resolved_cluster, username)
            jobs = await adapter.query_job(None) or []
            job_cards.extend(_normalize_job_card(job) for job in jobs)
        except Exception as exc:
            logger.exception(
                "assistant.query_jobs failed, username=%s, cluster_id=%s, error=%s",
                username,
                resolved_cluster,
                exc,
            )
            errors.append(f"{resolved_cluster}: {exc}")

    job_cards.sort(key=lambda item: _status_rank(str(item.get("job_status", ""))))
    return job_cards, errors


def find_job_card(job_cards: list[dict[str, Any]], job_id: str) -> dict[str, Any] | None:
    if not job_id:
        return None
    for job_card in job_cards:
        if str(job_card.get("job_id")) == str(job_id):
            return job_card
    return None
