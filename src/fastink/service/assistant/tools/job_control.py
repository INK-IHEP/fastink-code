from typing import Any

from fastink.computing.adapter.strategy import get_scheduler


async def cancel_user_job(
    username: str,
    *,
    cluster_id: str,
    job_id: str | None = None,
    submit_uuid: str | None = None,
) -> dict[str, Any]:
    adapter = get_scheduler(cluster_id, username)
    return await adapter.cancel_job(job_id=job_id, submit_uuid=submit_uuid)
