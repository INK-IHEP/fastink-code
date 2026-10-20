from fastink.common.config import get_config


def get_returnable_hpc_job_types() -> set[str]:
    """Return Slurm job types that should be visible through Ink."""
    try:
        configured = get_config("jobtype") or {}
    except Exception:
        configured = {}

    if isinstance(configured, dict):
        job_types = configured.keys()
    else:
        job_types = configured

    return {
        str(job_type).strip().lower()
        for job_type in job_types
        if str(job_type).strip()
    } | {"common"}


def requested_hpc_job_types(job_type: str | None) -> set[str] | None:
    if not job_type:
        return None

    requested = {
        item.strip().lower()
        for item in job_type.split(",")
        if item.strip()
    }
    if not requested or "all" in requested:
        return None

    return requested


def normalize_hpc_job_type(
    *candidates: object,
    returnable_job_types: set[str] | None = None,
) -> str:
    returnable = returnable_job_types or get_returnable_hpc_job_types()
    for candidate in candidates:
        job_type = str(candidate or "").strip().lower()
        if job_type and job_type in returnable:
            return job_type
    return ""


def should_return_hpc_job_type(
    job_type: str | None,
    requested_job_types: set[str] | None,
    returnable_job_types: set[str] | None = None,
) -> bool:
    normalized = normalize_hpc_job_type(
        job_type,
        returnable_job_types=returnable_job_types,
    )
    if not normalized:
        return False
    return requested_job_types is None or normalized in requested_job_types


def get_sacct_field(job_data: dict, *names: str) -> str:
    for name in names:
        if name in job_data:
            return job_data.get(name) or ""

    lower_map = {str(key).lower(): value for key, value in job_data.items()}
    for name in names:
        value = lower_map.get(name.lower())
        if value is not None:
            return value

    return ""
