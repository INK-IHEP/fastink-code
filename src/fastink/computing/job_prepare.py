from __future__ import annotations

from typing import Any, Mapping

from pydantic import BaseModel, Field

from fastink.common.config import get_config
from fastink.computing.apps import registry as computing_registry
from fastink.computing.cluster.cluster import HTC_JOB, SLURM_JOB
from fastink.computing.job_defaults import (
    SLURM_JOB_DEFAULTS,
    seconds_to_slurm_time,
)


ALLOWED_OVERRIDES = ("cpus", "memory_mb", "walltime_seconds", "gpu_count", "os")


class PrepareJobRequest(BaseModel):
    job_type: str = Field(..., min_length=1)
    cluster_id: str | None = None
    overrides: dict[str, Any] | None = None


class JobPreparationError(ValueError):
    pass


def _configured_clusters() -> list[str]:
    cluster_list = get_config("computing", "cluster_list", fallback=["htcondor"])
    if isinstance(cluster_list, str):
        cluster_list = [item.strip() for item in cluster_list.split(",") if item.strip()]
    clusters = list(cluster_list or [])
    if not clusters:
        raise JobPreparationError("No clusters are configured")
    return clusters


def _configured_htc_defaults(job_type: str, app) -> dict[str, object]:
    app_defaults = dict((app.request_defaults or {}).get("htc", {})) if app else {}
    config_defaults = get_config("jobtype", job_type, fallback={}) or {}
    config_htc_defaults = dict(config_defaults.get("htc", {}))
    return {**app_defaults, **config_htc_defaults}


def _configured_slurm_defaults(job_type: str) -> dict[str, object]:
    defaults = dict(SLURM_JOB_DEFAULTS[job_type])
    config_defaults = get_config("jobtype", job_type, fallback={}) or {}
    config_slurm_defaults = dict(config_defaults.get("slurm", {}))
    return {**defaults, **config_slurm_defaults}


def _supported_clusters(job_type: str, app, configured: list[str]) -> list[str]:
    htc_defaults = _configured_htc_defaults(job_type, app)
    supported = []
    for cluster_id in configured:
        if cluster_id == "slurm" and job_type in SLURM_JOB_DEFAULTS:
            supported.append(cluster_id)
        elif (
            cluster_id == "htcondor"
            and htc_defaults
            and "RequestMemory" in htc_defaults
        ):
            supported.append(cluster_id)
    return supported


def list_job_type_capabilities() -> dict[str, object]:
    clusters = _configured_clusters()
    job_types = []
    for app in computing_registry.all_apps():
        supported = _supported_clusters(app.name, app, clusters)
        if not supported:
            continue
        job_types.append(
            {
                "name": app.name,
                "connect_type": app.connect_type or app.name,
                "clusters": supported,
            }
        )
    return {
        "default_cluster": clusters[0],
        "clusters": clusters,
        "job_types": sorted(job_types, key=lambda item: item["name"]),
    }


def _validate_overrides(
    overrides: Mapping[str, Any], cluster_id: str
) -> dict[str, Any]:
    unknown = sorted(set(overrides) - set(ALLOWED_OVERRIDES))
    if unknown:
        raise JobPreparationError(
            f"Unknown override key(s): {', '.join(unknown)}. "
            f"Allowed keys: {', '.join(ALLOWED_OVERRIDES)}"
        )

    supported_knobs = (
        set(ALLOWED_OVERRIDES) - {"os"}
        if cluster_id == "slurm"
        else {"os"}
    )
    unsupported = sorted(set(overrides) - supported_knobs)
    if unsupported:
        if cluster_id == "htcondor":
            raise JobPreparationError(
                "htcondor resources are site-managed per job type "
                "(config-wins contract); use --payload for full control"
            )
        raise JobPreparationError(
            f"Override(s) {', '.join(unsupported)} have no unambiguous mapping "
            f"for cluster '{cluster_id}'"
        )

    validated = {}
    for key, value in overrides.items():
        if key == "os":
            if not isinstance(value, str) or not value.strip():
                raise JobPreparationError(
                    "Override 'os' must be a non-empty string"
                )
            validated[key] = value
            continue
        if type(value) is not int:
            raise JobPreparationError(f"Override '{key}' must be an integer")
        minimum = 0 if key == "gpu_count" else 1
        if value < minimum:
            qualifier = "non-negative" if key == "gpu_count" else "positive"
            raise JobPreparationError(f"Override '{key}' must be {qualifier}")
        validated[key] = value
    return validated


def _prepare_slurm(job_type: str, overrides: dict[str, int]) -> dict[str, object]:
    defaults = _configured_slurm_defaults(job_type)
    defaults.pop("script_file", None)
    payload = {**defaults, "job_type": job_type, "cluster_id": "slurm"}
    gpu_count = overrides.get("gpu_count", payload.get("gpu_num", 0))
    if gpu_count and not payload.get("gpu_name"):
        raise JobPreparationError(
            f"gpu_count requires a site-provided gpu_name for job type '{job_type}'; "
            "use the raw payload path for GPU jobs"
        )
    payload.update(
        {
            "cpu": overrides.get("cpus", payload["cpu"]),
            "mem": overrides.get("memory_mb", payload["mem"]),
            "gpu_num": gpu_count,
        }
    )
    if "walltime_seconds" in overrides:
        payload["time"] = seconds_to_slurm_time(overrides["walltime_seconds"])
    return SLURM_JOB.model_validate(payload).model_dump()


def _prepare_htc(
    job_type: str, app, overrides: Mapping[str, Any]
) -> dict[str, object]:
    defaults = _configured_htc_defaults(job_type, app)
    request_os = overrides.get("os", defaults.get("os"))
    if not isinstance(request_os, str) or not request_os.strip():
        raise JobPreparationError(
            f"job type '{job_type}' on htcondor requires an OS: pass os explicitly "
            f"or set jobtype.{job_type}.htc.os in config"
        )
    payload = {
        "job_type": job_type,
        "cluster_id": "htcondor",
        "cpu": int(defaults.get("RequestCpus", 1)),
        "mem": int(defaults.get("RequestMemory", 6000)),
        "os": request_os,
        "wn": str(defaults.get("workernode", "")),
        "arch": str(defaults.get("arch", "")),
    }
    return HTC_JOB.model_validate(payload).model_dump(exclude={"schedd", "cm"})


def prepare_job_request(
    job_type: str,
    cluster_id: str | None = None,
    overrides: Mapping[str, Any] | None = None,
) -> dict[str, object]:
    configured = _configured_clusters()
    app = computing_registry.try_get(job_type)
    if app is None:
        raise JobPreparationError(
            f"Unknown job type '{job_type}'. Available names: "
            f"{', '.join(computing_registry.names())}"
        )

    if cluster_id == "":
        raise JobPreparationError("cluster_id must not be empty")

    supported = _supported_clusters(job_type, app, configured)
    if cluster_id is None:
        default_cluster = configured[0]
        if default_cluster in supported:
            selected_cluster = default_cluster
        elif len(supported) == 1:
            selected_cluster = supported[0]
        else:
            available = ", ".join(supported) or "none"
            raise JobPreparationError(
                f"Job type '{job_type}' has no usable default cluster; "
                f"supported clusters: {available}"
            )
    else:
        selected_cluster = cluster_id

    if selected_cluster not in configured:
        raise JobPreparationError(
            f"Unknown cluster '{selected_cluster}'. Available clusters: "
            f"{', '.join(configured)}"
        )

    if selected_cluster not in supported:
        available = ", ".join(supported) or "none"
        raise JobPreparationError(
            f"Job type '{job_type}' is not supported on cluster '{selected_cluster}'; "
            f"supported clusters: {available}"
        )

    raw_overrides = {} if overrides is None else overrides
    if not isinstance(raw_overrides, Mapping):
        raise JobPreparationError("overrides must be an object")
    validated = _validate_overrides(raw_overrides, selected_cluster)
    if selected_cluster == "slurm":
        return _prepare_slurm(job_type, validated)
    return _prepare_htc(job_type, app, validated)
