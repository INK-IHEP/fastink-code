"""Slurm state normalization shared by API and cron paths."""

from typing import Optional

SLURM_TERMINAL_JOB_STATUSES = frozenset(
    {
        "COMPLETED",
        "FAILED",
        "TIMEOUT",
        "OUT_OF_MEMORY",
        "CANCELLED",
    }
)

def normalize_slurm_state(slurm_state: str) -> Optional[str]:
    """Map raw Slurm states to FastINK's stable API/DB status vocabulary."""
    state = (slurm_state or "").strip().upper()
    if not state:
        return None

    base_state = state.split()[0]

    if base_state == "PENDING":
        return "QUEUEING"

    if base_state in {
        "CONFIGURING",
        "COMPLETING",
        "RESIZING",
        "RUNNING",
        "SIGNALING",
        "SUSPENDED",
        "STOPPED",
    }:
        return "RUNNING"

    if base_state == "COMPLETED":
        return "COMPLETED"

    if base_state in {"CANCELLED", "CANCELED", "REVOKED"}:
        return "CANCELLED"

    if base_state in {"OUT_OF_MEMORY", "OUT_OF_ME"}:
        return "OUT_OF_MEMORY"

    if base_state == "TIMEOUT":
        return "TIMEOUT"

    if base_state in {
        "BOOT_FAIL",
        "DEADLINE",
        "FAILED",
        "NODE_FAIL",
        "PREEMPTED",
        "SPECIAL_EXIT",
    }:
        return "FAILED"

    return None


def normalize_slurm_text_field(value: str) -> Optional[str]:
    """Return None for Slurm placeholders that mean no value is available."""
    normalized = (value or "").strip()
    if not normalized or normalized.upper() in {"NONE", "UNKNOWN", "N/A"}:
        return None
    return normalized
