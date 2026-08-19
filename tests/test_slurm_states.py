from fastink.computing.tools.common.slurm_states import (
    SLURM_TERMINAL_JOB_STATUSES,
    normalize_slurm_state,
    normalize_slurm_text_field,
)


def test_normalize_slurm_states_seen_on_preview():
    assert normalize_slurm_state("PENDING") == "QUEUEING"
    assert normalize_slurm_state("RUNNING") == "RUNNING"
    assert normalize_slurm_state("NODE_FAIL") == "FAILED"
    assert normalize_slurm_state("CANCELLED by 0") == "CANCELLED"


def test_normalize_slurm_terminal_states():
    assert normalize_slurm_state("BOOT_FAIL") == "FAILED"
    assert normalize_slurm_state("DEADLINE") == "FAILED"
    assert normalize_slurm_state("PREEMPTED") == "FAILED"
    assert normalize_slurm_state("OUT_OF_MEMORY") == "OUT_OF_MEMORY"
    assert normalize_slurm_state("TIMEOUT") == "TIMEOUT"

    for status in ("FAILED", "OUT_OF_MEMORY", "TIMEOUT", "CANCELLED"):
        assert status in SLURM_TERMINAL_JOB_STATUSES


def test_normalize_slurm_running_transitional_states():
    assert normalize_slurm_state("CONFIGURING") == "RUNNING"
    assert normalize_slurm_state("COMPLETING") == "RUNNING"
    assert normalize_slurm_state("SUSPENDED") == "RUNNING"


def test_normalize_slurm_empty_text_placeholders():
    assert normalize_slurm_text_field("") is None
    assert normalize_slurm_text_field("None") is None
    assert normalize_slurm_text_field("Unknown") is None
    assert normalize_slurm_text_field("N/A") is None
    assert normalize_slurm_text_field("2026-08-19T10:23:45") == "2026-08-19T10:23:45"
