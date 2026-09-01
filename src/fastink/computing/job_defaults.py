def seconds_to_slurm_time(total_seconds: int) -> str:
    if total_seconds < 0:
        raise ValueError("Time limit seconds must be >= 0.")

    days, remainder = divmod(total_seconds, 24 * 3600)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    if days > 0:
        return f"{days}-{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def normalize_slurm_time_limit(
    raw_value: object, fallback: str = "24:00:00"
) -> str:
    value = str(raw_value).strip()
    if not value:
        return fallback
    if value.isdigit():
        return seconds_to_slurm_time(int(value))
    return value


_SLURM_BASE = {
    "cpu": 1,
    "mem": 6000,
    "time": "24:00:00",
    "partition": "",
    "account": "",
    "qos": "",
}

SLURM_JOB_DEFAULTS = {
    name: {**_SLURM_BASE, "script_file": script}
    for name, script in {
        "enode": "start-sshd.sh",
        "jupyter": "start-jupyterlab-token.sh",
        "rootbrowse": "start-rootbrowse.sh",
        "vscode": "start-vscode.sh",
        "vnc": "start-vnc.sh",
    }.items()
}
