# Cron Image

This image is the clean, publishable Redis-backed job runner for FastINK.

## How it works

The container runs `runner.py`, which loads `cron.yaml` (base config) and
optionally `cron.overlay.yml` (site overlay), merges them by job name, and
starts each job as an asyncio task.

Jobs can be defined in two ways:

- **module + function**: the runner imports the module and calls the function
  directly — no wrapper script file needed.
- **script**: the runner runs a `.py` file as a subprocess. Used for
  site-specific jobs with complex logic.

## Built-in jobs

All 7 generic jobs use module+function references:

| Job | Module | Function | Interval | Mode |
|-----|--------|----------|----------|------|
| job_queue_renew | condor_cron | update_completed_jobs | 600s | delay |
| job_submit | condor_cron | submit_job_from_redis | 5s | delay |
| reset_job_time | condor_cron | resert_start_end_time | 1800s | fixed |
| refresh_redis_jobs | condor_cron | refresh_redis_job_status | 5s | delay |
| slurm_job_submit | slurm_cron | submit_slurm_jobs | 5s | delay |
| slurm_update_job_state | slurm_cron | slurm_update_job_state | 5s | delay |
| slurm_update_job_time | slurm_cron | slurm_update_job_time | 300s | fixed |

## Site overlays

Sites can override intervals or add new jobs by mounting a `cron.overlay.yml`
and setting `FASTINK_CRON_CONFIG_OVERLAY`. Example:

```yaml
jobs:
  - name: job_queue_renew
    interval: 300  # override interval
  - name: my_custom_job
    script: my_custom_job.py
    interval: 60
    mode: fixed
```

Site-specific scripts should be mounted under `$FASTINK_CRON_BASE_DIR/overlay/`.

## Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| FASTINK_CRON_BASE_DIR | /opt/fastink-cron | Base directory for scripts |
| FASTINK_CRON_CONFIG | $BASE_DIR/cron.yaml | Base config file |
| FASTINK_CRON_CONFIG_OVERLAY | (empty) | Site overlay config file |
| FASTINK_CRON_LOG_DIR | /var/log/fastink-cron | Log directory |
| INSTALL_EDITABLE | false | Install fastink from INK_CODE_DIR |
| PLUGIN_PIP_PACKAGES | (empty) | Comma-separated pip packages |
| PLUGIN_EDITABLE_DIRS | (empty) | Comma-separated editable dirs |
| PRELOAD_SCRIPTS | (empty) | Comma-separated preload scripts |
| PRELOAD_SCRIPT_DIRS | (empty) | Comma-separated preload dirs |
