# Cron Image

This image is the clean, publishable Redis-backed job runner for FastINK.

Built-in jobs are intentionally limited to generic tasks:

- `job_queue_renew.py`
- `job_submit.py`
- `reset_job_time.py`

Site-specific jobs should be layered at runtime by mounting an alternate
`FASTINK_CRON_BASE_DIR` or `FASTINK_CRON_CONFIG`, rather than baking
environment-specific scripts, credentials, or endpoints into the image.

If those jobs need extra Python dependencies, install them at runtime with
`PLUGIN_PIP_PACKAGES` or mount editable source trees with `PLUGIN_EDITABLE_DIRS`.

Useful environment variables:

- `FASTINK_CRON_BASE_DIR`
- `FASTINK_CRON_CONFIG`
- `FASTINK_CRON_LOG_DIR`
- `INSTALL_EDITABLE`
- `PLUGIN_PIP_PACKAGES`
- `PLUGIN_EDITABLE_DIRS`
