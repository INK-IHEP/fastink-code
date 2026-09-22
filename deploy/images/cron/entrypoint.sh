#!/bin/bash
exec python3.12 "${FASTINK_CRON_RUNNER:-/opt/fastink-cron/runner.py}"
