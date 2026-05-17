#!/bin/bash

set -euo pipefail

timestamp() {
    date '+%Y-%m-%d %H:%M:%S'
}

log() {
    echo "[$(timestamp)] [openclaw-shell] $*"
}

PORT_CHECK_BIN="${PORT_CHECK_BIN:-$(command -v ss || command -v netstat || true)}"
APPTAINER_BIN="${APPTAINER_BIN:-$(command -v apptainer || true)}"

if [ -n "${INKPATH:-}" ] && [ -n "${INKLDPATH:-}" ]; then
    export PATH="$INKPATH:$PATH"
    export APPTAINERENV_PATH="$INKPATH"
    export LD_LIBRARY_PATH="$INKLDPATH"
    export APPTAINERENV_LD_LIBRARY_PATH="$INKLDPATH"
fi

APP_PATH="`/bin/pwd`"
APP_RUN_FQDN="`/bin/hostname -f 2>/dev/null || /bin/hostname`"
APP_RUN_HOST="`printf '%s' \"${APP_RUN_FQDN}\" | /bin/awk -F '.' '{print $1}'`"
LOG_FILE="${APP_PATH}/openclaw-launch.log"
OPENCLAW_USER_ROOT=${1:-}
OPENCLAW_DIR=${2:-}
OPENCLAW_IMAGE=${3:-}
OPENCLAW_USER=${4:-${USER:-}}
OPENCLAW_EXTRA_BINDS_FILE=${5:-}
RUN_SCRIPT="${APP_PATH}/run.sh"

touch "${LOG_FILE}"
# Keep a dedicated launch log while preserving stdout/stderr for the scheduler
# output files that drive connect_sign detection.
exec > >(tee -a "${LOG_FILE}") 2>&1

log "starting shell.sh"
log "hostname_fqdn=${APP_RUN_FQDN}"
log "hostname_short=${APP_RUN_HOST}"
log "app_path=${APP_PATH}"
log "shell_pid=$$"
log "uid=${UID:-unknown} user=${USER:-unknown} openclaw_user=${OPENCLAW_USER}"
log "port_check_bin=${PORT_CHECK_BIN:-missing}"
log "apptainer_bin=${APPTAINER_BIN:-missing}"
log "openclaw_user_root=${OPENCLAW_USER_ROOT:-missing}"
log "openclaw_dir=${OPENCLAW_DIR:-missing}"
log "openclaw_image=${OPENCLAW_IMAGE:-missing}"
log "openclaw_extra_binds_file=${OPENCLAW_EXTRA_BINDS_FILE:-missing}"
log "run_script=${RUN_SCRIPT}"

if [ -n "${OPENCLAW_IMAGE}" ] && [ -f "${OPENCLAW_IMAGE}" ]; then
    log "openclaw_image_exists=true"
else
    log "openclaw_image_exists=false"
fi

if [ -f "${RUN_SCRIPT}" ]; then
    log "run_script_exists=true"
else
    log "run_script_exists=false"
fi

if [ -z "${OPENCLAW_USER_ROOT}" ] || [ -z "${OPENCLAW_DIR}" ] || [ -z "${OPENCLAW_IMAGE}" ]; then
    log "missing required runtime arguments"
    exit 1
fi

if [ -f "${APP_PATH}/krb5cc_${UID}" ]; then
    export KRB5CCNAME="${APP_PATH}/krb5cc_${UID}"
    log "using kerberos cache ${KRB5CCNAME}"
else
    log "no local kerberos cache found at ${APP_PATH}/krb5cc_${UID}"
fi

if command -v /usr/bin/aklog >/dev/null 2>&1 && klist -s 2>/dev/null; then
    log "running aklog"
    /usr/bin/aklog
else
    log "skip aklog: aklog or valid klist not available"
fi

log "handoff to run.sh"
"${RUN_SCRIPT}" "${APP_PATH}" "${OPENCLAW_USER_ROOT}" "${OPENCLAW_DIR}" "${OPENCLAW_IMAGE}" "${OPENCLAW_USER}" "${OPENCLAW_EXTRA_BINDS_FILE}"
