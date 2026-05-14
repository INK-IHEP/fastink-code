#!/bin/bash

set -euo pipefail

timestamp() {
    date '+%Y-%m-%d %H:%M:%S'
}

log() {
    echo "[$(timestamp)] [openclaw-shell] $*"
}

PORT_CHECK_BIN="${PORT_CHECK_BIN:-$(command -v ss || command -v netstat || true)}"

if [ -n "${INKPATH:-}" ] && [ -n "${INKLDPATH:-}" ]; then
    export PATH="$INKPATH:$PATH"
    export APPTAINERENV_PATH="$INKPATH"
    export LD_LIBRARY_PATH="$INKLDPATH"
    export APPTAINERENV_LD_LIBRARY_PATH="$INKLDPATH"
fi

APP_PATH="`/bin/pwd`"
LOG_FILE="${APP_PATH}/openclaw-launch.log"
OPENCLAW_USER_ROOT=${1:-}
OPENCLAW_DIR=${2:-}
OPENCLAW_IMAGE=${3:-}
OPENCLAW_USER=${4:-${USER:-}}
OPENCLAW_EXTRA_BINDS_FILE=${5:-}

touch "${LOG_FILE}"
# Keep a dedicated launch log while preserving stdout/stderr for the scheduler
# output files that drive connect_sign detection.
exec > >(tee -a "${LOG_FILE}") 2>&1

log "starting shell.sh"
log "uid=${UID:-unknown} user=${USER:-unknown} openclaw_user=${OPENCLAW_USER}"
log "openclaw_user_root=${OPENCLAW_USER_ROOT:-missing}"
log "openclaw_dir=${OPENCLAW_DIR:-missing}"
log "openclaw_image=${OPENCLAW_IMAGE:-missing}"
log "openclaw_extra_binds_file=${OPENCLAW_EXTRA_BINDS_FILE:-missing}"

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
"${APP_PATH}/run.sh" "${APP_PATH}" "${OPENCLAW_USER_ROOT}" "${OPENCLAW_DIR}" "${OPENCLAW_IMAGE}" "${OPENCLAW_USER}" "${OPENCLAW_EXTRA_BINDS_FILE}"
