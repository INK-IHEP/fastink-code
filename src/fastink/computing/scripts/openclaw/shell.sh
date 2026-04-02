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

get_free_port() {
    if [ -z "${PORT_CHECK_BIN}" ]; then
        echo "No port check command found." >&2
        exit 1
    fi
    while true; do
        PORT=$(shuf -i 49152-65535 -n 1)
        if ! "${PORT_CHECK_BIN}" -ltn 2>/dev/null | grep -q ":$PORT\\b"; then
            echo "$PORT"
            break
        fi
    done
}

APP_PORT=$(get_free_port)
APP_PATH="`/bin/pwd`"
LOG_FILE="${APP_PATH}/openclaw-launch.log"
OPENCLAW_USER_ROOT=${1:-}
OPENCLAW_DIR=${2:-}
OPENCLAW_IMAGE=${3:-}
OPENCLAW_USER=${4:-${USER:-}}

touch "${LOG_FILE}"
exec >> "${LOG_FILE}" 2>&1

log "starting shell.sh"
log "uid=${UID:-unknown} user=${USER:-unknown} openclaw_user=${OPENCLAW_USER}"
log "selected_port=${APP_PORT}"
log "openclaw_user_root=${OPENCLAW_USER_ROOT:-missing}"
log "openclaw_dir=${OPENCLAW_DIR:-missing}"
log "openclaw_image=${OPENCLAW_IMAGE:-missing}"

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
"${APP_PATH}/run.sh" "${APP_PATH}" "${APP_PORT}" "${OPENCLAW_USER_ROOT}" "${OPENCLAW_DIR}" "${OPENCLAW_IMAGE}" "${OPENCLAW_USER}"
