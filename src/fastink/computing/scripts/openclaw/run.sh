#!/bin/bash

set -euo pipefail

timestamp() {
    date '+%Y-%m-%d %H:%M:%S'
}

log() {
    echo "[$(timestamp)] [openclaw-run] $*"
}

APP_PATH=${1}
OPENCLAW_USER_ROOT=${2}
OPENCLAW_DIR=${3}
OPENCLAW_IMAGE=${4}
OPENCLAW_USER=${5:-${USER:-}}
APP_LOGIN_INFO="app_login.info"
LOG_FILE="${APP_PATH}/openclaw-launch.log"
APP_RUN_FQDN="`/bin/hostname -f 2>/dev/null || /bin/hostname`"
APP_RUN_HOST="`printf '%s' \"${APP_RUN_FQDN}\" | /bin/awk -F '.' '{print $1}'`"
OPENCLAW_CONFIG_FILE="${OPENCLAW_DIR}/openclaw.json"
APPTAINER_BIN="${APPTAINER_BIN:-apptainer}"
PORT_CHECK_BIN="${PORT_CHECK_BIN:-$(command -v ss || command -v netstat || true)}"
APP_TOKEN="$(python3 - <<'PY'
import secrets
print(secrets.token_hex(32))
PY
)"

is_port_available() {
    local port=$1
    if [ -z "${port}" ]; then
        return 1
    fi
    if ! printf '%s' "${port}" | grep -Eq '^[0-9]+$'; then
        return 1
    fi
    if [ -n "${PORT_CHECK_BIN}" ] && "${PORT_CHECK_BIN}" -ltn 2>/dev/null | grep -q ":${port}\\b"; then
        return 1
    fi
    return 0
}

get_free_port() {
    if [ -z "${PORT_CHECK_BIN}" ]; then
        echo "No port check command found." >&2
        exit 1
    fi
    while true; do
        port=$(shuf -i 49152-65535 -n 1)
        if is_port_available "${port}"; then
            echo "${port}"
            break
        fi
    done
}

get_existing_config_port() {
    OPENCLAW_CONFIG_FILE="${OPENCLAW_CONFIG_FILE}" python3 - <<'PY'
import json
import os
from pathlib import Path

config_path = Path(os.environ["OPENCLAW_CONFIG_FILE"])
try:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    port = config.get("gateway", {}).get("port")
    if isinstance(port, int):
        print(port)
    elif isinstance(port, str) and port.isdigit():
        print(port)
except Exception:
    pass
PY
}

select_app_port() {
    local user_uid existing_port
    user_uid=$(id -u "${OPENCLAW_USER}" 2>/dev/null || id -u)
    if [ "${user_uid}" -ge 10000 ] && [ "${user_uid}" -le 65535 ] && is_port_available "${user_uid}"; then
        log "using uid port ${user_uid}" >&2
        echo "${user_uid}"
        return 0
    fi

    existing_port=$(get_existing_config_port)
    if is_port_available "${existing_port}"; then
        log "using existing config port ${existing_port}" >&2
        echo "${existing_port}"
        return 0
    fi

    log "falling back to random free port" >&2
    get_free_port
}

if [ ! -d "${OPENCLAW_USER_ROOT}" ]; then
    log "openclaw user root does not exist: ${OPENCLAW_USER_ROOT}"
    exit 1
fi

if [ ! -d "${OPENCLAW_DIR}" ]; then
    log "openclaw workspace does not exist: ${OPENCLAW_DIR}"
    exit 1
fi

if [ ! -f "${OPENCLAW_CONFIG_FILE}" ]; then
    log "openclaw config file does not exist: ${OPENCLAW_CONFIG_FILE}"
    exit 1
fi

if [ ! -f "${OPENCLAW_IMAGE}" ]; then
    log "openclaw image does not exist: ${OPENCLAW_IMAGE}"
    exit 1
fi

if ! command -v "${APPTAINER_BIN}" >/dev/null 2>&1; then
    log "apptainer command not found in PATH"
    exit 1
fi

APP_PORT=$(select_app_port)
APP_BASE_PATH="/openclaw/${APP_RUN_HOST}/${APP_PORT}/${OPENCLAW_USER}/"

log "starting run.sh"
log "app_port=${APP_PORT}"
log "openclaw_user=${OPENCLAW_USER}"
log "openclaw_dir=${OPENCLAW_DIR}"
log "openclaw_image=${OPENCLAW_IMAGE}"
log "hostname_fqdn=${APP_RUN_FQDN}"
log "base_path=${APP_BASE_PATH}"
log "auth_mode=token"

OPENCLAW_CONFIG_FILE="${OPENCLAW_CONFIG_FILE}" \
APP_PORT="${APP_PORT}" \
APP_BASE_PATH="${APP_BASE_PATH}" \
APP_RUN_FQDN="${APP_RUN_FQDN}" \
OPENCLAW_USER="${OPENCLAW_USER}" \
APP_TOKEN="${APP_TOKEN}" \
python3 - <<'PY'
import json
import os
from pathlib import Path

config_path = Path(os.environ["OPENCLAW_CONFIG_FILE"])
config = json.loads(config_path.read_text(encoding="utf-8"))

gateway = config.setdefault("gateway", {})
gateway["port"] = int(os.environ["APP_PORT"])
control_ui = gateway.setdefault("controlUi", {})
control_ui["basePath"] = os.environ["APP_BASE_PATH"]
existing_origins = control_ui.get("allowedOrigins", [])
merged_origins = []
for origin in existing_origins + [f"https://{os.environ['APP_RUN_FQDN']}"]:
    if origin and origin not in merged_origins:
        merged_origins.append(origin)
control_ui["allowedOrigins"] = merged_origins
control_ui["dangerouslyDisableDeviceAuth"] = True

auth = gateway.setdefault("auth", {})
auth["mode"] = "token"
auth["token"] = os.environ["APP_TOKEN"]

config_path.write_text(
    json.dumps(config, ensure_ascii=False, indent=2) + "\n",
    encoding="utf-8",
)
PY

log "updated gateway config"

/bin/echo "{\"HOST\": \"${APP_RUN_HOST}\", \"HOST_FQDN\": \"${APP_RUN_FQDN}\", \"PORT\": \"${APP_PORT}\", \"USERNAME\": \"${OPENCLAW_USER}\", \"BASE_PATH\": \"${APP_BASE_PATH}\", \"TOKEN\": \"${APP_TOKEN}\"}" > "${APP_LOGIN_INFO}"
log "wrote app_login.info"

(
    cd "${OPENCLAW_DIR}"
    log "launching apptainer from $(pwd)"
    "${APPTAINER_BIN}" run \
        --containall \
        --home "${OPENCLAW_USER_ROOT}" \
        --bind "${OPENCLAW_DIR}:/workspace:rw" \
        --bind /cvmfs:/cvmfs:ro \
        --bind "${OPENCLAW_USER_ROOT}:${OPENCLAW_USER_ROOT}:rw" \
        "${OPENCLAW_IMAGE}" openclaw gateway
) 2>&1 &
APP_PID=$!
log "spawned background pid=${APP_PID}"

READY=0
for _ in $(seq 1 60); do
    if ! kill -0 "${APP_PID}" 2>/dev/null; then
        log "apptainer process exited before port became ready"
        wait "${APP_PID}"
        exit $?
    fi

    if [ -n "${PORT_CHECK_BIN}" ] && "${PORT_CHECK_BIN}" -ltn 2>/dev/null | grep -q ":${APP_PORT}\\b"; then
        log "OpenClaw gateway listening on ${APP_RUN_FQDN}:${APP_PORT}"
        READY=1
        break
    fi

    sleep 1
done

if [ "${READY}" -eq 0 ]; then
    log "openclaw gateway did not start listening within 60 seconds"
fi

log "waiting for background process ${APP_PID}"
wait "${APP_PID}"
