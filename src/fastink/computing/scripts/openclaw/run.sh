#!/bin/bash

set -euo pipefail

timestamp() {
    date '+%Y-%m-%d %H:%M:%S'
}

log() {
    echo "[$(timestamp)] [openclaw-run] $*"
}

log_multiline() {
    local prefix=$1
    local content=${2:-}
    if [ -z "${content}" ]; then
        log "${prefix}: <empty>"
        return 0
    fi
    while IFS= read -r line; do
        log "${prefix}: ${line}"
    done <<< "${content}"
}

APP_PATH=${1}
OPENCLAW_USER_ROOT=${2}
OPENCLAW_DIR=${3}
OPENCLAW_IMAGE=${4}
OPENCLAW_USER=${5:-${USER:-}}
OPENCLAW_EXTRA_BINDS_FILE=${6:-}
APP_LOGIN_INFO="app_login.info"
LOG_FILE="${APP_PATH}/openclaw-launch.log"
APP_RUN_FQDN="`/bin/hostname -f 2>/dev/null || /bin/hostname`"
APP_RUN_HOST="`printf '%s' \"${APP_RUN_FQDN}\" | /bin/awk -F '.' '{print $1}'`"
OPENCLAW_CONFIG_FILE="${OPENCLAW_DIR}/openclaw.json"
APPTAINER_BIN="${APPTAINER_BIN:-apptainer}"
PORT_CHECK_BIN="${PORT_CHECK_BIN:-$(command -v ss || command -v netstat || true)}"
LOCAL_MODEL_BASE_URL="https://aiapi.ihep.ac.cn/apiv2"
MODEL_CHECK_INTERVAL="${MODEL_CHECK_INTERVAL:-60}"
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

inspect_config_models() {
    OPENCLAW_CONFIG_FILE="${OPENCLAW_CONFIG_FILE}" LOCAL_MODEL_BASE_URL="${LOCAL_MODEL_BASE_URL}" python3 - <<'PY'
import json
import os
from pathlib import Path

config_path = Path(os.environ["OPENCLAW_CONFIG_FILE"])
local_base_url = os.environ["LOCAL_MODEL_BASE_URL"].rstrip("/")

try:
    payload = json.loads(config_path.read_text(encoding="utf-8"))
except Exception:
    print("error=failed to parse openclaw.json")
    print("RESULT=false")
    raise SystemExit(0)

providers = payload.get("models", {}).get("providers")
if not isinstance(providers, dict):
    print("error=models.providers is not a dict")
    print("RESULT=false")
    raise SystemExit(0)

has_model = False
for provider in providers.values():
    if not isinstance(provider, dict):
        print("error=provider entry is not a dict")
        print("RESULT=false")
        raise SystemExit(0)
    provider_base_url = str(provider.get("baseUrl", "")).rstrip("/")
    models = provider.get("models")
    if not isinstance(models, list):
        print(f"provider_base_url={provider_base_url} models_type=invalid")
        print("RESULT=false")
        raise SystemExit(0)
    for model in models:
        if not isinstance(model, dict):
            print(f"provider_base_url={provider_base_url} model_type=invalid")
            print("RESULT=false")
            raise SystemExit(0)
        has_model = True
        model_id = str(model.get("id", ""))
        is_local = provider_base_url == local_base_url and model_id.startswith("hepai/")
        print(f"provider_base_url={provider_base_url} model_id={model_id} local_only={str(is_local).lower()}")
        if not is_local:
            print("RESULT=false")
            raise SystemExit(0)

print("RESULT=true" if has_model else "RESULT=false")
PY
}

all_config_models_are_local_only() {
    local report
    report="$(inspect_config_models)"
    printf '%s\n' "${report}" | awk -F= '/^RESULT=/{print $2}' | tail -n 1
}

inspect_agent_models() {
    OPENCLAW_DIR="${OPENCLAW_DIR}" LOCAL_MODEL_BASE_URL="${LOCAL_MODEL_BASE_URL}" python3 - <<'PY'
import json
import os
from pathlib import Path

openclaw_dir = Path(os.environ["OPENCLAW_DIR"])
local_base_url = os.environ["LOCAL_MODEL_BASE_URL"].rstrip("/")
agents_dir = openclaw_dir / "agents"
model_files = sorted(agents_dir.glob("*/agent/models.json"))

if not model_files:
    print("error=no agent models.json found")
    print("RESULT=false")
    raise SystemExit(0)

has_model = False
for model_file in model_files:
    try:
        payload = json.loads(model_file.read_text(encoding="utf-8"))
    except Exception:
        print(f"file={model_file} error=failed to parse")
        print("RESULT=false")
        raise SystemExit(0)

    providers = payload.get("providers")
    if not isinstance(providers, dict):
        print(f"file={model_file} error=providers is not a dict")
        print("RESULT=false")
        raise SystemExit(0)

    for provider in providers.values():
        if not isinstance(provider, dict):
            print(f"file={model_file} error=provider entry is not a dict")
            print("RESULT=false")
            raise SystemExit(0)
        provider_base_url = str(provider.get("baseUrl", "")).rstrip("/")
        models = provider.get("models")
        if not isinstance(models, list):
            print(f"file={model_file} provider_base_url={provider_base_url} models_type=invalid")
            print("RESULT=false")
            raise SystemExit(0)
        for model in models:
            if not isinstance(model, dict):
                print(f"file={model_file} provider_base_url={provider_base_url} model_type=invalid")
                print("RESULT=false")
                raise SystemExit(0)
            has_model = True
            model_id = str(model.get("id", ""))
            is_local = provider_base_url == local_base_url and model_id.startswith("hepai/")
            print(f"file={model_file} provider_base_url={provider_base_url} model_id={model_id} local_only={str(is_local).lower()}")
            if not is_local:
                print("RESULT=false")
                raise SystemExit(0)

print("RESULT=true" if has_model else "RESULT=false")
PY
}

resolve_extra_readonly_binds() {
    if [ -z "${OPENCLAW_EXTRA_BINDS_FILE}" ] || [ ! -f "${OPENCLAW_EXTRA_BINDS_FILE}" ]; then
        return 0
    fi

    OPENCLAW_EXTRA_BINDS_FILE="${OPENCLAW_EXTRA_BINDS_FILE}" python3 - <<'PY'
import json
import os
import sys
from pathlib import Path

metadata_path = Path(os.environ["OPENCLAW_EXTRA_BINDS_FILE"])
try:
    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
except Exception as exc:
    print(f"error=failed to parse bind metadata: {exc}", file=sys.stderr)
    raise SystemExit(0)

binds = payload.get("readonly_binds", [])
if not isinstance(binds, list):
    print("error=readonly_binds is not a list", file=sys.stderr)
    raise SystemExit(0)

for bind_entry in binds:
    if isinstance(bind_entry, str) and bind_entry:
        print(bind_entry)
PY
}

run_openclaw_command() {
    "${APPTAINER_BIN}" run \
        --containall \
        --home "${OPENCLAW_USER_ROOT}" \
        --bind "${OPENCLAW_DIR}:/workspace:rw" \
        --bind /cvmfs:/cvmfs:ro \
        --bind "${OPENCLAW_USER_ROOT}:${OPENCLAW_USER_ROOT}:rw" \
        "${EXTRA_BINDS[@]}" \
        "${OPENCLAW_IMAGE}" openclaw "$@"
}

extract_pending_device_count() {
    sed -n 's/^Pending (\([0-9][0-9]*\)).*/\1/p' | tail -n 1
}

monitor_openclaw_runtime() {
    local app_pid=$1
    local enforce_local_only=$2
    local agent_report agent_local_only devices_output devices_rc pending_count approve_output approve_rc
    while kill -0 "${app_pid}" 2>/dev/null; do
        sleep "${MODEL_CHECK_INTERVAL}"
        if ! kill -0 "${app_pid}" 2>/dev/null; then
            break
        fi

        log "monitor tick: checking agent models"
        agent_report="$(inspect_agent_models)"
        log_multiline "agent-models" "${agent_report}"
        agent_local_only="$(printf '%s\n' "${agent_report}" | awk -F= '/^RESULT=/{print $2}' | tail -n 1)"

        if [ "${enforce_local_only}" = "true" ] && [ "${agent_local_only}" != "true" ]; then
            log "detected non-local model during runtime; stopping openclaw gateway"
            kill "${app_pid}" 2>/dev/null || true
            sleep 2
            kill -9 "${app_pid}" 2>/dev/null || true
            exit 0
        fi

        log "monitor tick: checking openclaw devices list"
        if devices_output="$(run_openclaw_command devices list 2>&1)"; then
            devices_rc=0
        else
            devices_rc=$?
        fi
        log "devices list exit_code=${devices_rc}"
        log_multiline "devices-list" "${devices_output}"

        pending_count="$(printf '%s\n' "${devices_output}" | extract_pending_device_count)"
        if [ -n "${pending_count}" ] && [ "${pending_count}" -gt 0 ]; then
            log "pending device requests detected: ${pending_count}; running approve"
            if approve_output="$(run_openclaw_command devices approve 2>&1)"; then
                approve_rc=0
            else
                approve_rc=$?
            fi
            log "devices approve exit_code=${approve_rc}"
            log_multiline "devices-approve" "${approve_output}"
        else
            log "no pending device requests detected"
        fi
    done
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

if [ -n "${OPENCLAW_EXTRA_BINDS_FILE}" ] && [ ! -f "${OPENCLAW_EXTRA_BINDS_FILE}" ]; then
    log "openclaw extra binds metadata file does not exist: ${OPENCLAW_EXTRA_BINDS_FILE}"
fi

if ! command -v "${APPTAINER_BIN}" >/dev/null 2>&1; then
    log "apptainer command not found in PATH"
    exit 1
fi

APP_PORT=$(select_app_port)
APP_BASE_PATH="/openclaw/${APP_RUN_HOST}/${APP_PORT}/${OPENCLAW_USER}/"
LOCAL_ONLY_MODELS="false"
CONFIG_MODEL_REPORT=""
EXTRA_BINDS=()

CONFIG_MODEL_REPORT="$(inspect_config_models)"
log_multiline "config-models" "${CONFIG_MODEL_REPORT}"
if [ "$(printf '%s\n' "${CONFIG_MODEL_REPORT}" | awk -F= '/^RESULT=/{print $2}' | tail -n 1)" = "true" ]; then
    LOCAL_ONLY_MODELS="true"
    while IFS= read -r bind_entry; do
        [ -n "${bind_entry}" ] || continue
        EXTRA_BINDS+=(--bind "${bind_entry}")
    done < <(resolve_extra_readonly_binds)
fi

log "starting run.sh"
log "app_port=${APP_PORT}"
log "openclaw_user=${OPENCLAW_USER}"
log "openclaw_dir=${OPENCLAW_DIR}"
log "openclaw_image=${OPENCLAW_IMAGE}"
log "openclaw_extra_binds_file=${OPENCLAW_EXTRA_BINDS_FILE:-missing}"
log "hostname_fqdn=${APP_RUN_FQDN}"
log "base_path=${APP_BASE_PATH}"
log "auth_mode=token"
log "local_only_models=${LOCAL_ONLY_MODELS}"
if [ "${#EXTRA_BINDS[@]}" -gt 0 ]; then
    log "extra_readonly_binds=${EXTRA_BINDS[*]}"
fi

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
    run_openclaw_command gateway
) 2>&1 &
APP_PID=$!
log "spawned background pid=${APP_PID}"

MONITOR_PID=""
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
else
    monitor_openclaw_runtime "${APP_PID}" "${LOCAL_ONLY_MODELS}" &
    MONITOR_PID=$!
    log "started runtime monitor pid=${MONITOR_PID} enforce_local_only=${LOCAL_ONLY_MODELS}"
fi

log "waiting for background process ${APP_PID}"
APP_STATUS=0
wait "${APP_PID}" || APP_STATUS=$?

if [ -n "${MONITOR_PID}" ]; then
    kill "${MONITOR_PID}" 2>/dev/null || true
    wait "${MONITOR_PID}" 2>/dev/null || true
fi

exit "${APP_STATUS}"
