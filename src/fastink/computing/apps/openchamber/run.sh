#!/bin/bash
#
# OpenChamber interactive web job — session-based proxy, no --base-path.
#
# Architecture:
#   browser → OpenResty (8446) → backend nginx → OpenChamber Express
#
# OpenChamber Express (daemon) manages its own opencode internally.
# The fork PID exits immediately; watchdog polls the port via
# "cli.js status" to detect liveness.
#
# $1 = HTCondor ClusterId (injected by prepare_submit in __init__.py)

# ---- binaries ----
export OPENCODE_BINARY="${OPENCODE_BINARY:-/cvmfs/common.ihep.ac.cn/software/opencode/opencode-latest-linux-x86_64/bin/opencode}"
export NODE_BIN="${NODE_BIN:-/cvmfs/common.ihep.ac.cn/software/node.js/latest/x64/bin/node}"
export OPENCHAMBER_BIN="${OPENCHAMBER_BIN:-/cvmfs/common.ihep.ac.cn/software/openchamber-web/openchamber-web-latest/bin/cli.js}"

JOB_ID="${1:-0}"
APP_PORT="${APP_PORT:-$(get_free_port)}"
APP_PASSWD=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
APP_LOGIN_INFO="app_login.info"
APP_RUN_HOST="$(/bin/hostname | /bin/awk -F '.' '{print $1}')"
DEADLINE=$(( $(date +%s) + INK_INIT_HOURS * 3600 ))

# ---- optional per-site environment ----
OPENCODE_ENV_FILE="${OPENCODE_ENV_FILE:-$(realpath -m -- "${APP_PATH}/../../envs/opencode/env.sh")}"
if [ -f "${OPENCODE_ENV_FILE}" ]; then
    echo "[INK] Sourcing OpenCode env: ${OPENCODE_ENV_FILE}"
    # shellcheck disable=SC1090
    source "${OPENCODE_ENV_FILE}"
else
    echo "[INK] No OpenCode env file (${OPENCODE_ENV_FILE}), skipping."
fi

# ---- write login info ----
/bin/echo "{\"HOST\": \"${APP_RUN_HOST}\", \"PORT\": \"${APP_PORT}\", \"PASSWD\": \"${APP_PASSWD}\", \"JOB_ID\": \"${JOB_ID}\"}" > ${APP_LOGIN_INFO}

echo "[INK] OpenChamber init ${INK_INIT_HOURS}h, deadline: $(date -d @${DEADLINE} '+%F %T')"
echo "[INK] port: ${APP_PORT}"

# ---- start OpenChamber Express (daemon) ----
OPENCODE_BINARY="${OPENCODE_BINARY}" \
OPENCODE_SERVER_PASSWORD="${APP_PASSWD}" \
  "${NODE_BIN}" "${OPENCHAMBER_BIN}" serve \
    --lan --port "${APP_PORT}" --ui-password "${APP_PASSWD}" 2>&1 &
FORK_PID=$!

# Wait for the daemon to start listening
for i in $(seq 1 20); do
    if ss -tlnp 2>/dev/null | grep -q ":${APP_PORT}\b"; then
        break
    fi
    sleep 1
done

echo "[INK] OpenChamber Express forked, pid=${FORK_PID}"

# The fork exits once daemon is running; hold until deadline by polling
wait "${FORK_PID}" 2>/dev/null || true

echo "[INK] Fork exited, monitoring daemon..."
while true; do
    now=$(date +%s)
    if (( now >= DEADLINE )); then
        echo "[INK] Deadline reached, stopping."
        "${NODE_BIN}" "${OPENCHAMBER_BIN}" stop 2>/dev/null
        exit 0
    fi
    if ! ss -tlnp 2>/dev/null | grep -q ":${APP_PORT}\b"; then
        echo "[INK] Daemon port ${APP_PORT} gone, exiting."
        exit 1
    fi
    sleep "${INK_CHECK_INTERVAL:-900}"
done
