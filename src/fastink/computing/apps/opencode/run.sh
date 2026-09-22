#!/bin/bash
#
# OpenCode interactive web job — session-based proxy, no --base-path.
#
# Architecture:
#   browser → OpenResty (8447) → backend nginx → opencode serve
#
# The OpenResty authenticates from the browser session cookie, resolves the
# user's running job via the FastINK API, injects Basic Auth, and proxies
# transparently to the worker at /. No per-node nginx, no URL encoding, no
# --base-path flag, so the generic (non-basepath) CVMFS opencode binary is
# used instead of a manually built variant.
#
# $1 = HTCondor ClusterId (injected by prepare_submit in __init__.py)

# ---- app-specific env ----
export OPENCODE_BIN="${OPENCODE_BIN:-/cvmfs/common.ihep.ac.cn/software/opencode/opencode-latest-linux-x86_64/bin/opencode}"

JOB_ID="${1:-0}"
APP_PORT="${APP_PORT:-$(get_free_port)}"
APP_PASSWD=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
APP_LOGIN_INFO="app_login.info"
APP_RUN_HOST="$(/bin/hostname | /bin/awk -F '.' '{print $1}')"
DEADLINE=$(( $(date +%s) + INK_INIT_HOURS * 3600 ))

# ---- optional per-site environment ----
# Shared per-app env material lives under <base>/.ink/envs (same place as
# krb5cc and enode's sshd config). Resolve relative to the job dir ($APP_PATH)
# because the absolute .ink path differs per site (~ may be a symlink/off-home).
OPENCODE_ENV_FILE="${OPENCODE_ENV_FILE:-$(realpath -m -- "${APP_PATH}/../../envs/opencode/env.sh")}"
if [ -f "${OPENCODE_ENV_FILE}" ]; then
    echo "[INK] Sourcing OpenCode env: ${OPENCODE_ENV_FILE}"
    # shellcheck disable=SC1090
    source "${OPENCODE_ENV_FILE}"
else
    echo "[INK] No OpenCode env file (${OPENCODE_ENV_FILE}), skipping."
fi

# ---- fail fast on broken symlinks opencode must mkdir through ----
# opencode creates ~/.cache/opencode/bin and ~/.local/share/opencode at
# startup; a broken symlink on those paths (e.g. ~/.cache pointing at a
# removed /tmp or group volume) kills it with a confusing ENOENT.  We
# must not rewrite the user's symlink — report and exit so the user
# fixes their own environment.
for _p in "$HOME/.cache" "$HOME/.config" "$HOME/.local" \
          "$HOME/.local/share" "$HOME/.local/share/opencode"; do
    ink_assert_not_broken_link "$_p"
done
unset _p

# ---- write login info (PASSWD returned to frontend via connect() API) ----
write_login_info() {
    /bin/echo "{\"HOST\": \"${APP_RUN_HOST}\", \"PORT\": \"${APP_PORT}\", \"PASSWD\": \"${APP_PASSWD}\", \"JOB_ID\": \"${JOB_ID}\"}" > ${APP_LOGIN_INFO}
}
write_login_info

echo "[INK] OpenCode init ${INK_INIT_HOURS}h, deadline: $(date -d @${DEADLINE} '+%F %T')"
echo "[INK] Port: ${APP_PORT}"

# ---- watchdog ----
watchdog() {
    local app_pid=$1
    while kill -0 "$app_pid" 2>/dev/null; do
        now=$(date +%s)
        if (( now >= DEADLINE )); then
            echo "[INK] Deadline reached, stopping opencode."
            kill -TERM "$app_pid" 2>/dev/null
            break
        fi
        sleep "$INK_CHECK_INTERVAL"
    done
}

# ---- start opencode (headless, no --base-path), retry on port conflicts ----
# get_free_port's ss check races with other jobs on the same node. If
# the chosen port gets taken before opencode binds, the process exits
# immediately — retry with a fresh port instead of failing the job.
MAX_START_ATTEMPTS=3
STARTED=0
for attempt in $(seq 1 ${MAX_START_ATTEMPTS}); do
    OPENCODE_SERVER_PASSWORD="${APP_PASSWD}" "${OPENCODE_BIN}" serve \
        --hostname 0.0.0.0 \
        --port "${APP_PORT}" 2>&1 &
    APP_PID=$!

    # A port conflict kills opencode within seconds (ServeError, exit 1)
    # but there is a short window before it exits, and during that window
    # the port is held by the OTHER process — a bare connect check would
    # false-positive. Success = OUR process owns the listener, verified
    # via ss -ltnp pid attribution.
    for i in $(seq 1 10); do
        if ! kill -0 "${APP_PID}" 2>/dev/null; then
            break
        fi
        if ss -ltnp 2>/dev/null | grep ":${APP_PORT}\b" | grep -q "pid=${APP_PID},"; then
            STARTED=1
            break
        fi
        sleep 1
    done

    if [ "${STARTED}" = "1" ]; then
        break
    fi

    echo "[INK] opencode did not listen on port ${APP_PORT} (attempt ${attempt}/${MAX_START_ATTEMPTS}); trying a new port."
    wait "${APP_PID}" 2>/dev/null || true
    APP_PORT="$(get_free_port)"
    write_login_info
    echo "[INK] New port: ${APP_PORT}"
done

if [ "${STARTED}" != "1" ]; then
    echo "[INK] ERROR: opencode failed to start after ${MAX_START_ATTEMPTS} attempts; see output above."
    exit 1
fi

trap 'kill -TERM ${APP_PID} 2>/dev/null' INT TERM

watchdog "${APP_PID}" &
WATCHDOG_PID=$!

wait "${APP_PID}"
EXIT_CODE=$?
kill "${WATCHDOG_PID}" 2>/dev/null
exit ${EXIT_CODE}
