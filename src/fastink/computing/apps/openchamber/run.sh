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
# Idle-based auto-stop (admin resource efficiency): stop the job after
# INK_IDLE_KILL_SEC seconds without detected user interaction. 0/unset =
# disabled (deadline-only, backwards compatible).
IDLE_KILL="${INK_IDLE_KILL_SEC:-0}"
LAST_ACTIVE=$(date +%s)

# Activity probe — four OR'ed signals (see
# docs/openchamber-long-lived-session.md section 4.1):
#   A. opencode writes its SQLite WAL on every real interaction (messages
#      sent, tools run). Fresh WAL/db mtime == the user is actively using
#      this instance.
#   B. tool-output/ streaming files mtime (may not exist in all opencode
#      versions — verified absent locally; harmless if missing).
#   C. process CPU delta (cgroup v2 first, pidstat fallback) — covers
#      silent long-running compute tasks that produce no writes.
#   D. inbound network activity on the app port — the terminal UI streams
#      keystrokes (and mouse clicks when reported) to the server over the
#      WebSocket, so bytes received == the user is typing/clicking even
#      before a completed message (signal A).  A lightweight background
#      monitor stamps $APP_PATH/.ink_heartbeat on any meaningful inbound
#      data so short bursts between watchdog checks are not missed.
# $APP_PATH/.ink_heartbeat is reserved for explicit heartbeat integrations.
# Signal C's CPU-activity helpers below are intentionally an openchamber-local
# copy (kept in this file on purpose: only openchamber gets the idle-recycle
# behaviour; vnc and the other apps keep their own watchdogs untouched).
_get_cgroup_v2_path() {
    local line path
    line=$(/bin/awk 'NR==1 && /^0::/' /proc/self/cgroup 2>/dev/null)
    [[ -n "$line" ]] || return 1
    path="${line#0::}"
    [[ -n "$path" && "$path" != "/" ]] || return 1
    printf '%s\n' "$path"
}

# Tri-state: 0=active, 1=inactive, 2=cgroup unusable (caller falls back).
_has_cpu_activity_cgroup() {
    local cg stat_file t1 t2 delta_us thresh_pct thresh_us
    cg=$(_get_cgroup_v2_path) || return 2
    stat_file="/sys/fs/cgroup${cg}/cpu.stat"
    [[ -r "$stat_file" ]] || return 2
    t1=$(/bin/awk '/^usage_usec/{print $2; exit}' "$stat_file" 2>/dev/null)
    [[ "$t1" =~ ^[0-9]+$ ]] || return 2
    sleep 5
    t2=$(/bin/awk '/^usage_usec/{print $2; exit}' "$stat_file" 2>/dev/null)
    [[ "$t2" =~ ^[0-9]+$ ]] || return 2
    delta_us=$(( t2 - t1 ))
    # Coerce non-positive-integer thresholds to the default 5 so a bad
    # env value can never make the watchdog fire forever-true.
    thresh_pct="${INK_ACTIVE_CPU_MIN_PCT:-5}"
    [[ "$thresh_pct" =~ ^[1-9][0-9]*$ ]] || thresh_pct=5
    thresh_us=$(( 5 * thresh_pct * 10000 ))
    (( delta_us >= thresh_us ))
}

_has_cpu_activity_pidstat() {
    command -v pidstat >/dev/null 2>&1 || return 1
    local user_name thresh
    user_name=$(/usr/bin/id -un)
    thresh="${INK_ACTIVE_CPU_MIN_PCT:-5}"
    [[ "$thresh" =~ ^[1-9][0-9]*$ ]] || thresh=5
    # LC_ALL=C: pidstat localises "Average:"; $(NF-2) = %CPU column
    # across sysstat 10.x (9 cols) and 12.x (10 cols).
    LC_ALL=C pidstat -u -U "$user_name" 5 1 2>/dev/null | /bin/awk -v thresh="$thresh" '
        /^Average:/ && $(NF-2)+0 >= thresh+0 && $NF !~ /pidstat/ {
            found = 1
            exit
        }
        END { exit(!found) }
    '
}

_has_cpu_activity() {
    _has_cpu_activity_cgroup
    local rc=$?
    (( rc != 2 )) && return $rc
    _has_cpu_activity_pidstat
}

# Signal D: sum of bytes received on the app port's established sockets.
# Keystrokes/clicks arrive as WebSocket frames from the browser (relayed
# by the backend nginx), so this counter only grows on real user input —
# idle keepalive ACKs do not advance bytes_received.
_net_rx_bytes() {
    LC_ALL=C ss -Htin "sport = :${APP_PORT}" 2>/dev/null | \
        /bin/awk '/bytes_received:/ {
            for (i = 1; i <= NF; i++)
                if ($i ~ /^bytes_received:/) { gsub(/bytes_received:/, "", $i); s += $i }
        } END { print s + 0 }'
}

# Background monitor: sample received-byte delta over a short window and
# stamp the heartbeat file on any meaningful inbound data.  Runs
# continuously so activity is recorded as a file mtime (persistent) that
# the periodic watchdog can read even if the burst happens between checks.
net_activity_monitor() {
    local t1 t2 delta thresh
    # Same sanity guard as the CPU threshold: 0 (or any non-positive
    # integer) would make `delta >= thresh` always true and the job
    # would never be idle-killed.
    thresh="${INK_ACTIVE_NET_MIN_BYTES:-64}"
    [[ "$thresh" =~ ^[1-9][0-9]*$ ]] || thresh=64
    while :; do
        t1=$(_net_rx_bytes)
        sleep "${INK_NET_POLL_SEC:-5}"
        t2=$(_net_rx_bytes)
        delta=$(( t2 - t1 ))
        # Negative delta = connection churn (old socket closed, counter
        # reset): treat as no activity rather than a false positive.
        if (( delta >= thresh )); then
            touch "${APP_PATH}/.ink_heartbeat" 2>/dev/null
        fi
    done
}

is_active() {
    local now=$(date +%s) newest=0 base f m
    # Signals A/B: newest mtime across opencode db files, tool-output
    # streaming files and the reserved heartbeat file.
    for base in "$HOME/.local/share/opencode" "${OPENCODE_DATA_HOME:-}"; do
        [[ -n "$base" ]] || continue
        for f in "$base/opencode.db-wal" "$base/opencode.db" \
                 "$base"/tool-output/*; do
            [[ -f "$f" ]] || continue
            m=$(stat -c %Y "$f" 2>/dev/null) || continue
            (( m > newest )) && newest=$m
        done
    done
    f="${APP_PATH}/.ink_heartbeat"
    if [[ -f "$f" ]]; then
        m=$(stat -c %Y "$f" 2>/dev/null) || m=0
        (( m > newest )) && newest=$m
    fi
    (( newest > LAST_ACTIVE )) && LAST_ACTIVE=$newest
    # Signal C: process CPU delta — silent compute tasks stay alive.
    if _has_cpu_activity; then
        LAST_ACTIVE=$now
        return 0
    fi
    (( now - LAST_ACTIVE < IDLE_KILL ))
}

# ---- optional per-site environment ----
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

# ---- optional site/app preflight (config-driven) ----
# Sites may point OPENCHAMBER_PREFLIGHT (via jobtype.openchamber.env) at a
# script that validates user storage/quota before the daemon starts.  A
# missing/non-executable path is skipped with a warning so a broken site
# config cannot block every job; a non-zero exit fails the job.
if [ -n "${OPENCHAMBER_PREFLIGHT:-}" ]; then
    if [ -x "${OPENCHAMBER_PREFLIGHT}" ]; then
        /bin/echo "[INK] Running preflight: ${OPENCHAMBER_PREFLIGHT}"
        if ! "${OPENCHAMBER_PREFLIGHT}"; then
            /bin/echo "[INK] ERROR: preflight failed: ${OPENCHAMBER_PREFLIGHT}" >&2
            exit 1
        fi
    else
        /bin/echo "[INK] WARN: OPENCHAMBER_PREFLIGHT not executable, skipping: ${OPENCHAMBER_PREFLIGHT}" >&2
    fi
fi

# ---- write login info ----
write_login_info() {
    /bin/echo "{\"HOST\": \"${APP_RUN_HOST}\", \"PORT\": \"${APP_PORT}\", \"PASSWD\": \"${APP_PASSWD}\", \"JOB_ID\": \"${JOB_ID}\"}" > ${APP_LOGIN_INFO}
}
write_login_info

echo "[INK] OpenChamber init ${INK_INIT_HOURS}h, deadline: $(date -d @${DEADLINE} '+%F %T')"
echo "[INK] port: ${APP_PORT}"

# openchamber-web state (logs, projects, config) defaults to
# ~/.config/openchamber on AFS home — quota-limited and shared across
# jobs. Keep it per-job inside the job directory on workfs. The site
# env hook above may still override it.
export OPENCHAMBER_DATA_DIR="${OPENCHAMBER_DATA_DIR:-${APP_PATH}/openchamber-data}"
mkdir -p "${OPENCHAMBER_DATA_DIR}"

# ---- start OpenChamber Express (daemon), retry on port conflicts ----
# get_free_port's ss check races with other jobs on the same node: the
# chosen port can get taken between the check and the daemon's bind.
# (The daemon's run-dir pid records live under OPENCHAMBER_DATA_DIR —
# per-job — and 1.23.x itself drops dead records, so no stale-record
# cleanup is needed here.) Retry with a fresh port instead of failing
# the job.
MAX_START_ATTEMPTS=3
STARTED=0
for attempt in $(seq 1 ${MAX_START_ATTEMPTS}); do
    OPENCODE_BINARY="${OPENCODE_BINARY}" \
    OPENCODE_SERVER_PASSWORD="${APP_PASSWD}" \
      "${NODE_BIN}" "${OPENCHAMBER_BIN}" serve \
        --lan --port "${APP_PORT}" --ui-password "${APP_PASSWD}" 2>&1 &
    FORK_PID=$!

    # Daemon mode: the fork exits early and the port may be held by
    # ANOTHER process (the race we are retrying), so a bare port check
    # would false-positive. The daemon only writes its pid file after a
    # successful bind — use port-listening AND our pid file as the
    # success signal.
    for i in $(seq 1 20); do
        if ss -ltn 2>/dev/null | grep -q ":${APP_PORT}\b" \
           && [ -f "${OPENCHAMBER_DATA_DIR}/run/openchamber-${APP_PORT}.pid" ]; then
            STARTED=1
            break
        fi
        sleep 1
    done

    if [ "${STARTED}" = "1" ]; then
        break
    fi

    echo "[INK] OpenChamber did not listen on port ${APP_PORT} (attempt ${attempt}/${MAX_START_ATTEMPTS}); trying a new port."
    kill "${FORK_PID}" 2>/dev/null || true
    wait "${FORK_PID}" 2>/dev/null || true
    APP_PORT="$(get_free_port)"
    write_login_info
    echo "[INK] New port: ${APP_PORT}"
done

if [ "${STARTED}" != "1" ]; then
    echo "[INK] ERROR: OpenChamber failed to start after ${MAX_START_ATTEMPTS} attempts; see output above."
    exit 1
fi

echo "[INK] OpenChamber Express forked, pid=${FORK_PID}"

# The fork exits once daemon is running; hold until deadline by polling
wait "${FORK_PID}" 2>/dev/null || true

echo "[INK] Fork exited, monitoring daemon... (deadline=${INK_INIT_HOURS}h, idle-kill=$(( (IDLE_KILL + 59) / 60 ))min)"

# Signal D net monitor: only meaningful when idle-kill is enabled.  It
# stamps .ink_heartbeat (read by is_active) on inbound keystroke/click
# traffic, and must be reaped on every exit path below.  The trap is
# registered BEFORE the monitor starts so there is no window in which a
# dying script would leak the background subshell.
NET_MON_PID=""
_cleanup() {
    [[ -n "$NET_MON_PID" ]] && kill "$NET_MON_PID" 2>/dev/null
}
trap _cleanup EXIT
if (( IDLE_KILL > 0 )); then
    net_activity_monitor &
    NET_MON_PID=$!
    echo "[INK] Net activity monitor started, pid=${NET_MON_PID} (poll=${INK_NET_POLL_SEC:-5}s, min=${INK_ACTIVE_NET_MIN_BYTES:-64}B)"
fi

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
    if (( IDLE_KILL > 0 )) && ! is_active; then
        echo "[INK] No interaction for $(( (IDLE_KILL + 59) / 60 ))min, stopping (idle-kill)."
        "${NODE_BIN}" "${OPENCHAMBER_BIN}" stop 2>/dev/null
        exit 0
    fi
    sleep "${INK_CHECK_INTERVAL:-900}"
done
