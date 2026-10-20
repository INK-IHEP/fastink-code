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

# ---- storage layout (D1/D2/D3/D5/D8, docs/openchamber-data-tiering.md) ----
# FS probes (stat/readlink/mkdir) can hang indefinitely on a sick
# lustre/AFS/scratchfs — wrap them so a hung FS fails fast instead of
# hanging job startup forever.
INK_FS_CMD_TIMEOUT="${INK_FS_CMD_TIMEOUT:-10}"
fs_cmd() { timeout "${INK_FS_CMD_TIMEOUT}" "$@"; }

# openchamber-web state (settings, projects, logs). Default: per-job inside
# the job directory (upstream behavior, unchanged). Sites that set
# jobtype.openchamber.env.OPENCHAMBER_DATA_MODE=per-user get one fixed
# per-user directory next to the Jobs tree so sessions/projects/settings
# survive across jobs. A value already set by the site env hook above
# (OPENCODE_ENV_FILE) always wins — the derivation only fills the unset case.
OPENCHAMBER_DATA_MODE="${OPENCHAMBER_DATA_MODE:-per-job}"
[ -n "${OPENCHAMBER_DATA_DIR:-}" ] && INK_DATA_DIR_PINNED=1
if [ "${OPENCHAMBER_DATA_MODE}" = "per-user" ] && [ -z "${OPENCHAMBER_DATA_DIR:-}" ]; then
    # Job dir layout: <ink_root>/.ink/Jobs/<cluster>/<type>-<ts>; three
    # levels up is the per-user .ink root that persists across jobs.
    _derived="$(fs_cmd realpath -m "${APP_PATH}/../../../openchamber-data" 2>/dev/null || true)"
    if [ -n "$_derived" ]; then
        OPENCHAMBER_DATA_DIR="$_derived"
        INK_DATA_DIR_DERIVED=1   # derived by us -> eligible for one-time seeding below
    else
        echo "[INK] WARN: cannot resolve the per-user data dir (filesystem not responding?); falling back to per-job." >&2
    fi
    unset _derived
fi
export OPENCHAMBER_DATA_DIR="${OPENCHAMBER_DATA_DIR:-${APP_PATH}/openchamber-data}"
if ! fs_cmd mkdir -p "${OPENCHAMBER_DATA_DIR}"; then
    echo "[INK] ERROR: cannot create data dir ${OPENCHAMBER_DATA_DIR} (filesystem not responding?)" >&2
    exit 1
fi
# Tighten permissions only on dirs this script owns. A dir pinned by the site
# env hook may deliberately be shared — do not chmod it.
if [ "${INK_DATA_DIR_PINNED:-0}" != "1" ]; then
    fs_cmd chmod 700 "${OPENCHAMBER_DATA_DIR}" 2>/dev/null || true
fi

# ---- single-instance lock (D4) ----
# Taken before seeding and held for the daemon's lifetime: the shared-state
# critical section (seed + maintenance + daemon) must be single-instance.
# Submit-side dedup normally prevents overlap; this flock is the filesystem
# backstop. In per-job mode every job locks its own fresh directory — never
# contended, so this costs nothing upstream. A missing flock tool falls back
# to dedup alone rather than turning every job into a startup failure.
if command -v flock >/dev/null 2>&1; then
    exec 9>"${OPENCHAMBER_DATA_DIR}/.instance.lock"
    if [ "${INK_DATA_DIR_DERIVED:-0}" = "1" ] || [ "${INK_DATA_DIR_PINNED:-0}" = "1" ]; then
        # Shared per-user dir: this lock is load-bearing — lose it and two
        # jobs could rewrite the same state, so we fail closed. Wait briefly
        # first so a short overlap can clear; a real running job still fails.
        if ! flock -w 30 9; then
            echo "[INK] ERROR: another openchamber job holds the instance lock" >&2
            echo "[INK]        (${OPENCHAMBER_DATA_DIR}/.instance.lock) or this filesystem lacks flock." >&2
            echo "[INK]        Refusing to start a second instance; use the running job or wait for it to exit." >&2
            exit 1
        fi
    elif ! flock -n 9; then
        echo "[INK] WARN: could not take the per-job instance lock; continuing on submit-side dedup." >&2
    fi
else
    echo "[INK] WARN: flock not available; single-instance enforcement falls back to the submit-side dedup." >&2
fi

# One-time relocation of the opencode db off legacy roots was dropped: the db
# stays on the large scratch tier (workfs2's per-user quota is too small for
# multi-GB databases). See docs/openchamber-data-tiering.md D2.

# Optional scratch split (D3): bulky regenerable dirs live on the site's
# large/no-backup filesystem. INK_SCRATCH_ROOT is a per-user absolute path
# rendered from service.scratch_root by the submit side. Unset → everything
# stays under the data dir (upstream default). Unreachable scratch → warn
# and keep going with the unsplit layout.
if [ -n "${INK_SCRATCH_ROOT:-}" ]; then
    if fs_cmd mkdir -p "${INK_SCRATCH_ROOT}/openchamber-chats" "${INK_SCRATCH_ROOT}/opencode-tool-output" 2>/dev/null; then
        export OPENCHAMBER_CHATS_DIR="${OPENCHAMBER_CHATS_DIR:-${INK_SCRATCH_ROOT}/openchamber-chats}"
        # opencode has no env var for tool-output; split via symlink (idempotent).
        _db_dir="$(fs_cmd readlink -f "$HOME/.local/share/opencode" 2>/dev/null || true)"
        if [ -n "$_db_dir" ] && [ -d "$_db_dir" ]; then
            if [ -d "$_db_dir/tool-output" ] && [ ! -L "$_db_dir/tool-output" ]; then
                # Move the existing payload across first; only when it is fully
                # gone may we symlink (ln onto a surviving real dir would nest
                # the link INSIDE it instead of replacing it).
                if timeout 300 cp -a "$_db_dir/tool-output/." "${INK_SCRATCH_ROOT}/opencode-tool-output/" 2>/dev/null; then
                    timeout 120 rm -rf "$_db_dir/tool-output"
                else
                    echo "[INK] WARN: tool-output copy to scratch failed; keeping it in the db dir." >&2
                fi
            fi
            [ -e "$_db_dir/tool-output" ] || [ -L "$_db_dir/tool-output" ] \
                || timeout 30 ln -s "${INK_SCRATCH_ROOT}/opencode-tool-output" "$_db_dir/tool-output" 2>/dev/null || true
        fi
        unset _db_dir
    else
        echo "[INK] WARN: scratch root ${INK_SCRATCH_ROOT} not reachable; keeping chats/tool-output in the data dir." >&2
    fi
fi

# Let preflight quota-check the dirs this job will actually use (D5).
# Append to any site-provided list so our dirs are always checked.
export OPENCHAMBER_PREFLIGHT_EXTRA_PATHS="${OPENCHAMBER_PREFLIGHT_EXTRA_PATHS:+${OPENCHAMBER_PREFLIGHT_EXTRA_PATHS}:}${OPENCHAMBER_DATA_DIR}${OPENCHAMBER_CHATS_DIR:+:${OPENCHAMBER_CHATS_DIR}}"

# ---- optional site/app preflight (config-driven) ----
# Sites may point OPENCHAMBER_PREFLIGHT (via jobtype.openchamber.env) at a
# script that validates user storage/quota before the daemon starts.  A
# missing/non-executable path is skipped with a warning so a broken site
# config cannot block every job; a non-zero exit fails the job.
if [ -n "${OPENCHAMBER_PREFLIGHT:-}" ]; then
    if [ -x "${OPENCHAMBER_PREFLIGHT}" ]; then
        /bin/echo "[INK] Running preflight: ${OPENCHAMBER_PREFLIGHT}"
        timeout "${INK_PREFLIGHT_BUDGET_SEC:-180}" "${OPENCHAMBER_PREFLIGHT}"
        _pf_rc=$?
        if [ "$_pf_rc" -ne 0 ]; then
            /bin/echo "[INK] ERROR: preflight failed (rc=${_pf_rc}; 124 = time budget): ${OPENCHAMBER_PREFLIGHT}" >&2
            exit 1
        fi
    else
        /bin/echo "[INK] WARN: OPENCHAMBER_PREFLIGHT not executable, skipping: ${OPENCHAMBER_PREFLIGHT}" >&2
    fi
fi

# ---- (single-instance lock moved above, before seeding) ----

# ---- one-time seeding of the fixed data dir (D1.1) ----
# COPY (never move, never delete the source) the newest legacy per-job
# openchamber-data into the fresh per-user dir, so settings / projects /
# chats carry over. Hard guarantees:
#   * source job dirs are never modified — the worst a failed seed can do is
#     leave the fixed dir empty (history stays in the old job dirs);
#   * skipped when the user pinned their own OPENCHAMBER_DATA_DIR via the env
#     hook (INK_DATA_DIR_DERIVED unset) — their data already persists;
#   * skipped once seeded (.seeded marker) or when the dir already has a
#     settings.json (never clobber existing state).
# Best-effort: failure warns and startup continues with the empty dir.
if [ "${INK_DATA_DIR_DERIVED:-0}" = "1" ] && [ ! -e "${OPENCHAMBER_DATA_DIR}/.seeded" ] \
   && [ ! -e "${OPENCHAMBER_DATA_DIR}/settings.json" ]; then
    # Both job-dir layouts: the nested <cluster>/<jobtype>-<ts> and the older
    # flat <jobtype>-<ts> (see computing/apps/_helpers.py).
    candidates="$(fs_cmd ls -dt "${APP_PATH}"/../../*/openchamber-data "${APP_PATH}"/../../*/*/openchamber-data 2>/dev/null)"
    _ls_rc=$?
    latest="$(printf '%s\n' "$candidates" | head -1)"
    if [ "$_ls_rc" -eq 124 ]; then
        # The scan itself timed out (sick filesystem): we cannot tell "no
        # source" from "not looked yet", so do NOT mark — retry next boot.
        echo "[INK] WARN: seeding scan timed out; leaving seeding pending for the next job start." >&2
    elif [ -n "$latest" ] \
       && [ "$(fs_cmd readlink -f "$latest")" != "$(fs_cmd readlink -f "${OPENCHAMBER_DATA_DIR}")" ]; then
        echo "[INK] Seeding fixed data dir from $latest (copy-only; source untouched)"
        size_kb="$(timeout 60 du -sk "$latest" 2>/dev/null | awk '{print $1}')"
        budget=$(( ${size_kb:-0} / 10240 + 60 ))   # pessimistic 10 MB/s + margin
        staging="${OPENCHAMBER_DATA_DIR}.seed-tmp-$$"
        if fs_cmd mkdir -p "$staging" && timeout "$budget" cp -a "$latest/." "$staging/"; then
            timeout 120 rm -rf "$staging/run"    # drop stale pid records from OUR copy only
            if timeout "$budget" cp -a "$staging/." "${OPENCHAMBER_DATA_DIR}/"; then
                touch "${OPENCHAMBER_DATA_DIR}/.seeded"
                echo "[INK] Seeding done."
            else
                echo "[INK] WARN: seeding copy-in failed; starting with an empty data dir (history remains in old job dirs)." >&2
            fi
        else
            echo "[INK] WARN: seeding copy failed; starting with an empty data dir (history remains in old job dirs)." >&2
        fi
        timeout 120 rm -rf "$staging" 2>/dev/null || true   # our own staging only
    else
        # Confirmably no candidate source (first-ever job): mark so we do not
        # re-scan every start.
        touch "${OPENCHAMBER_DATA_DIR}/.seeded"
    fi
fi

# ---- optional site maintenance hook (D6) ----
# Runs with the instance lock held, before the daemon starts: the previous
# openchamber instance is gone and the new one is not up, so the db is
# exclusive on the openchamber side (a login-node "opencode" CLI writing the
# same db is outside this lock — see the non-goals). Sites point
# OPENCHAMBER_MAINTENANCE at a housekeeping script (vacuum / retention
# / archive); empty upstream default skips it. Maintenance failure never
# blocks startup. The outer budget bounds the worst-case startup delay (the
# script is resumable: it keeps per-session transactions and re-runs next boot).
if [ -n "${OPENCHAMBER_MAINTENANCE:-}" ]; then
    if [ -x "${OPENCHAMBER_MAINTENANCE}" ]; then
        /bin/echo "[INK] Running maintenance: ${OPENCHAMBER_MAINTENANCE}"
        timeout "${INK_MAINTENANCE_BUDGET_SEC:-600}" "${OPENCHAMBER_MAINTENANCE}" \
            || /bin/echo "[INK] WARN: maintenance rc=$? (0=ok,124=budget); continuing startup." >&2
    else
        /bin/echo "[INK] WARN: OPENCHAMBER_MAINTENANCE not executable, skipping: ${OPENCHAMBER_MAINTENANCE}" >&2
    fi
fi

# ---- write login info ----
write_login_info() {
    /bin/echo "{\"HOST\": \"${APP_RUN_HOST}\", \"PORT\": \"${APP_PORT}\", \"PASSWD\": \"${APP_PASSWD}\", \"JOB_ID\": \"${JOB_ID}\"}" > ${APP_LOGIN_INFO}
}
write_login_info

echo "[INK] OpenChamber init ${INK_INIT_HOURS}h, deadline: $(date -d @${DEADLINE} '+%F %T')"
echo "[INK] port: ${APP_PORT}"
echo "[INK] data dir: ${OPENCHAMBER_DATA_DIR} (mode ${OPENCHAMBER_DATA_MODE})"

# ---- start OpenChamber Express (daemon), retry on port conflicts ----
# get_free_port's ss check races with other jobs on the same node: the
# chosen port can get taken between the check and the daemon's bind.
# (The daemon's run-dir pid records live under OPENCHAMBER_DATA_DIR; in
# per-user mode they are shared across jobs — the single-instance lock
# above plus the daemon's own dead-record dropping keep that safe.)
# Retry with a fresh port instead of failing the job.
MAX_START_ATTEMPTS=3
STARTED=0
for attempt in $(seq 1 ${MAX_START_ATTEMPTS}); do
    OPENCODE_BINARY="${OPENCODE_BINARY}" \
    OPENCODE_SERVER_PASSWORD="${APP_PASSWD}" \
      "${NODE_BIN}" "${OPENCHAMBER_BIN}" serve \
        --lan --port "${APP_PORT}" --ui-password="${APP_PASSWD}" 2>&1 &
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
    # Keep the Kerberos TGT / AFS tokens fresh (self-throttled to once per
    # INK_KRB_RENEW_HOURS, default 6h): openchamber runs up to init_hours (168h
    # at IHEP), far past the 48h ticket lifetime, and reads/writes /afs --
    # without renewal those accesses fail once the ticket lapses.
    #
    # Deliberately NOT wired to ink_past_ceiling: openchamber has no max_hours,
    # so its DEADLINE below already is a hard cap, and the intended guarantee is
    # "run the full init_hours even if the ticket cannot be renewed" -- the same
    # choice made for asic/asicbm.  A renewal failure therefore only warns; it
    # never shortens the session.
    declare -F ink_renew_krb >/dev/null 2>&1 && ink_renew_krb
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
