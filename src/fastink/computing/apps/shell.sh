#!/bin/bash
# ---------------------------------------------------------------------------
# FastINK shared job launcher.
#
# One shell.sh for every job type. Executed as the ``executable`` in each
# HTCondor submit file; it performs cross-app setup (INKPATH, kerberos,
# aklog, port helper, watchdog defaults) and then hands off to the per-app
# ``run.sh`` shipped alongside this file (uploaded to the job dir by
# ``generate_condor_submit``).
#
# Extra positional args, if any, are forwarded verbatim to run.sh.  Apps
# that need job-type-specific env vars should export them inside their own
# run.sh -- this launcher is intentionally job-type-agnostic.
# ---------------------------------------------------------------------------

set -u

# -- generic PATH/library shim propagated by the site image --
if [ -n "${INKPATH:-}" ] && [ -n "${INKLDPATH:-}" ]; then
    export PATH="$INKPATH:$PATH"
    export APPTAINERENV_PATH="$INKPATH"
    export LD_LIBRARY_PATH="$INKLDPATH"
    export APPTAINERENV_LD_LIBRARY_PATH="$INKLDPATH"
fi

# -- shared helpers exposed to run.sh --
INK_PORT_RANGE_LOW="${INK_PORT_RANGE_LOW:-49152}"
INK_PORT_RANGE_HIGH="${INK_PORT_RANGE_HIGH:-65535}"

get_free_port() {
    while true; do
        PORT=$(shuf -i "${INK_PORT_RANGE_LOW}-${INK_PORT_RANGE_HIGH}" -n 1)
        if ! /usr/sbin/ss -ltn | grep -q ":${PORT}\b"; then
            echo "$PORT"
            break
        fi
    done
}
export -f get_free_port
export INK_PORT_RANGE_LOW INK_PORT_RANGE_HIGH

# ---------------------------------------------------------------------------
# Broken-symlink guard for XDG-style apps (opencode / openchamber).
#
# opencode creates ~/.cache/opencode/bin and ~/.local/share/opencode at
# startup (recursive mkdir).  A *missing* directory is fine (mkdir creates
# it), but a *broken symlink* anywhere on those paths makes mkdir fail
# with ENOENT and the app dies with a confusing startup error.  Users hit
# this when ~/.cache or ~/.local points at a node-local /tmp or a moved
# group volume that no longer exists.  We must not rewrite the user's own
# symlink, so fail fast with an actionable message instead.
# ---------------------------------------------------------------------------
ink_assert_not_broken_link() {
    local p="$1" target
    # timeout-wrap the probes: on a sick filesystem (lustre/AFS hang) an
    # unwrapped test would hang job startup forever instead of failing fast.
    if timeout "${INK_FS_CMD_TIMEOUT:-10}" test -L "$p" \
       && ! timeout "${INK_FS_CMD_TIMEOUT:-10}" test -e "$p"; then
        target=$(timeout "${INK_FS_CMD_TIMEOUT:-10}" readlink "$p" 2>/dev/null || echo '?')
        echo "[INK] ERROR: $p is a broken symlink (-> $target)." >&2
        echo "[INK]        opencode cannot create its directories through it." >&2
        echo "[INK]        Fix it yourself (rm '$p' or recreate the target '$target'), then resubmit." >&2
        exit 1
    fi
}
export -f ink_assert_not_broken_link

# ---------------------------------------------------------------------------
# Shared VNC OTP listener (FS-based RPC).
#
# Historically fastink-server would ssh(root) into the compute node and
# ``sudo -iu <user>`` a site OTP script every time the user clicked
# "connect".  We now use the shared FS instead: fastink-server drops a
# request file into $APP_PATH/otp/req_<uuid>; the loop below mints a
# fresh OTP with ``vncpasswd -o`` (as the user, already inside its VNC
# session) and writes $APP_PATH/otp/resp_<uuid>.  See
# fastink.computing.apps._helpers.generate_userotp for the server side.
#
# Apps that need this (vnc / asic / asicbm / ink_special) call
# ``otp_start_listener`` once their vncserver is up; the rest of the
# jobtypes ignore these helpers entirely.
# ---------------------------------------------------------------------------

# Mint one OTP for the currently running VNC session.
#   arg $1  optional X display (":N"); if empty, uses ${INK_VNC_DISPLAY:-}
#           then falls back to whatever ``vncserver -list`` reports first.
# Prints the OTP on stdout.  Non-zero rc on any failure.
otp_mint_userotp() {
    local disp="${1:-${INK_VNC_DISPLAY:-}}" out otp
    local vnc="${VNC_CMD:-/opt/TurboVNC/bin/vncserver}"
    local vncpasswd="${VNC_PASSWD_CMD:-${vnc%/vncserver}/vncpasswd}"
    if [[ -z "$disp" ]]; then
        disp="$("$vnc" -list 2>/dev/null | /bin/awk '/^:/ {print $1; exit}')"
    fi
    [[ -n "$disp" ]] || { echo "[otp] no active VNC display" >&2; return 1; }
    if ! out="$("$vncpasswd" -o -display "$disp" 2>&1)"; then
        echo "[otp] vncpasswd rc!=0: $out" >&2
        return 1
    fi
    # TurboVNC prints e.g. "Full-access one-time password: XXXXXXXX";
    # older versions may print just the OTP on the last non-blank line.
    otp="$(echo "$out" | /bin/awk -F': *' '/[Ff]ull-access one-time password/{print $2; exit}')"
    [[ -n "$otp" ]] || otp="$(echo "$out" | /bin/awk 'NF{last=$0} END{print last}' | /bin/awk '{print $NF}')"
    [[ -n "$otp" ]] || { echo "[otp] could not parse OTP from: $out" >&2; return 1; }
    echo "$otp"
}

# One pass over pending req_* files. Called from the watcher loop.
_otp_drain_once() {
    local otp_dir="$1" req uuid resp errf otp err_out
    for req in "$otp_dir"/req_*; do
        [[ -e "$req" ]] || continue                # empty glob guard
        uuid="${req##*/req_}"
        resp="$otp_dir/resp_$uuid"
        errf="$otp_dir/resp_$uuid.err"
        if err_out="$(otp_mint_userotp 2>&1 1>/tmp/.otp_stdout.$$)"; then
            otp="$(cat /tmp/.otp_stdout.$$ 2>/dev/null)"
            /bin/rm -f /tmp/.otp_stdout.$$
            printf '{"otp":"%s","ts":%d}\n' "$otp" "$(date +%s)" > "$resp.tmp"
            /bin/mv -f "$resp.tmp" "$resp"
        else
            /bin/rm -f /tmp/.otp_stdout.$$
            printf 'otp mint failed: %s\n' "$err_out" > "$errf.tmp"
            /bin/mv -f "$errf.tmp" "$errf"
        fi
        /bin/rm -f "$req"
    done
}

# Age out request/response files server never picked up (server crashed
# between write+read; safe fallback so the job dir never fills up).
_otp_gc_loop() {
    local otp_dir="$1"
    while true; do
        /usr/bin/find "$otp_dir" -maxdepth 1 -type f \
            \( -name 'req_*' -o -name 'resp_*' -o -name 'resp_*.err' \) \
            -mmin +1 -delete 2>/dev/null || true
        sleep 60
    done
}

# Main watcher: inotifywait for < 50 ms latency; fall back to 1 s polling
# on hosts without inotify-tools.  Foreground; the entrypoint backgrounds it.
_otp_watcher_loop() {
    local otp_dir="$1"
    if command -v inotifywait >/dev/null 2>&1; then
        while true; do
            _otp_drain_once "$otp_dir"
            inotifywait -qq -e create,moved_to "$otp_dir" 2>/dev/null || sleep 1
        done
    else
        while true; do
            _otp_drain_once "$otp_dir"
            sleep 1
        done
    fi
}

# Public entrypoint.  Call after your vncserver is up (INK_VNC_DISPLAY
# populated where applicable).  Backgrounds watcher + gc, publishes a
# ``.ready`` marker so fastink-server can tell "listener not started
# yet" apart from "listener down".
#
# Sets OTP_WATCHER_PID and OTP_GC_PID so the caller's cleanup trap can
# terminate them cleanly.
otp_start_listener() {
    local otp_dir="${APP_PATH:-$(pwd)}/otp"
    /bin/mkdir -p "$otp_dir"
    /bin/chmod 700 "$otp_dir"
    # Wipe any leftovers if this job dir is being reused.  Include
    # .ready so that a stale marker from an earlier run does not lie
    # to the server during the brief window before the new watcher
    # actually starts.
    /bin/rm -f "$otp_dir"/req_* "$otp_dir"/resp_* "$otp_dir/.ready" 2>/dev/null || true

    _otp_watcher_loop "$otp_dir" &
    OTP_WATCHER_PID=$!
    _otp_gc_loop "$otp_dir" &
    OTP_GC_PID=$!

    /bin/touch "$otp_dir/.ready"
    echo "[INK] OTP listener started, otp_dir=$otp_dir watcher_pid=$OTP_WATCHER_PID gc_pid=$OTP_GC_PID"
}

# Idempotent shutdown; call from cleanup / trap.
otp_stop_listener() {
    [[ -n "${OTP_WATCHER_PID:-}" ]] && kill "$OTP_WATCHER_PID" 2>/dev/null || true
    [[ -n "${OTP_GC_PID:-}" ]]      && kill "$OTP_GC_PID"      2>/dev/null || true
}

export -f otp_mint_userotp _otp_drain_once _otp_gc_loop _otp_watcher_loop \
          otp_start_listener otp_stop_listener

# ===========================================================================
# Shared interactive-session libraries.
#
# Two app families share nearly all of their job script:
#
#   VNC family          vnc, asic, asicbm, ink_special  -> ink_run_vnc_session
#   code-server family  vscode, compile, npu            -> ink_run_codeserver_session
#
# A family member's run.sh sets only what is genuinely app-specific (extra
# env, binary paths) and then calls the shared runner, so each session
# implementation exists exactly once.  Previously every app carried its own
# full copy -- vnc/asic/asicbm were 276 lines each and differed only in
# comments, which is how an is_active() definition went missing from two of
# the three copies and silently killed every ASIC job at its deadline.
#
# All functions are export -f'd so they survive the exec into run.sh, the
# same mechanism get_free_port / otp_start_listener above already use.
# ===========================================================================

# -- activity signals -------------------------------------------------------

# Choose a system python for the X-idle probe.  Must NOT be a user conda or
# venv python: user environments differ and would make the reading
# inconsistent.  Absolute system paths first, PATH lookup only as a last
# resort.
ink_pick_system_python() {
    local c
    for c in /usr/bin/python3 /usr/libexec/platform-python /bin/python3 /usr/bin/python; do
        [[ -x "$c" ]] && { echo "$c"; return 0; }
    done
    command -v python3 2>/dev/null || command -v python 2>/dev/null
}

# Read the X server's input-idle time in milliseconds via the XScreenSaver
# extension (libXss).  Reflects real keyboard/mouse input only.  Prints an
# integer on success, -1 on any failure.  Self-contained: inline python, no
# third-party packages, no separate .py file to ship.
# Requires INK_VNC_DISPLAY from the caller.  INK_PYTHON is honoured when the
# caller has already picked one, and resolved here otherwise, so the function
# has no hidden ordering dependency on the session runner.
ink_read_x_idle_ms() {
    local py="${INK_PYTHON:-$(ink_pick_system_python)}"
    [[ -n "$py" ]] || { echo -1; return 1; }
    "$py" - <<'PY' 2>/dev/null
import ctypes, sys
class XSSInfo(ctypes.Structure):
    _fields_ = [("window", ctypes.c_ulong), ("state", ctypes.c_int),
                ("kind", ctypes.c_int), ("til_or_since", ctypes.c_ulong),
                ("idle", ctypes.c_ulong), ("event_mask", ctypes.c_ulong)]
try:
    x11 = ctypes.CDLL("libX11.so.6"); xss = ctypes.CDLL("libXss.so.1")
except OSError:
    print(-1); sys.exit(2)
x11.XOpenDisplay.restype = ctypes.c_void_p
dpy = x11.XOpenDisplay(None)            # reads the DISPLAY env var
if not dpy:
    print(-1); sys.exit(3)
try:
    xss.XScreenSaverAllocInfo.restype = ctypes.c_void_p
    info = xss.XScreenSaverAllocInfo()
    root = x11.XDefaultRootWindow(ctypes.c_void_p(dpy))
    if not xss.XScreenSaverQueryInfo(ctypes.c_void_p(dpy), ctypes.c_ulong(root), ctypes.c_void_p(info)):
        print(-1); sys.exit(4)
    print(int(ctypes.cast(info, ctypes.POINTER(XSSInfo)).contents.idle))
finally:
    x11.XCloseDisplay(ctypes.c_void_p(dpy))
PY
}

# Return this process's cgroup v2 path ("/foo/bar"), or rc=1 when it cannot
# be determined or we sit in the root cgroup (no job-scoped isolation).
_ink_get_cgroup_v2_path() {
    local line path
    line=$(/bin/awk 'NR==1 && /^0::/' /proc/self/cgroup 2>/dev/null)
    [[ -n "$line" ]] || return 1
    path="${line#0::}"
    [[ -n "$path" && "$path" != "/" ]] || return 1
    printf '%s\n' "$path"
}

# Tri-state: 0=active, 1=inactive, 2=cgroup unusable (caller falls back).
_ink_has_cpu_activity_cgroup() {
    local cg stat_file t1 t2 delta_us thresh_pct thresh_us
    cg=$(_ink_get_cgroup_v2_path) || return 2
    stat_file="/sys/fs/cgroup${cg}/cpu.stat"
    [[ -r "$stat_file" ]] || return 2
    t1=$(/bin/awk '/^usage_usec/{print $2; exit}' "$stat_file" 2>/dev/null)
    [[ "$t1" =~ ^[0-9]+$ ]] || return 2
    sleep 5
    t2=$(/bin/awk '/^usage_usec/{print $2; exit}' "$stat_file" 2>/dev/null)
    [[ "$t2" =~ ^[0-9]+$ ]] || return 2
    delta_us=$(( t2 - t1 ))
    # Sanity guard: coerce non-positive-integer thresholds to the default 5.
    # Without it a negative threshold makes every delta "active" and the job
    # would never be reaped.
    thresh_pct="${INK_ACTIVE_CPU_MIN_PCT:-5}"
    [[ "$thresh_pct" =~ ^[1-9][0-9]*$ ]] || thresh_pct=5
    thresh_us=$(( 5 * thresh_pct * 10000 ))
    (( delta_us >= thresh_us ))
}

# Fallback when cgroup v2 is unavailable: pidstat's 5-second sample, scoped
# to this user.  Isolated from other users, but shared with this user's
# other concurrent jobs on the same node.
_ink_has_cpu_activity_pidstat() {
    command -v pidstat >/dev/null 2>&1 || return 1
    local user_name thresh
    user_name=$(/usr/bin/id -un)
    thresh="${INK_ACTIVE_CPU_MIN_PCT:-5}"
    [[ "$thresh" =~ ^[1-9][0-9]*$ ]] || thresh=5
    # LC_ALL=C because sysstat localises the "Average:" prefix (e.g. to a
    # Chinese word under zh_CN), which would bypass the regex below.
    # $(NF-2) rather than $8 for the %CPU column: sysstat 10.x (CentOS 7)
    # emits 9 columns without %wait, 11.1.1+/12.x emit 10 with it.  The last
    # three columns are always "%CPU CPU Command", so NF-2 is version-proof.
    LC_ALL=C pidstat -u -U "$user_name" 5 1 2>/dev/null | /bin/awk -v thresh="$thresh" '
        /^Average:/ && $(NF-2)+0 >= thresh+0 && $NF !~ /pidstat/ {
            found = 1
            exit
        }
        END { exit(!found) }
    '
}

# Background-CPU activity, cgroup first then pidstat.  Covers "the user
# closed the browser but left a simulation running" -- X-idle and the
# code-server heartbeat both go quiet in that case, and the watchdog used
# to reap the still-productive job (see the job 55256 postmortem).
ink_has_cpu_activity() {
    _ink_has_cpu_activity_cgroup
    local rc=$?
    (( rc != 2 )) && return $rc
    _ink_has_cpu_activity_pidstat
}

# VNC family liveness: X keyboard/mouse input OR background CPU work.
ink_active_vnc() {
    local idle_ms
    idle_ms=$(DISPLAY="${INK_VNC_DISPLAY}" ink_read_x_idle_ms)
    if [[ "$idle_ms" =~ ^[0-9]+$ ]] && (( idle_ms < INK_ACTIVE_IDLE_SEC * 1000 )); then
        return 0
    fi
    ink_has_cpu_activity
}

# Terminal-level liveness: is a program running in one of this session's
# integrated terminals, or did the user type into one recently?
#
# Neither of the other two code-server signals covers an agent CLI (claude /
# opencode) driven from the integrated terminal.  The heartbeat extension only
# fires on editor interaction, and such a process is network-bound -- it spends
# its wall time waiting on a remote API, so its CPU sits below
# INK_ACTIVE_CPU_MIN_PCT while it is genuinely working.  The watchdog therefore
# saw "inactive" and reaped the job at its deadline with the task still running.
#
# Two signals, both scoped to this session's own process tree ($1, default
# $APP_PID) so the user's other jobs on the same node cannot keep this one alive:
#   - a tree process holding a PTY slave has a live child -> a program is
#     running in a terminal (true even at 0% CPU)
#   - a PTY slave's atime is fresh -> the user typed recently
# Zombies are excluded: an unreaped exit is not work in progress.
#
# Returns 0 only on positive evidence.  Anything unusable (no python, no root
# pid, unreadable /proc) returns 1 so the caller falls through to the CPU
# signal -- this probe can only make reaping less eager, never more.
#
# Ported from jupyter/run.sh's is_active() signals 3+4; same /proc BFS.
ink_active_pty() {
    local py root_pid rc
    py="${INK_PYTHON:-$(ink_pick_system_python)}"
    [[ -n "$py" && -x "$py" ]] || return 1
    root_pid="${1:-${APP_PID:-}}"
    [[ "$root_pid" =~ ^[0-9]+$ ]] || return 1

    INK_PTY_ROOT_PID="$root_pid" \
    INK_PTY_IDLE_SEC="${INK_ACTIVE_IDLE_SEC:-1800}" \
    "$py" - <<'PY' 2>/dev/null
import os, sys, time

root = int(os.environ["INK_PTY_ROOT_PID"])
# Same sanity guard the shell side applies to INK_ACTIVE_CPU_MIN_PCT: a
# non-numeric or non-positive window would either raise (losing the signal
# silently) or make the atime test never true.  Fall back to the default.
try:
    window = int(os.environ["INK_PTY_IDLE_SEC"])
except (TypeError, ValueError):
    window = 1800
if window <= 0:
    window = 1800
now = time.time()


def children(pid):
    # Walk EVERY thread's children file, not just the main thread's
    # (task/<pid>/children).  A child forked from a still-alive non-main thread
    # is listed under that thread's task/<tid>/children, not the main one --
    # and code-server is node.js, which runs its pty-host / terminal work off
    # dedicated threads.  Reading only the main thread would miss that whole
    # subtree, i.e. fail to see a program running in the terminal, which is a
    # false negative -- exactly the direction this probe must never err in
    # (its contract is "less eager to reap, never more").  A child appears in
    # exactly one thread's list, so the union is the true child set.
    out = []
    try:
        tids = os.listdir("/proc/%d/task" % pid)
    except OSError:
        return out
    for tid in tids:
        try:
            with open("/proc/%d/task/%s/children" % (pid, tid)) as fh:
                out.extend(int(c) for c in fh.read().split())
        except (OSError, ValueError):
            continue
    return out


def live_children(pid):
    out = []
    for c in children(pid):
        try:
            with open("/proc/%d/stat" % c) as fh:
                stat = fh.read()
        except OSError:
            continue
        # Field 3 is the state; rsplit on ")" first because comm may itself
        # contain spaces and parentheses.
        state = stat.rsplit(")", 1)[-1].split()[0]
        if state != "Z":
            out.append(c)
    return out


def tree(root):
    # Dedupe at ENQUEUE time, not at pop time: a pop-time check still lets a
    # self-referential or cyclic /proc children file re-queue the same pid
    # forever and grow the queue without bound.  This probe runs inside the
    # watchdog with no timeout around it, so a hang here would wedge the
    # watchdog loop and make the job unreapable -- worse than the false kill
    # this probe exists to prevent.  MAX_NODES caps a pathological /proc.
    MAX_NODES = 4096
    seen, queue = {root}, []
    queue.extend(children(root))
    while queue and len(seen) < MAX_NODES:
        pid = queue.pop()
        if pid in seen:
            continue
        seen.add(pid)
        for c in children(pid):
            if c not in seen:
                queue.append(c)
    return seen


def pty_slaves(pid):
    slaves = set()
    try:
        fds = os.listdir("/proc/%d/fd" % pid)
    except OSError:
        return slaves
    for fd in fds:
        try:
            target = os.readlink("/proc/%d/fd/%s" % (pid, fd))
        except OSError:
            continue
        if target.startswith("/dev/pts/") and target[9:].isdigit():
            slaves.add(target)
    return slaves


pids = tree(root)

# Signal 3: a tree process holds a PTY slave AND has a live (non-zombie) child
# -- a program is running in a terminal.  This is the signal that matters for
# "claude is running a task in the integrated terminal" and it is true even at
# 0% CPU.  It does not depend on atime, so it works regardless of mount options.
for pid in pids:
    if pty_slaves(pid) and live_children(pid):
        sys.exit(0)

# Signal 4: a PTY slave's atime is fresh -> the user typed recently, even if no
# command is running right now.  NOTE this is best-effort: /dev/pts is commonly
# mounted relatime (verified on the el9 workers), under which a read/write does
# NOT bump atime, so this signal is effectively inert there.  It is kept because
# it is correct where atime is live (other kernels/mounts, jupyter's origin) and
# can only ever add a keep-alive, never remove one -- signal 3 above is what
# actually guards a running terminal program.
for pid in pids:
    for slave in pty_slaves(pid):
        try:
            if now - os.stat(slave).st_atime <= window:
                sys.exit(0)
        except OSError:
            pass

sys.exit(1)
PY
    rc=$?
    # 0 = positive evidence of activity, 1 = no evidence.  Anything else means
    # the probe itself broke (python crash, killed by a signal, bad env); say
    # so on stderr before falling back, because a silently dead probe degrades
    # to exactly the CPU-only behaviour that mis-reaped jobs in the first place
    # and would otherwise be indistinguishable from "user is idle".
    case "$rc" in
        0) return 0 ;;
        1) return 1 ;;
        *)
            echo "[INK] WARN: terminal-activity probe failed (rc=$rc); falling back to the CPU signal" >&2
            return 1
            ;;
    esac
}

# code-server family liveness, in ascending cost: the ink-heartbeat browser
# extension's file mtime (one stat), then terminal activity (one python + a
# /proc walk), then background CPU (a 5-second sample).
#
# The extension only writes while the window is focused and the user is
# editing/scrolling/selecting, so a backgrounded tab does not keep the job
# alive.  The terminal probe covers programs driven from the integrated
# terminal; the CPU signal remains the last resort for compute-only work with
# no terminal attached (see the job 55256 postmortem).
ink_active_codeserver() {
    local hb mtime now
    hb="${INK_HEARTBEAT_FILE:-}"
    if [[ -n "$hb" && -f "$hb" ]]; then
        mtime=$(stat -c %Y "$hb" 2>/dev/null) || mtime=""
        if [[ "$mtime" =~ ^[0-9]+$ ]]; then
            now=$(date +%s)
            (( now - mtime < INK_ACTIVE_IDLE_SEC )) && return 0
        fi
    fi
    ink_active_pty && return 0
    ink_has_cpu_activity
}

# Deadlines shared by the session runners:
#   DEADLINE     (soft) -- when the watchdog starts intervening, from
#                           INK_INIT_HOURS (default 24h).
#   MAX_DEADLINE (hard) -- absolute ceiling on total life, from INK_MAX_HOURS;
#                           past it the job is closed even if still active, so
#                           a forgotten-but-busy session cannot run forever.
#                           0 (default / unset / non-positive) = no ceiling:
#                           renew indefinitely while active -- the historical
#                           behaviour, kept for apps whose long runs must not be
#                           cut at a fixed wall (e.g. asic/asicbm design runs).
ink_set_deadlines() {
    local now=$(( $(date +%s) ))
    # Default-guarded: openclaw/run.sh runs under `set -u`, and these exported
    # functions inherit the caller's shell options.
    DEADLINE=$(( now + ${INK_INIT_HOURS:-24} * 3600 ))
    if [[ "${INK_MAX_HOURS:-0}" =~ ^[1-9][0-9]*$ ]]; then
        MAX_DEADLINE=$(( now + INK_MAX_HOURS * 3600 ))
    else
        MAX_DEADLINE=0
    fi
}

# Renew the forwarded Kerberos TGT and re-derive AFS tokens.  A job that outlives
# its initial ticket (or is extended by the watchdog) would otherwise lose /afs
# access mid-session: reads/writes under /afs start failing even though the job
# is still alive and the user is still working.  `kinit -R` renews from the
# ticket's renewable lifetime without a password; aklog then refreshes the AFS
# tokens from the renewed TGT.
#
# Throttled to one attempt per INK_KRB_RENEW_HOURS (default 6).  The IHEP ticket
# lifetime is 48h, so 6h leaves an 8x margin while keeping KDC/AFS chatter -- and
# the warning below -- far quieter than renewing on every 900s liveness poll.
# Note this is deliberately decoupled from INK_CHECK_INTERVAL: the liveness poll
# stays frequent because it is cheap and local, and its interval sets the
# reaping latency and the INK_INACTIVE_GRACE window.
#
# Records _INK_KRB_RENEW_FAILED so ink_past_ceiling can fall back to the
# ticket's own expiry once renewal stops working.  Best-effort and never fatal:
# a renewal failure must not take down the watchdog, so this always returns 0.
ink_renew_krb() {
    [[ -n "${KRB5CCNAME:-}" ]] || return 0
    command -v kinit >/dev/null 2>&1 || return 0
    local now=$(( $(date +%s) )) every
    every="${INK_KRB_RENEW_HOURS:-6}"
    [[ "$every" =~ ^[1-9][0-9]*$ ]] || every=6
    every=$(( every * 3600 ))
    if [[ -n "${_INK_KRB_LAST_TRY:-}" ]] && (( now - _INK_KRB_LAST_TRY < every )); then
        return 0                      # not due yet
    fi
    _INK_KRB_LAST_TRY=$now
    if kinit -Rc "${KRB5CCNAME}" 2>/dev/null; then
        # `|| true`: openclaw/run.sh runs under `set -euo pipefail`, and these
        # exported functions execute in the CALLER's shell options.  A failing
        # command in the final position of a && list is not exempt from set -e,
        # so an aklog failure (no AFS reachability, ticket not renewable) would
        # otherwise kill the job -- the exact opposite of "best-effort renewal".
        if command -v /usr/bin/aklog >/dev/null 2>&1; then
            /usr/bin/aklog 2>/dev/null || true
        fi
        _INK_KRB_RENEW_FAILED=0
    else
        _INK_KRB_RENEW_FAILED=1
        echo "[INK] WARN: kinit -R failed for ${KRB5CCNAME}; AFS access will lapse when the ticket expires" >&2
    fi
    return 0
}

# True (0) when the job's Kerberos ticket is no longer usable.
#
# Uses `klist -s` (exit status only: 0 = a valid ticket is present, 1 = none)
# rather than parsing klist's date text.  That text is locale-dependent --
# MM/DD/YY under LC_ALL=C, DD/MM/YY under e.g. en_GB -- and an earlier version
# parsed it as DD/MM/YY while pinning LC_ALL=C, so it built dates like
# 2026-20-10 from a real 10/20/26 (invalid -> the fallback silently disabled
# itself) or 2026-05-11 from a real 11/05/26 (valid but in the PAST -> the job
# was killed while its ticket was still good).  Exit status has no such
# ambiguity and no locale/version dependency.
#
# Unknown (no KRB5CCNAME, no klist) is treated as ALIVE so a missing tool can
# never kill a job.  `|| true` is not needed: klist -s is in an `if` condition,
# which is exempt from a caller's `set -e` / `set -o pipefail`.
ink_krb_ticket_dead() {
    [[ -n "${KRB5CCNAME:-}" ]] || return 1
    command -v klist >/dev/null 2>&1 || return 1
    if LC_ALL=C klist -s -c "${KRB5CCNAME}" >/dev/null 2>&1; then
        return 1                       # a valid ticket is present
    fi
    return 0                           # expired / missing / unreadable ccache
}

# True (0) once the job has passed its effective runtime ceiling.  $1 = now.
#
# Ceiling = MAX_DEADLINE (INK_MAX_HOURS).  Additionally, once renewal has
# stopped working AND the ticket is actually gone, the job stops too: running on
# would mean every /afs access fails, so the ticket's own lifetime becomes the
# effective cap -- "7 days while the token renews, the token's limit when it
# cannot".  Note this fires when the ticket is truly dead, not before, so a job
# is never killed while its ticket still works.
#
# Returns false immediately when no ceiling is configured (MAX_DEADLINE = 0), so
# apps that deliberately run uncapped (asic/asicbm design runs) keep going even
# with a dead ticket -- their guarantee is "run the full init_hours regardless".
#
# Every test here is an `if` condition, never a bare (( )): a bare arithmetic
# test that evaluates false returns 1, which under a caller's `set -e`
# (openclaw/run.sh) would kill the job.
ink_past_ceiling() {
    local now=$1 ceiling=${MAX_DEADLINE:-0}
    if (( ceiling <= 0 )); then
        return 1                       # uncapped app: no ceiling to pass
    fi
    if (( now >= ceiling )); then
        return 0                       # configured runtime cap reached
    fi
    if (( ${_INK_KRB_RENEW_FAILED:-0} == 1 )) && ink_krb_ticket_dead; then
        return 0                       # renewal broken and ticket already dead
    fi
    return 1
}

# -- generic watchdog -------------------------------------------------------

# Background watchdog: $1 = app pid, $2 = human-readable app label for the
# log line, $3 = name of the liveness function.
#
# Before DEADLINE it does not interfere.  After DEADLINE it polls every
# INK_CHECK_INTERVAL and keeps the job alive while the liveness function says
# the user is still working; once that goes false it sends SIGTERM.  Renewal is
# bounded by ink_past_ceiling -- INK_MAX_HOURS when configured, or the Kerberos
# ticket's own expiry once the ticket can no longer be renewed.  With no
# ceiling configured the job runs as long as it is in use.
#
# Every liveness signal is point-in-time (a 5-second CPU window, a heartbeat
# or PTY mtime), so one unlucky sample landing between two bursts of work
# used to reap a job that was still in use.  Reaping now requires
# INK_INACTIVE_GRACE consecutive inactive polls (default 3); any active poll
# resets the counter.
ink_watchdog() {
    local app_pid=$1 label=$2 active_fn=$3
    local grace="${INK_INACTIVE_GRACE:-3}" misses=0
    # Sanity guard, same shape as the CPU threshold: a non-positive-integer
    # grace would either kill on the first sample or never kill at all.
    [[ "$grace" =~ ^[1-9][0-9]*$ ]] || grace=3
    while kill -0 "$app_pid" 2>/dev/null; do
        now=$(date +%s)
        # Keep the Kerberos TGT / AFS tokens fresh (self-throttled to once per
        # INK_KRB_RENEW_HOURS) so a long or extended session never loses /afs
        # access.  Best-effort, never fatal.
        ink_renew_krb
        # Absolute ceiling, checked BEFORE the deadline gate so that it really is
        # absolute: a site configuring max_hours below init_hours still stops at
        # max_hours, and a ticket that dies before DEADLINE ends the job when it
        # dies rather than at the next deadline poll.  For uncapped apps
        # (MAX_DEADLINE=0) this returns false immediately, leaving the "run the
        # full init_hours no matter what" guarantee untouched.
        if ink_past_ceiling "$now"; then
            echo "[INK] 已达运行时长上限（${INK_MAX_HOURS}h，或票据无法续期且已失效），不再延期，正常关闭 ${label}。"
            kill -TERM "$app_pid" 2>/dev/null
            break
        fi
        if (( now >= DEADLINE )); then
            if "$active_fn"; then
                if (( misses > 0 )); then
                    echo "[INK] 已到期，活跃度恢复（此前连续 ${misses} 次未检测到），保持运行。"
                else
                    echo "[INK] 已到期但用户仍在使用，保持运行。"
                fi
                misses=0
            else
                misses=$(( misses + 1 ))
                if (( misses >= grace )); then
                    echo "[INK] 已到期且连续 ${misses} 次未检测到使用，正常关闭 ${label}。"
                    kill -TERM "$app_pid" 2>/dev/null
                    break
                fi
                echo "[INK] 已到期且未检测到使用（${misses}/${grace}），${INK_CHECK_INTERVAL}s 后复查。"
            fi
        fi
        sleep "$INK_CHECK_INTERVAL"
    done
}

# -- VNC family session -----------------------------------------------------

# Full TurboVNC + noVNC session: port allocation, stale-session cleanup,
# vncserver start, FS-based OTP listener, noVNC proxy, and the monitor loop.
#
# Knobs (all optional, all default to the historical values):
#   VNC_CMD    vncserver binary    default /opt/TurboVNC/bin/vncserver
#   NOVNC_CMD  novnc_proxy binary  default the IHEP cvmfs path
#   APP_PORT / LISTEN_PORT         default freshly allocated free ports
ink_run_vnc_session() {
    export PATH="$HOME/.local/bin:$PATH"
    if grep -q "CentOS Linux release 7.9" /etc/redhat-release 2>/dev/null; then
        _VNC_CENTOS_BIN="/cvmfs/slurm.ihep.ac.cn/centos7.9/anaconda3/envs/ink/bin"
        _VNC_CENTOS_LD="/cvmfs/slurm.ihep.ac.cn/centos7.9/anaconda3/envs/ink/lib"
        export PATH="${_VNC_CENTOS_BIN}:${PATH}"
        export LD_LIBRARY_PATH="${_VNC_CENTOS_LD}:${LD_LIBRARY_PATH:-}"
    fi
    _PYBIN="$(command -v python3 || command -v python || true)"
    if ! command -v websockify >/dev/null 2>&1; then
        if [ -n "${_PYBIN}" ]; then
            "${_PYBIN}" -m pip install --user --quiet websockify || \
                echo "[WARN] pip install websockify failed; will use 'python -m websockify' instead." >&2
            hash -r
        else
            echo "[WARN] no python found; cannot install websockify." >&2
        fi
    fi
    if command -v websockify >/dev/null 2>&1; then
        export WEBSOCKIFY="$(command -v websockify)"
    fi
    export VNC_CMD="${VNC_CMD:-/opt/TurboVNC/bin/vncserver}"
    export NOVNC_CMD="${NOVNC_CMD:-/cvmfs/common.ihep.ac.cn/software/noVNC-master/utils/novnc_proxy}"

    APP_PORT="${APP_PORT:-$(get_free_port)}"
    LISTEN_PORT="${LISTEN_PORT:-$(get_free_port)}"
    ink_set_deadlines    # sets DEADLINE (soft) + MAX_DEADLINE (hard ceiling)

    INK_PYTHON="$(ink_pick_system_python)"
    # Filled in below from the vncserver startup banner.
    INK_VNC_DISPLAY=""

    cleanup() {
        if [ -n "${NOVNC_PID}" ]; then
            # Killing the novnc_proxy wrapper makes its own trap forward the
            # signal to the websockify child.  But if we signal during the
            # window between fork and the wrapper installing that trap, the
            # child is orphaned -- so anchor-kill any leftover proxy/websockify
            # as a backstop.
            #
            # This backstop is scoped to THIS job's own ports, NOT uid-wide like
            # the startup "new kills old" sweep below.  cleanup() fires on our
            # (possibly delayed) exit: the monitor loop only polls every
            # INK_CHECK_INTERVAL, so by the time we exit a newer same-user
            # session on this node may already be running its own noVNC.  A
            # uid-wide pkill here would kill that live session's proxy.  Anchor
            # on our own "<host>:<APP_PORT> --listen <LISTEN_PORT>" (wrapper) and
            # the websockify child's "<LISTEN_PORT> <host>:<APP_PORT>" tail so we
            # only ever reap the proxy we started.  \b guards against a port that
            # is a prefix of a longer one (5901 vs 59010).
            kill ${NOVNC_PID} 2>/dev/null
            if [ -n "${HOST_FULL_NAME:-}" ] && [ -n "${APP_PORT:-}" ] && [ -n "${LISTEN_PORT:-}" ]; then
                pkill -u "$(id -u)" -f \
                    "${NOVNC_CMD} --vnc ${HOST_FULL_NAME}:${APP_PORT} --listen ${LISTEN_PORT}" 2>/dev/null
                pkill -u "$(id -u)" -f \
                    " ${LISTEN_PORT} ${HOST_FULL_NAME}:${APP_PORT}\b" 2>/dev/null
            fi
        fi
        # OTP watcher/gc backgrounded by otp_start_listener.
        otp_stop_listener 2>/dev/null || true
    }
    trap cleanup EXIT INT TERM

    unset DBUS_SESSION_BUS_ADDRESS
    unset DEBUGINFOD_URLS

    mkdir -p RUN_TMP
    chmod 700 RUN_TMP
    export XDG_RUNTIME_DIR=$(/bin/realpath RUN_TMP)

    if [[ -z "${HOME:-}" ]]; then
        export HOME="$(getent passwd "$(id -un)" | cut -d: -f6)"
    fi
    export XAUTHORITY="$HOME/.Xauthority"

    APP_LOGIN_INFO="app_login.info"
    APP_RUN_HOST=$(/bin/hostname | /bin/awk -F '.' '{print $1}')
    HOST_FULL_NAME=$(/bin/hostname)

    # Clear stale VNC sessions and X locks from a previous job on this node.
    # Policy: one VNC per user per node -- starting a new session kills the old.
    EX_DPS=$(${VNC_CMD} -list 2> /dev/null | /bin/awk '/^:/ {print $1}')
    if [ -n "${EX_DPS}" ]; then
        for disp in ${EX_DPS}; do
            ${VNC_CMD} -kill "$disp" &> /dev/null
        done
    fi

    # Kill the old session's noVNC proxy too, or it lingers as an orphan: the
    # previous job's own cleanup() only runs on a clean exit, and its monitor
    # loop notices the dead VNC only at the next INK_CHECK_INTERVAL poll (900s
    # default) -- so a condor_rm / OOM / node-eviction leaves websockify bound
    # to LISTEN_PORT with no backend, and a fresh session that reuses the port
    # would connect to that dead proxy ("no VNC display").  Same "new kills old"
    # rule the VNC server above follows, scoped to this uid so other users on a
    # shared node are untouched.
    #
    # Match tightly, NOT on the bare install path: a bare "pkill -f <dir>" also
    # kills any process that merely mentions the path in its argv (an editor
    # viewing the file, a grep, a script passing it as data).  The wrapper is
    # always launched as "<dir>/utils/novnc_proxy --vnc ..." so we anchor on the
    # "--vnc" flag; its websockify child is launched by novnc_proxy as
    # "<websockify> --web <home> <port> <host:port> ...", but <websockify> may be
    # the pip "websockify", the bundled "utils/websockify/run", or "python .../run"
    # -- so we must NOT anchor on the binary name.  Anchor on the realpath'd
    # "--web <home> " argument novnc_proxy always passes (it realpaths WEB, and
    # <home> here is likewise realpath'd via cd+pwd, verified equal), which hits
    # every websockify variant and still spares path-mentioning bystanders like
    # vim/less/grep on the dir (they carry no "--web <home> " token).
    #
    # This sweep is deliberately uid-wide (all of this user's stale proxies on
    # the node): it runs BEFORE our own noVNC starts, so it cannot hit ours, and
    # "new kills old" is exactly the intent.  The cleanup() backstop above is the
    # opposite -- port-scoped to our own session -- because it fires on our exit,
    # when a newer same-user session may already own a live proxy here.
    _NOVNC_HOME=$(cd "$(dirname "${NOVNC_CMD}")/.." 2>/dev/null && pwd)
    if [ -n "${_NOVNC_HOME}" ]; then
        pkill -u "$(id -u)" -f "${NOVNC_CMD} --vnc" 2>/dev/null || true
        pkill -u "$(id -u)" -f -- "--web ${_NOVNC_HOME} " 2>/dev/null || true
    fi

    /bin/rm -f "$HOME/.vnc/${HOSTNAME}"*.pid
    /bin/rm -f "$HOME/.vnc/${HOSTNAME}"*.lock
    /bin/rm -f "$HOME/.vnc/${HOSTNAME}"*.log
    /bin/rm -f /tmp/.X*-lock /tmp/.X11-unix/X* 2>/dev/null

    # === 1. start the VNC server ===
    VNC_START_OUTPUT=$(${VNC_CMD} -rfbport ${APP_PORT} -securitytypes OTP -otp 2>&1)
    echo "${VNC_START_OUTPUT}"
    INK_VNC_DISPLAY=$(echo "${VNC_START_OUTPUT}" \
        | /bin/grep -oP 'started on display \S+:\K[0-9]+' \
        | /bin/awk '{print ":" $1; exit}')
    echo "[INK] VNC display: ${INK_VNC_DISPLAY}"
    /bin/echo "{\"HOST\": \"${APP_RUN_HOST}\", \"PORT\": \"${LISTEN_PORT}\"}" > ${APP_LOGIN_INFO}

    # FS-based OTP listener, now that the VNC session exists.  Server side:
    # fastink.computing.apps._helpers.generate_userotp.
    otp_start_listener

    # === 2. noVNC proxy in the background ===
    # This exact line is the app's start_keywords match -- do not reword.
    echo "Starting noVNC proxy on port ${LISTEN_PORT}..."
    ${NOVNC_CMD} --vnc ${HOST_FULL_NAME}:${APP_PORT} --listen ${LISTEN_PORT} >/dev/null 2>&1 &
    NOVNC_PID=$!

    echo "[INK] VNC 初始时长 ${INK_INIT_HOURS} 小时，初始截止：$(date -d @${DEADLINE} '+%F %T')"
    (( ${MAX_DEADLINE:-0} > 0 )) && echo "[INK] VNC 最大运行上限 ${INK_MAX_HOURS} 小时（$(date -d @${MAX_DEADLINE} '+%F %T')）；到期后即使仍在使用也不再延期。"

    # === 3. monitor loop: liveness of proxy/port, then deadline watch ===
    while true; do
        if ! kill -0 ${NOVNC_PID} 2>/dev/null; then
            echo "[INFO] noVNC proxy process ended unexpectedly."
            break
        fi
        if ! /usr/sbin/ss -ltn | grep -q ":${APP_PORT}\b"; then
            echo "[INFO] VNC Server port ${APP_PORT} is closed. VNC likely crashed or exited."
            break
        fi

        # Keep the Kerberos TGT / AFS tokens fresh (self-throttled to once per
        # INK_KRB_RENEW_HOURS) so a long or extended session never loses /afs
        # access.  Best-effort, never fatal.
        ink_renew_krb

        now=$(date +%s)
        # Absolute ceiling, checked BEFORE the deadline gate so that it really is
        # absolute (see the same note in ink_watchdog).  Uncapped apps such as
        # asic/asicbm have MAX_DEADLINE=0, so this is a no-op for them and their
        # "run the full init_hours even if the ticket dies" guarantee holds.
        if ink_past_ceiling "$now"; then
            echo "[INK] 已达运行时长上限（${INK_MAX_HOURS}h，或票据无法续期且已失效），不再延期，正常关闭 VNC。"
            break
        fi
        if (( now >= DEADLINE )); then
            if ink_active_vnc; then
                echo "[INK] 已到期但用户仍在使用，保持运行。"
            else
                echo "[INK] 已到期且用户不再使用，正常关闭 VNC。"
                break
            fi
        fi

        sleep "${INK_CHECK_INTERVAL}"
    done

    exit 0
}

# -- code-server family session --------------------------------------------

# Full code-server session: password generation, config.yaml, heartbeat
# extension install, background launch, watchdog, and exit-code propagation.
#
# Knobs (all optional):
#   VSCODE_BIN                default /usr/bin/code-server
#   NODE_OPTIONS              default --max-old-space-size=5120
#   INK_VSCODE_EXT_SHARED     heartbeat extension source on shared storage
#   INK_HEARTBEAT_FILE        default ${APP_PATH}/.ink_heartbeat
ink_run_codeserver_session() {
    export VSCODE_BIN="${VSCODE_BIN:-/usr/bin/code-server}"
    export NODE_OPTIONS="${NODE_OPTIONS:---max-old-space-size=5120}"

    APP_PATH="${APP_PATH:-$(/bin/pwd)}"
    APP_PORT="${APP_PORT:-$(get_free_port)}"
    APP_PASSWD=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
    APP_CONFIG_FILE="config.yaml"
    APP_LOGIN_INFO="app_login.info"
    APP_RUN_HOST="`/bin/hostname | /bin/awk -F '.' '{print $1}'`"
    ink_set_deadlines    # sets DEADLINE (soft) + MAX_DEADLINE (hard ceiling)

    # Written by the ink-heartbeat extension when the window is focused and
    # the user is really interacting with the editor.
    INK_HEARTBEAT_FILE="${INK_HEARTBEAT_FILE:-${APP_PATH}/.ink_heartbeat}"
    export INK_HEARTBEAT_FILE

    (
    umask 077
    cat > "${APP_CONFIG_FILE}" << EOL
bind-addr: 0.0.0.0:${APP_PORT}
auth: password
password: ${APP_PASSWD}
EOL
    )

    /bin/echo "{\"HOST\": \"${APP_RUN_HOST}\", \"PORT\": \"${APP_PORT}\", \"PASSWD\": \"${APP_PASSWD}\"}" > ${APP_LOGIN_INFO}

    echo "[INK] VSCode 初始时长 ${INK_INIT_HOURS} 小时，初始截止：$(date -d @${DEADLINE} '+%F %T')"
    (( ${MAX_DEADLINE:-0} > 0 )) && echo "[INK] VSCode 最大运行上限 ${INK_MAX_HOURS} 小时（$(date -d @${MAX_DEADLINE} '+%F %T')）；到期后即使仍在使用也不再延期。"

    INK_VSCODE_EXT_SHARED="${INK_VSCODE_EXT_SHARED:-/cvmfs/common.ihep.ac.cn/software/ink/soft/extensions/ink-heartbeat-ext}"
    INK_EXT_DIR="${HOME}/.local/share/code-server/extensions/ink.ink-heartbeat-1.0.0"
    mkdir -p "$INK_EXT_DIR"
    cp -rf "${INK_VSCODE_EXT_SHARED}/." "$INK_EXT_DIR/"

    # Copying the files is not enough: code-server decides which extensions to
    # activate from the profile manifest extensions.json, NOT by scanning the
    # directory (scanUserExtensions -> scanExtensionsFromProfile).  The one-time
    # dir->manifest migration only runs when the manifest file is absent, so an
    # existing [] manifest (the common case here) means a plain cp is never
    # picked up and the heartbeat silently never loads.  Worse, when another
    # code-server-family job of the same user cp's into this shared $HOME dir,
    # code-server can mark our extension in .obsolete, disabling it outright.
    # Register the extension in the manifest and clear any stale .obsolete flag.
    INK_EXT_ROOT="$(dirname "$INK_EXT_DIR")"
    INK_PY_REG="${INK_PYTHON:-$(ink_pick_system_python)}"
    if [[ -n "$INK_PY_REG" && -x "$INK_PY_REG" && -f "${INK_EXT_DIR}/package.json" ]]; then
        INK_EXT_RELDIR="$(basename "$INK_EXT_DIR")" \
        INK_EXT_ROOT="$INK_EXT_ROOT" \
        INK_EXT_PKG="${INK_EXT_DIR}/package.json" \
        "$INK_PY_REG" - <<'PY' || echo "[INK] WARN: heartbeat extension registration skipped" >&2
import json, os, sys

rel = os.environ["INK_EXT_RELDIR"]
ext_root = os.environ["INK_EXT_ROOT"]
ext_dir = os.path.join(ext_root, rel)
manifest = os.path.join(ext_root, "extensions.json")
obsolete = os.path.join(ext_root, ".obsolete")

with open(os.environ["INK_EXT_PKG"]) as fh:
    pkg = json.load(fh)
ext_id = "%s.%s" % (pkg["publisher"], pkg["name"])
# Exactly the entry shape code-server writes itself when it migrates a
# directory into the manifest: identifier + version + an absolute file:// URI
# in `location` (required -- a manifest entry without it is rejected as
# "Invalid extensions content") plus `relativeLocation` so the entry survives a
# move of the extensions dir.  metadata is intentionally omitted; the migrated
# entry has none.
entry = {
    "identifier": {"id": ext_id},
    "version": str(pkg["version"]),
    "location": {"$mid": 1, "path": ext_dir, "scheme": "file"},
    "relativeLocation": rel,
}

# Absent/empty manifest is safe to create; a present-but-unparseable one may
# hold real extensions we cannot read, so never clobber it -- overwriting would
# silently uninstall the user's whole set.
items = []
if os.path.exists(manifest):
    try:
        with open(manifest) as fh:
            txt = fh.read().strip()
        items = json.loads(txt) if txt else []
    except ValueError:
        items = None
    if items is not None and not isinstance(items, list):
        items = None
if items is None:
    sys.stderr.write("[ink-heartbeat] extensions.json unparseable; left untouched\n")
    sys.exit(2)

# Idempotent: drop any prior entry for this id, then append the current one, so
# repeated job starts and version bumps never leave duplicates.
items = [it for it in items
         if not (isinstance(it, dict) and (it.get("identifier") or {}).get("id") == ext_id)]
items.append(entry)
tmp = manifest + ".tmp.%d" % os.getpid()
with open(tmp, "w") as fh:
    json.dump(items, fh)
os.replace(tmp, manifest)

# Undo a .obsolete tombstone a concurrent job may have written for us; touch
# only our own key so other pending removals are preserved.
try:
    with open(obsolete) as fh:
        ob = json.load(fh)
    if isinstance(ob, dict) and rel in ob:
        del ob[rel]
        if ob:
            tmp = obsolete + ".tmp.%d" % os.getpid()
            with open(tmp, "w") as fh:
                json.dump(ob, fh)
            os.replace(tmp, obsolete)
        else:
            os.remove(obsolete)
except (OSError, ValueError):
    pass
PY
    fi

    # code-server in the background; the watchdog guards its pid.
    ${VSCODE_BIN} --config ${APP_PATH}/${APP_CONFIG_FILE} 2>&1 &
    APP_PID=$!
    # APP_PID is the tree root ink_active_pty walks.  The watchdog below is a
    # forked subshell of this shell, so it inherits the variable whether or not
    # it is exported; exporting keeps the contract explicit and matches
    # jupyter/run.sh.  Note code-server is launched *above*, before this
    # assignment, so APP_PID is deliberately NOT in code-server's own
    # environment -- do not probe /proc/<code-server>/environ for it.
    export APP_PID

    trap 'kill -TERM ${APP_PID} 2>/dev/null' INT TERM

    ink_watchdog "${APP_PID}" "VSCode" ink_active_codeserver &
    WATCHDOG_PID=$!

    wait "${APP_PID}"
    EXIT_CODE=$?
    kill "${WATCHDOG_PID}" 2>/dev/null
    exit ${EXIT_CODE}
}

export -f ink_pick_system_python ink_read_x_idle_ms \
          _ink_get_cgroup_v2_path _ink_has_cpu_activity_cgroup \
          _ink_has_cpu_activity_pidstat ink_has_cpu_activity \
          ink_active_pty \
          ink_active_vnc ink_active_codeserver ink_watchdog \
          ink_set_deadlines ink_renew_krb \
          ink_krb_ticket_dead ink_past_ceiling \
          ink_run_vnc_session ink_run_codeserver_session

# -- job working dir --
APP_PATH="$(/bin/pwd)"
export APP_PATH

# -- kerberos / AFS --
if [ -f "${APP_PATH}/krb5cc_${UID}" ]; then
    export KRB5CCNAME="${APP_PATH}/krb5cc_${UID}"
fi
if command -v /usr/bin/aklog >/dev/null 2>&1; then
    /usr/bin/aklog
fi

# -- interactive-job watchdog defaults --
export INK_INIT_HOURS="${INK_INIT_HOURS:-24}"
# Absolute ceiling on total runtime (hours). The watchdog starts intervening at
# INK_INIT_HOURS and keeps an active session alive, but never past INK_MAX_HOURS.
# 0 (default) = no ceiling: renew indefinitely while active. Set per-app at
# submit time from jobtype.<app>.htc.max_hours (e.g. 168 = 7 days for the
# standard interactive apps).
export INK_MAX_HOURS="${INK_MAX_HOURS:-0}"
# How often (hours) the watchdog renews the forwarded Kerberos TGT / AFS tokens.
# Deliberately decoupled from INK_CHECK_INTERVAL: the ticket lifetime is 48h at
# IHEP, so 6h leaves an 8x margin, while the liveness poll stays at 900s because
# it is cheap, local, and its interval sets the reaping latency and grace window.
export INK_KRB_RENEW_HOURS="${INK_KRB_RENEW_HOURS:-6}"
export INK_CHECK_INTERVAL="${INK_CHECK_INTERVAL:-900}"
export INK_ACTIVE_IDLE_SEC="${INK_ACTIVE_IDLE_SEC:-1800}"
# Per-core CPU-usage percentage threshold. Post-deadline "active" check for
# VNC-family apps (vnc / asic / asicbm) treats "any single process's %CPU >=
# this threshold within the past 5s window" as active.  Covers the case
# "user closed the browser but spectre/HSPICE is still running in the
# background" (see job 55256 postmortem).
export INK_ACTIVE_CPU_MIN_PCT="${INK_ACTIVE_CPU_MIN_PCT:-5}"

# -- hand off to the app's run.sh --
if [ ! -x "${APP_PATH}/run.sh" ]; then
    echo "[shell.sh] ${APP_PATH}/run.sh missing or not executable" >&2
    exit 127
fi

exec "${APP_PATH}/run.sh" "$@"
