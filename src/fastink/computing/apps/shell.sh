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

# -- job working dir --
APP_PATH="$(/bin/pwd)"
export APP_PATH

# -- kerberos / AFS --
if [ -f "${APP_PATH}/krb5cc_${UID}" ]; then
    export KRB5CCNAME="${APP_PATH}/krb5cc_${UID}"
fi
if command -v /usr/bin/aklog >/dev/null 2>&1 && klist -s 2>/dev/null; then
    /usr/bin/aklog
fi

# -- interactive-job watchdog defaults --
export INK_INIT_HOURS="${INK_INIT_HOURS:-24}"
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
