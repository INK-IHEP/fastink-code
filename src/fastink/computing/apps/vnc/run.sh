#!/bin/bash
#
# Per-app entrypoint invoked by ../../shell.sh after the shared setup.

# App-specific env (was scripts/vnc/shell.sh)
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
DEADLINE=$(( $(date +%s) + INK_INIT_HOURS * 3600 ))

# X idle 读取依赖 Python 标准库 ctypes + 系统库 libXss（无任何第三方包）。
# 关键：必须用"系统自带的 python"，不能用用户 PATH 里的 conda/venv，
# 否则用户环境差异会导致行为不一致。这里固定按系统绝对路径优先选择。
_pick_system_python() {
    local c
    for c in /usr/bin/python3 /usr/libexec/platform-python /bin/python3 /usr/bin/python; do
        [[ -x "$c" ]] && { echo "$c"; return 0; }
    done
    # 最后兜底：PATH 里的 python3（一般不会走到这）
    command -v python3 2>/dev/null || command -v python 2>/dev/null
}
INK_PYTHON="$(_pick_system_python)"

# 本会话实际 X DISPLAY（由 VNC 启动输出解析后填入）
INK_VNC_DISPLAY=""

# 读取 X server 输入空闲毫秒数（内联，无需独立 .py 文件，脚本自包含）。
# 通过 XScreenSaver 扩展（libXss）实现，仅反映真实键盘/鼠标输入。
# 成功打印整数毫秒；任何失败打印 -1。
_read_x_idle_ms() {
    "${INK_PYTHON}" - <<'PY' 2>/dev/null
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
dpy = x11.XOpenDisplay(None)            # 读 DISPLAY 环境变量
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

# =============================================================================
# Background-CPU activity signal (companion to X-idle).  Covers the case where
# the user launched a long spectre/HSPICE simulation in the VNC session, then
# closed the browser: X stops receiving keyboard/mouse events, is_active()
# used to report false, and the watchdog killed the still-productive job at
# the 24h deadline (see job 55256 postmortem).
#
# Preference chain:
#   1. cgroup v2 delta   -- Precise, this-job scope.  HTCondor slot and Slurm
#                           step both place the current process in a dedicated
#                           cgroup; both cross-user and same-user-cross-job
#                           isolation come for free.
#   2. pidstat -U <user> -- User scope.  Isolated from other users but shares
#                           accounting with this user's other concurrent jobs.
#                           Fallback when cgroup is unavailable (open-source
#                           deployments without cgroup, or when we ended up in
#                           the root cgroup).
#   3. Neither available -- Return "not active", degrading to the pre-MR
#                           X-idle-only behaviour.
#
# Threshold INK_ACTIVE_CPU_MIN_PCT (percent of one core) is exported by
# shell.sh with default 5.  For the cgroup path this translates to a delta
# threshold of 5s x 5% x 10000us-per-pct-second = 250,000us per 5-second
# window.
# =============================================================================

# Return the cgroup v2 path of the current process (form "/foo/bar"), or
# rc=1 if we cannot determine one, or we are in the root cgroup (meaning
# no job-scoped isolation exists).
_get_cgroup_v2_path() {
    local line path
    line=$(/bin/awk 'NR==1 && /^0::/' /proc/self/cgroup 2>/dev/null)
    [[ -n "$line" ]] || return 1
    path="${line#0::}"
    [[ -n "$path" && "$path" != "/" ]] || return 1
    printf '%s\n' "$path"
}

# Tri-state return: 0=active, 1=inactive, 2=cgroup unusable (caller must
# fall back to the next signal in the chain).
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
    # Sanity guard on threshold input: coerce non-positive-integer values
    # (negative / empty / non-numeric) to the default 5.  Prevents the
    # "watchdog never fires" failure mode when the env var is set to a
    # value that would compute a non-positive threshold, causing any
    # delta_us >= thresh_us to be true and the job to run forever.
    thresh_pct="${INK_ACTIVE_CPU_MIN_PCT:-5}"
    [[ "$thresh_pct" =~ ^[1-9][0-9]*$ ]] || thresh_pct=5
    thresh_us=$(( 5 * thresh_pct * 10000 ))
    (( delta_us >= thresh_us ))
}

# Fallback: use pidstat's 5-second sample, user-scoped.
_has_cpu_activity_pidstat() {
    command -v pidstat >/dev/null 2>&1 || return 1
    local user_name thresh
    user_name=$(/usr/bin/id -un)
    # Same sanity guard as _has_cpu_activity_cgroup.
    thresh="${INK_ACTIVE_CPU_MIN_PCT:-5}"
    [[ "$thresh" =~ ^[1-9][0-9]*$ ]] || thresh=5
    # Force English output with LC_ALL=C: sysstat localises the "Average:"
    # prefix (e.g. to a Chinese/French/etc. word under a non-C locale),
    # which would bypass the /^Average:/ regex below.
    # Use $(NF-2) instead of $8 to locate the %CPU column: sysstat 10.x
    # (CentOS 7) emits 9 columns (no %wait), while 11.1.1+ / 12.x emit 10
    # columns (with %wait).  The last three columns are always
    # "%CPU CPU Command", so %CPU = NF-2 is version-independent.
    LC_ALL=C pidstat -u -U "$user_name" 5 1 2>/dev/null | /bin/awk -v thresh="$thresh" '
        /^Average:/ && $(NF-2)+0 >= thresh+0 && $NF !~ /pidstat/ {
            found = 1
            exit
        }
        END { exit(!found) }
    '
}

# Public entry point: chain-of-responsibility, cgroup first, pidstat fallback.
_has_cpu_activity() {
    _has_cpu_activity_cgroup
    local rc=$?
    (( rc != 2 )) && return $rc
    _has_cpu_activity_pidstat
}

# vnc active-liveness decision (authoritative signal):
#   OR (
#     A. X server received real keyboard/mouse input (user is driving the GUI)
#     B. This job has recent background CPU activity (e.g. an ASIC simulation
#        running in the background)
#   )
# Either signal true -> keep running past deadline; both false -> shut down.
is_active() {
    # Signal A: X keyboard/mouse
    local idle_ms
    idle_ms=$(DISPLAY="${INK_VNC_DISPLAY}" _read_x_idle_ms)
    if [[ "$idle_ms" =~ ^[0-9]+$ ]] && (( idle_ms < INK_ACTIVE_IDLE_SEC * 1000 )); then
        return 0
    fi
    # Signal B: this job's background CPU work
    _has_cpu_activity
}

# === 定义清理函数 ===
cleanup() {
    if [ -n "${NOVNC_PID}" ]; then
        kill ${NOVNC_PID} 2>/dev/null
    fi
    # OTP watcher/gc backgrounded by otp_start_listener (see apps/shell.sh)
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

# 清理旧的 VNC 会话
EX_DPS=$(${VNC_CMD} -list 2> /dev/null | /bin/awk '/^:/ {print $1}')
if [ -n "${EX_DPS}" ]; then
    for disp in ${EX_DPS}; do
        ${VNC_CMD} -kill "$disp" &> /dev/null
    done
fi

/bin/rm -f "$HOME/.vnc/${HOSTNAME}"*.pid
/bin/rm -f "$HOME/.vnc/${HOSTNAME}"*.lock
/bin/rm -f "$HOME/.vnc/${HOSTNAME}"*.log
/bin/rm -f /tmp/.X*-lock /tmp/.X11-unix/X* 2>/dev/null

# === 1. 启动 VNC Server ===
VNC_START_OUTPUT=$(${VNC_CMD} -rfbport ${APP_PORT} -securitytypes OTP -otp 2>&1)
echo "${VNC_START_OUTPUT}"
INK_VNC_DISPLAY=$(echo "${VNC_START_OUTPUT}" \
    | /bin/grep -oP 'started on display \S+:\K[0-9]+' \
    | /bin/awk '{print ":" $1; exit}')
echo "[INK] VNC display: ${INK_VNC_DISPLAY}"
/bin/echo "{\"HOST\": \"${APP_RUN_HOST}\", \"PORT\": \"${LISTEN_PORT}\"}" > ${APP_LOGIN_INFO}

# Start the FS-based OTP listener now that the VNC session is up.  The
# helper is defined in apps/shell.sh and consumed by
# fastink.computing.apps._helpers.generate_userotp on the server side.
otp_start_listener

# === 2. 后台启动 noVNC ===
echo "Starting noVNC proxy on port ${LISTEN_PORT}..."
${NOVNC_CMD} --vnc ${HOST_FULL_NAME}:${APP_PORT} --listen ${LISTEN_PORT} >/dev/null 2>&1 &
NOVNC_PID=$!

echo "[INK] VNC 初始时长 ${INK_INIT_HOURS} 小时，初始截止：$(date -d @${DEADLINE} '+%F %T')"

# === 3. 监控循环（端口/进程存活 + 到期看护） ===
# 到期前：不干预。到期后转入看护模式：定期检测，只要用户仍在使用就保持运行，
# 一旦不再使用则立即关闭（不再固定延长时长）。
while true; do
    if ! kill -0 ${NOVNC_PID} 2>/dev/null; then
        echo "[INFO] noVNC proxy process ended unexpectedly."
        break
    fi
    if ! /usr/sbin/ss -ltn | grep -q ":${APP_PORT}\b"; then
        echo "[INFO] VNC Server port ${APP_PORT} is closed. VNC likely crashed or exited."
        break
    fi

    now=$(date +%s)
    if (( now >= DEADLINE )); then
        if is_active; then
            echo "[INK] 已到期但用户仍在使用，保持运行。"
        else
            echo "[INK] 已到期且用户不再使用，正常关闭 VNC。"
            break
        fi
    fi

    sleep "${INK_CHECK_INTERVAL}"
done

exit 0