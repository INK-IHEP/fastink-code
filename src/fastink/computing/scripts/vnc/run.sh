#!/bin/bash

APP_PORT=${1}
LISTEN_PORT=${2}
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

# vnc 活跃判定（权威信号）：读取 X server 输入空闲时间。
# 只反映真实键盘/鼠标输入：人真的在操作 -> idle 很小；
# 浏览器挂着标签页但不动 -> idle 持续增长，绝不会被误判为活跃。
is_active() {
    local idle_ms
    idle_ms=$(DISPLAY="${INK_VNC_DISPLAY}" _read_x_idle_ms)
    # 读取失败（-1 或空）时保守处理：视为不活跃，不续期
    [[ "$idle_ms" =~ ^[0-9]+$ ]] || return 1
    (( idle_ms < INK_ACTIVE_IDLE_SEC * 1000 ))
}

# === 定义清理函数 ===
cleanup() {
    if [ -n "${NOVNC_PID}" ]; then
        kill ${NOVNC_PID} 2>/dev/null
    fi
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