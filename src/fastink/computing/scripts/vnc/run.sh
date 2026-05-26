#!/bin/bash

APP_PORT=${1}
LISTEN_PORT=${2}

# === 定义清理函数 ===
cleanup() {
    # 如果 NOVNC_PID 变量存在且进程在运行，则杀掉它
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
${VNC_CMD} -rfbport ${APP_PORT} -securitytypes OTP -otp 2>&1
/bin/echo "{\"HOST\": \"${APP_RUN_HOST}\", \"PORT\": \"${LISTEN_PORT}\"}" > ${APP_LOGIN_INFO}

# === 2. 后台启动 noVNC ===
echo "Starting noVNC proxy on port ${LISTEN_PORT}..."
${NOVNC_CMD} --vnc ${HOST_FULL_NAME}:${APP_PORT} --listen ${LISTEN_PORT} >/dev/null 2>&1 &
NOVNC_PID=$!  # 获取 noVNC 的进程 ID

# === 3. 监控循环 ===
while true; do
    # 检查 noVNC 进程是否还活着
    if ! kill -0 ${NOVNC_PID} 2>/dev/null; then
        echo "[INFO] noVNC proxy process ended unexpectedly."
        break
    fi

    # 检查 VNC 端口是否还在监听
    # 使用 ss 命令查看 APP_PORT 是否处于 LISTEN 状态
    if ! /usr/sbin/ss -ltn | grep -q ":${APP_PORT}\b"; then
        echo "[INFO] VNC Server port ${APP_PORT} is closed. VNC likely crashed or exited."
        break
    fi

    sleep 30
done

exit 0