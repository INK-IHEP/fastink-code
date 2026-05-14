#!/bin/bash

APP_PORT=${1}
LISTEN_PORT=${2}

unset DBUS_SESSION_BUS_ADDRESS
unset DEBUGINFOD_URLS

mkdir RUN_TMP
chmod 700 RUN_TMP
export XDG_RUNTIME_DIR=$(/bin/realpath RUN_TMP)

if [[ -z "${HOME:-}" ]]; then
  export HOME="$(getent passwd "$(id -un)" | cut -d: -f6)"
fi
export XAUTHORITY="$HOME/.Xauthority"

APP_LOGIN_INFO="app_login.info"
APP_RUN_HOST=$(/bin/hostname | /bin/awk -F '.' '{print $1}')
HOST_FULL_NAME=$(/bin/hostname)

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

${VNC_CMD} -rfbport ${APP_PORT} -securitytypes OTP -otp 2>&1

/bin/echo "{\"HOST\": \"${APP_RUN_HOST}\", \"PORT\": \"${LISTEN_PORT}\"}" > ${APP_LOGIN_INFO}
${NOVNC_CMD} --vnc ${HOST_FULL_NAME}:${APP_PORT} --listen ${LISTEN_PORT} 2>&1