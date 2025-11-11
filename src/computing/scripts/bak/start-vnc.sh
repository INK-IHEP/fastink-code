#!/bin/bash

unset DBUS_SESSION_BUS_ADDRESS
unset DEBUGINFOD_URLS
export XAUTHORITY="$HOME/.Xauthority"

function get_free_port() {
    while true; do
        PORT=$(shuf -i 49152-65535 -n 1)
        if ! /usr/sbin/ss -ltn | grep -q ":$PORT\b"; then
            echo $PORT
            break
        fi
    done
}

mkdir RUN_TMP
chmod 700 RUN_TMP
export XDG_RUNTIME_DIR=$(/bin/realpath RUN_TMP)

APP_PORT=$(get_free_port)
LISTEN_PORT=$(get_free_port)
APP_LOGIN_INFO="app_login.info"
VNC_CMD="/opt/TurboVNC/bin/vncserver"
APP_RUN_HOST=$(/bin/hostname | /bin/awk -F '.' '{print $1}')

APP_PATH="`/bin/pwd`"
export KRB5CCNAME="${APP_PATH}/krb5cc_${UID}"
/usr/bin/aklog


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

${VNC_CMD} -rfbport ${APP_PORT} -securitytypes OTP -otp 
/bin/echo "{\"HOST\": \"${APP_RUN_HOST}\", \"PORT\": \"${LISTEN_PORT}\"}" > ${APP_LOGIN_INFO}

if grep -q "CentOS Linux release 7.9" /etc/redhat-release; then
    export PATH=/cvmfs/slurm.ihep.ac.cn/centos7.9/anaconda3/envs/ink/bin:$PATH
    export LD_LIBRARY_PATH=/cvmfs/slurm.ihep.ac.cn/centos7.9/anaconda3/envs/ink/lib:$LD_LIBRARY_PATH
fi

/cvmfs/common.ihep.ac.cn/software/noVNC-master/utils/novnc_proxy --vnc ${APP_RUN_HOST}.ihep.ac.cn:${APP_PORT} --listen ${LISTEN_PORT} 