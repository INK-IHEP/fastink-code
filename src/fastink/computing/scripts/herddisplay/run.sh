#!/bin/bash

APP_PORT=${1}
HERD_ROOT=${2}
HERD_XML=${3}
HERD_BIN=${4}
APP_LOGIN_INFO="app_login.info"
APP_RUN_HOST="`/bin/hostname | /bin/awk -F '.' '{print $1}'`"
HERD_HTML="/tmp/app/testv4.html"

/bin/echo "{\"HOST\": \"${APP_RUN_HOST}\", \"PORT\": \"${APP_PORT}\", \"TOKEN\": \"\"  }" > ${APP_LOGIN_INFO}

patch_websocket_protocol() {
    for _ in $(seq 1 100); do
        if [ -f "${HERD_HTML}" ]; then
            /bin/sed -i \
                's|const wsUrl = `ws://${window.location.host}${API_BASE}/ws/web`;|const wsProtocol = window.location.protocol === "https:" ? "wss:" : "ws:";\nconst wsUrl = `${wsProtocol}//${window.location.host}${API_BASE}/ws/web`;|' \
                "${HERD_HTML}"
            /bin/echo "Patched HERD WebSocket protocol in ${HERD_HTML}"
            return 0
        fi
        /bin/sleep 0.1
    done

    /bin/echo "HERD HTML was not found at ${HERD_HTML}; WebSocket protocol was not patched" >&2
    return 1
}

patch_websocket_protocol &

/bin/bash ${HERD_BIN} ${APP_PORT} ${HERD_XML} ${HERD_ROOT} ${APP_RUN_HOST} 2>&1
