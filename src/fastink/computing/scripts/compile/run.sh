#!/bin/bash

APP_PATH=${1}
APP_PORT=${2}
APP_PASSWD=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
APP_CONFIG_FILE="config.yaml"
APP_LOGIN_INFO="app_login.info"
APP_RUN_HOST="`/bin/hostname | /bin/awk -F '.' '{print $1}'`"

(
umask 077
cat > "${APP_CONFIG_FILE}" << EOL
bind-addr: 0.0.0.0:${APP_PORT}
auth: password
password: ${APP_PASSWD}
EOL
)

/bin/echo "{\"HOST\": \"${APP_RUN_HOST}\", \"PORT\": \"${APP_PORT}\", \"PASSWD\": \"${APP_PASSWD}\"}" > ${APP_LOGIN_INFO}

${VSCODE_BIN} --config ${APP_PATH}/${APP_CONFIG_FILE} 2>&1