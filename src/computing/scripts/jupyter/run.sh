#!/bin/bash

APP_PORT=${1}
APP_CONFIG_FILE="jupyter_config.py"
APP_LOGIN_INFO="app_login.info"
APP_RUN_HOST="`/bin/hostname | /bin/awk -F '.' '{print $1}'`"
APP_TOKEN=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")

(
umask 077
cat > "${APP_CONFIG_FILE}" << EOL
c.ServerApp.ip = '0.0.0.0'
c.ServerApp.port = ${APP_PORT}
c.ServerApp.port_retries = 0
c.ServerApp.token = "${APP_TOKEN}"
c.ServerApp.open_browser = False
c.ServerApp.base_url = "/jupyter/${APP_RUN_HOST}/${APP_PORT}/"
c.ServerApp.allow_origin = '*'
c.ServerApp.disable_check_xsrf = True
EOL
)

/bin/echo "{\"HOST\": \"${APP_RUN_HOST}\", \"PORT\": \"${APP_PORT}\", \"TOKEN\": \"${APP_TOKEN}\"}" > ${APP_LOGIN_INFO}
unset PYTHONPATH

${JUPYTER_HOME}/bin/jupyter-lab --config=${APP_CONFIG_FILE} --notebook-dir=~ 2>&1