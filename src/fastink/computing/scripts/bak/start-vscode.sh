#!/bin/bash


if [ -n "${INKPATH:-}" ] && [ -n "${INKLDPATH:-}" ]; then
    export PATH="$INKPATH:$PATH"
    export APPTAINERENV_PATH="$INKPATH"
    export LD_LIBRARY_PATH=$INKLDPATH
    export APPTAINERENV_LD_LIBRARY_PATH=$INKLDPATH
fi


function get_free_port() {
    while true; do
        PORT=$(shuf -i 49152-65535 -n 1)
        if ! /usr/sbin/ss -ltn | grep -q ":$PORT\b"; then
            echo $PORT
            break
        fi
    done
}

export APP_RUN_HOST="`/bin/hostname | /bin/awk -F '.' '{print $1}'`"

# set the vscode env
APP_PASSWD=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
APP_PORT=$(get_free_port)
APP_PATH="`/bin/pwd`"
APP_LOGIN_INFO="app_login.info"
APP_CONFIG_FILE="config.yaml"
export KRB5CCNAME="${APP_PATH}/krb5cc_${UID}"
/usr/bin/aklog

(
umask 077
cat > "${APP_CONFIG_FILE}" << EOL
bind-addr: 0.0.0.0:${APP_PORT}
auth: password
password: ${APP_PASSWD}
EOL
)

/bin/echo "{\"HOST\": \"${APP_RUN_HOST}\", \"PORT\": \"${APP_PORT}\", \"PASSWD\": \"${APP_PASSWD}\"}" > ${APP_LOGIN_INFO}
/usr/bin/code-server --config ${APP_PATH}/${APP_CONFIG_FILE} 
