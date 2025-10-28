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


APP_PORT=$(get_free_port)
APP_PATH="`/bin/pwd`"
if [ -f "${APP_PATH}/krb5cc_${UID}" ]; then
  export KRB5CCNAME="${APP_PATH}/krb5cc_${UID}"
  /usr/bin/aklog
fi

./run.sh ${APP_PATH} ${APP_PORT}
