#!/bin/bash

function get_free_port() {
    while true; do
        PORT=$(shuf -i 49152-65535 -n 1)
        if ! /usr/sbin/ss -ltn | grep -q ":$PORT\b"; then
            echo $PORT
            break
        fi
    done
}

# set the jupyter env
APP_PORT=$(get_free_port)
APP_PATH="`/bin/pwd`"
if [ -f "${APP_PATH}/krb5cc_${UID}" ]; then
  export KRB5CCNAME="${APP_PATH}/krb5cc_${UID}"
fi

if command -v /usr/bin/aklog >/dev/null 2>&1 && klist -s 2>/dev/null; then
  /usr/bin/aklog
fi

./run.sh ${APP_PORT}

