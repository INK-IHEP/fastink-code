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

# Configure HERD Event Display environment
APP_PORT=$(get_free_port)
APP_PATH="`/bin/pwd`"
if [ -f "${APP_PATH}/krb5cc_${UID}" ]; then
  export KRB5CCNAME="${APP_PATH}/krb5cc_${UID}"
fi

if command -v /usr/bin/aklog >/dev/null 2>&1 && klist -s 2>/dev/null; then
  /usr/bin/aklog
fi

export HERD_BIN="/herdfs/user/quzy/public/runink.sh"
export HERD_XML="compact/v2024b/v2024b-test.xml"
export HERD_ROOT="/herdfs/user/quzy/public/demo/compact_electron_30.root"

${APP_PATH}/run.sh ${APP_PORT}  ${HERD_XML}  ${HERD_ROOT}
