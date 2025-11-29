#! /bin/bash
# FileName      : check-rootbrowse.sh
# Author        : HAN Xiao
# Email         : hanx@ihep.ac.cn
# Date          : Mon Jun 16 15:32:22 2025 CST
# Last modified : Mon Jun 16 16:00:43 2025 CST
# Description   :

SESSION_NAME="$1"
APP_PORT="$2"
TMPFILE="$3"
FLAG_CONNECT=0
retries=0
max_retries=3600

while [ $retries -lt $max_retries ]; do
    RB_port_check=$(lsof -i :${APP_PORT} | grep "ESTABLISHED")

    if [[ -n $RB_port_check && $FLAG_CONNECT -eq 0 ]]; then
        echo -e "$(date +'[%Y-%d-%m %H:%M:%S]') connected: \c" >>"$TMPFILE"
        echo "$RB_port_check" | awk '{print $9}' >>"$TMPFILE"
        FLAG_CONNECT=1
    elif [[ ! -n $RB_port_check && $FLAG_CONNECT -eq 1 ]]; then
        echo -e "$(date +'[%Y-%d-%m %H:%M:%S]') disconnected... \c" >>"$TMPFILE"
        sleep 0.5
        screen -S "$SESSION_NAME" -X stuff ".q\n"
        sleep 1
        if screen -list | grep -q "$SESSION_NAME"; then
            continue
        fi
        echo "closed." >>"$TMPFILE"
        exit
    fi

    if ! screen -list | grep -q "$SESSION_NAME"; then
        exit
    fi

    retries=$((retries + 1))
    sleep 1
done

if $(screen -list | grep -q "$SESSION_NAME"); then
    echo "closing $SESSION_NAME" >>"$TMPFILE"
    screen -S "$SESSION_NAME" -X stuff ".q\n"
    sleep 1
    echo "closed(timed out)." >>"$TMPFILE"
fi
