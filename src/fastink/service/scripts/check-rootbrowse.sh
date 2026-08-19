#! /bin/bash
# FileName      : check-rootbrowse.sh
# Author        : HAN Xiao
# Email         : hanx@ihep.ac.cn
# Date          : Mon Jun 16 15:32:22 2025 CST
# Last modified : Fri Nov 21 16:48:09 2025 CST
# Description   :

SESSION_NAME="$1"
APP_PORT="$2"
TMPFILE="$3"
FLAG_CONNECT=0
retries=0
max_retries=3600

while [ $retries -lt $max_retries ]; do
    # Check whether the connection is established
    # RB_port_check=$(lsof -i :${APP_PORT} | grep "ESTABLISHED")
    # ss check
    RB_port_check=$(ss -nt sport = :${APP_PORT} | grep "ESTAB")

    if [[ -n $RB_port_check && $FLAG_CONNECT -eq 0 ]]; then
        # If the connection status is ESTABLISHED and checked at first time
        echo -e "$(date +'[%Y-%d-%m %H:%M:%S]') connected: \c" >>"$TMPFILE"
        echo "$RB_port_check" | awk '{print $9}' >>"$TMPFILE"
        FLAG_CONNECT=1
    elif [[ ! -n $RB_port_check && $FLAG_CONNECT -eq 1 ]]; then
        # If the connection is NOT ESTABLISHED and established before
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

    # Check whether the screen session is still running with FLAG_CONNECT = 1
    if [[ $FLAG_CONNECT -eq 1 ]]; then
        if ! screen -list | grep -q "$SESSION_NAME"; then
            exit
        fi
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
