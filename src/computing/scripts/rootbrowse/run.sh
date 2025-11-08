#! /bin/bash
# FileName      : run.sh
# Author        : HAN Xiao
# Email         : hanx@ihep.ac.cn
# Date          : Wed Dec 25 18:23:29 2024 CST
# Last modified : Thu Oct 16 17:48:52 2025 CST
# Description   : This script automates the setup and management of a ROOT RBrowser
#                 session using a dynamically assigned port, running in a detached
#                 screen session, and making it accessible through an Nginx proxy.

START_TIME=$(date +%s)

# function: transfer url from localhost to Nginx host
function trans_url() {
    echo "$1" | sed "s/http/https/g" |
        sed "s/localhost:/${NGINX}\/rootbrowse\/${APP_RUN_HOST}\//g" |
        sed "s/^New web window: //" | sed "s/^(std::string) //" | sed "s/\"//g"
}

# Get short host name
export APP_RUN_HOST="$(/bin/hostname -s)"
# Set login information path
APP_LOGIN_INFO="app_login.info"
# Get a free port
APP_PORT=$1
# Create a session name
SESSION_NAME="rb-"$(uuidgen)
# Set tmp dir path
if [ -z "$TMP" ]; then TMP=$(pwd); fi
TMPFILE="${TMP}/${SESSION_NAME}"
# Set Nginx host
NGINX="ink.ihep.ac.cn:443"

# Command to load the ROOT environment and start rootbrowse in ROOT interactive mode
CMD_SOURCE='source /cvmfs/sft.cern.ch/lcg/app/releases/ROOT/6.32.06/x86_64-almalinux9.4-gcc114-opt/bin/thisroot.sh'
CMD_ROOT="'ROOT::RWebWindowsManager::SetLoopbackMode(false);gROOT->SetWebDisplay(\"server:$APP_PORT\");auto browser = new ROOT::RBrowser();'"
# Remove zombie screen sessions
screen -wipe >/dev/null 2>&1 || true
# Start the ROOT command and redirect the output to a file
echo "Start rootbrowse in screen session ${SESSION_NAME} in ${APP_RUN_HOST}"
screen -dmS "$SESSION_NAME" bash -c "$CMD_SOURCE; date; cd ~; root -l -b -e $CMD_ROOT &> $TMPFILE"

# Check SESSION
while ! screen -list | grep -q "$SESSION_NAME"; do
    sleep 1
    screen_retries=$((screen_retries + 1))
    if [ $screen_retries -eq 30 ]; then
        echo "Failed to create Session $SESSION_NAME."
        exit
    fi
done
echo "Screen session ${SESSION_NAME} started."

max_retries=120
retries=0

while [ $retries -lt $max_retries ]; do
    if grep -q "New web window" "$TMPFILE"; then
        URL=$(trans_url "$(grep 'New web window' $TMPFILE | tail -n 1)")
        APP_TOKEN=$(echo "$URL" | grep -oP '(?<=key=)[^&]*')

        if [ -n "$URL" ]; then
            echo "{\"HOST\": \"${APP_RUN_HOST}\", \"PORT\": \"${APP_PORT}\", \"TOKEN\": \"${APP_TOKEN}\"}" >${APP_LOGIN_INFO}
            END_TIME=$(date +%s)
            echo "Elapsed time: $((END_TIME - START_TIME)) seconds."
            FLAG_CONNECT=0
            break
        fi
    else
        retries=$((retries + 1))
    fi
    sleep 1
done

if [ $retries -eq $max_retries ]; then
    echo "Failed to capture URL after $max_retries attempts."
    echo "Captured output($TMPFILE):"
    cat "$TMPFILE"
    exit
fi

# Check every second whether the connection is established;
# if it is established and then disconnected, refresh the URL.
while true; do
    sleep 1
    RB_port_check=$(lsof -i :${APP_PORT} | grep "ESTABLISHED")

    if [[ -n $RB_port_check && $FLAG_CONNECT -eq 0 ]]; then
        echo "$(date) connected."
        echo "$RB_port_check" | awk '{print $9}'
        FLAG_CONNECT=1
    elif [[ ! -n $RB_port_check && $FLAG_CONNECT -eq 1 ]]; then
        echo -e "$(date) disconnected... \c"
        FLAG_CONNECT=0
        sleep 0.5
        screen -S "$SESSION_NAME" -X stuff "browser->Show()\n"
        sleep 1
        #URL=$(trans_url "$(grep '(std::string)' $TMPFILE | tail -n 1)")
        URL=$(trans_url "$(grep 'New web window' $TMPFILE | tail -n 1)")
        APP_TOKEN=$(echo "$URL" | grep -oP '(?<=key=)[^&]*')
        echo "{\"HOST\": \"${APP_RUN_HOST}\", \"PORT\": \"${APP_PORT}\", \"TOKEN\": \"${APP_TOKEN}\"}" >${APP_LOGIN_INFO}
        echo "URL updated."
    fi
done
