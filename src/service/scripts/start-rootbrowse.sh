#! /bin/bash
# FileName      : start-rootbrowse.sh
# Author        : HAN Xiao
# Email         : hanx@ihep.ac.cn
# Date          : Wed Dec 25 18:23:29 2024 CST
# Last modified : Thu Oct 16 18:56:07 2025 CST
# Description   : This script automates the setup and management of a ROOT RBrowser
#                 session using a dynamically assigned port, running in a detached
#                 screen session, and making it accessible through an Nginx proxy.

START_TIME=$(date +%s%3N)

if [ "$KRB5CCNAME" ]; then aklog; fi

# function：Get a free port
function get_free_port() {
    while true; do
        PORT=$(shuf -i 49152-65535 -n 1)
        if ! ss -ltn | grep -q ":$PORT\b"; then
            echo $PORT
            break
        fi
    done
}

# function: transfer url from localhost to Nginx host
function trans_url() {
    echo "$1" | sed "s/http/https/g" |
        sed "s/localhost:/${NGINX}\/rootbrowse\/${APP_RUN_HOST}\//g" |
        sed "s/^New web window: //" | sed "s/^(std::string) //" | sed "s/\"//g"
}

# Get short host name
export APP_RUN_HOST="$(/bin/hostname -s)"
# Get a free port
APP_PORT=$(get_free_port)
# Create a session name
SESSION_NAME="rb-"$(date +%s)"-"$(uuidgen)
# Set tmp dir path
TMP="/tmp/rootbrowse-$USER/$(date +%Y-%m-%d)/"
mkdir -p $TMP
TMPFILE="${TMP}/${SESSION_NAME}"
# Set Nginx host. It will be replaced by fastink. No need to change
NGINX="ink.ihep.ac.cn"

echo "$(date +'[%Y-%d-%m %H:%M:%S]') $0 $@" >>"$TMPFILE"

if [ -f "$1" ] && [[ "$1" =~ \.root$ ]]; then
    # Set root file path
    ROOT_PATH="$(dirname "$1")"
    ROOT_FILE="$(basename "$1")"
    echo "File $ROOT_FILE (path: $ROOT_PATH) exists and ends with .root" >$TMPFILE
else
    echo "File '$1' does not exist or cannot be accessed due to permission issues." >&2
    exit
fi

# Command to load the ROOT environment and start rootbrowse in ROOT interactive mode
# check kernel version
if [[ $(uname -r) == *"9_6"* ]]; then
    CMD_SOURCE='source /cvmfs/sft.cern.ch/lcg/app/releases/ROOT/6.36.02/x86_64-almalinux9.6-gcc115-opt/bin/thisroot.sh'
else
    CMD_SOURCE='source /cvmfs/sft.cern.ch/lcg/app/releases/ROOT/6.32.06/x86_64-almalinux9.4-gcc114-opt/bin/thisroot.sh'
fi
CMD_ROOT="'ROOT::RWebWindowsManager::SetLoopbackMode(false);gROOT->SetWebDisplay(\"server:$APP_PORT\");auto browser = new ROOT::RBrowser();'"
# Remove zombie screen sessions
screen -wipe >/dev/null 2>&1 || true
# Start the ROOT command and redirect the output to a file
echo "Start rootbrowse in screen session ${SESSION_NAME} in ${APP_RUN_HOST}" >>$TMPFILE
screen -dmS "$SESSION_NAME" bash -c "$CMD_SOURCE; date; cd \"$ROOT_PATH\"; root -l -b \"$ROOT_FILE\" -e $CMD_ROOT >> $TMPFILE 2>&1"

# Check SESSION
while ! screen -list | grep -q "$SESSION_NAME"; do
    sleep 0.1
    screen_retries=$((screen_retries + 1))
    if [ $screen_retries -eq 300 ]; then
        echo "Failed to create Session $SESSION_NAME." >&2
        exit
    fi
done
echo "Screen session ${SESSION_NAME} started." >>$TMPFILE

max_retries=1200
retries=0

while [ $retries -lt $max_retries ]; do
    if grep -q "New web window" "$TMPFILE"; then
        URL=$(trans_url "$(grep 'New web window' $TMPFILE | tail -n 1)")
        APP_TOKEN=$(echo "$URL" | grep -oP '(?<=key=)[^&]*')

        if [ -n "$URL" ]; then
            echo -e "$URL\c"
            echo $URL >>$TMPFILE
            END_TIME=$(date +%s%3N) >>$TMPFILE
            echo "Elapsed time: $((END_TIME - START_TIME)) ms." >>$TMPFILE
            FLAG_CONNECT=0
            break
        fi
    else
        retries=$((retries + 1))
    fi
    sleep 0.1
done

if [ $retries -eq $max_retries ]; then
    echo "Failed to capture URL after $max_retries attempts." >&2
    echo "Captured output($TMPFILE):" >&2
    cat "$TMPFILE" >&2
    exit
fi

# Check every second whether the connection is established;
# if it is established and then disconnected, refresh the URL.
# Disconnected in 3600 second.
nohup /dev/shm/check-rootbrowse.sh $SESSION_NAME $APP_PORT $TMPFILE >>$TMPFILE 2>&1 &

exit 0
