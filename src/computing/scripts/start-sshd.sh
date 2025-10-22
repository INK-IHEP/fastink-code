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


SSH_CONFIG_FILE="sshd_config"
APP_USER="`whoami`"
APP_RUN_HOST="`hostname`"
APP_PORT=$(get_free_port)
APP_LOGIN_INFO="ssh_login.info"
APP_PATH="`/bin/pwd`"
export KRB5CCNAME="${APP_PATH}/krb5cc_${UID}"
/usr/bin/aklog


ssh-keygen -t rsa -f ssh_host_rsa_key -N ""
ssh-keygen -t ecdsa -f ssh_host_ecdsa_key -N ""
ssh-keygen -t ed25519 -f ssh_host_ed25519_key -N ""


(
umask 077
cat > "${SSH_CONFIG_FILE}" << EOL

HostKey ${APP_PATH}/ssh_host_rsa_key
HostKey ${APP_PATH}/ssh_host_ecdsa_key
HostKey ${APP_PATH}/ssh_host_ed25519_key

Port ${APP_PORT}

SyslogFacility AUTHPRIV
PermitRootLogin yes
PubkeyAuthentication yes
AuthorizedKeysFile      .ssh/authorized_keys
PasswordAuthentication yes
ChallengeResponseAuthentication no
KerberosAuthentication yes
GSSAPIAuthentication yes
GSSAPICleanupCredentials no
X11Forwarding yes
PrintMotd yes
AcceptEnv LANG LC_CTYPE LC_NUMERIC LC_TIME LC_COLLATE LC_MONETARY LC_MESSAGES
AcceptEnv LC_PAPER LC_NAME LC_ADDRESS LC_TELEPHONE LC_MEASUREMENT
AcceptEnv LC_IDENTIFICATION LC_ALL LANGUAGE
AcceptEnv XMODIFIERS
Subsystem       sftp    /usr/libexec/openssh/sftp-server
AllowUsers ${APP_USER}

EOL
)

/bin/echo "{\"HOST\": \"${APP_RUN_HOST}\", \"PORT\": \"${APP_PORT}\"}" > ${APP_LOGIN_INFO}

nohup /usr/sbin/sshd -D -f ${APP_PATH}/sshd_config > sshd.log 2>&1 &

SSHD_PID=$!
wait $SSHD_PID

