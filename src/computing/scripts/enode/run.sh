#!/bin/bash


APP_USER="`whoami`"
APP_RUN_HOST="`hostname`"
APP_PATH=${1}
APP_PORT=${2}
APP_LOGIN_INFO="ssh_login.info"
SSH_CONFIG_FILE="sshd_config"

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

