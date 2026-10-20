#!/bin/bash
#
# Per-app entrypoint invoked by ../../shell.sh after the shared setup.

APP_USER="`whoami`"
APP_RUN_HOST="`hostname`"
APP_PATH="${APP_PATH:-$(/bin/pwd)}"
APP_PORT="${APP_PORT:-$(get_free_port)}"
APP_LOGIN_INFO="ssh_login.info"
SSH_CONFIG_FILE="sshd_config"

SSH_CONFIG_DIR="$(realpath -m -- "${APP_PATH}/../../envs/sshd")"
mkdir -p "${SSH_CONFIG_DIR}"

(
  umask 077
  for t in rsa ecdsa ed25519; do
    key="${SSH_CONFIG_DIR}/ssh_host_${t}_key"
    [ -s "$key" ] || ssh-keygen -q -t "$t" -f "$key" -N ""
  done
)

(
umask 077
cat > "${SSH_CONFIG_FILE}" << EOL

HostKey ${SSH_CONFIG_DIR}/ssh_host_rsa_key
HostKey ${SSH_CONFIG_DIR}/ssh_host_ecdsa_key
HostKey ${SSH_CONFIG_DIR}/ssh_host_ed25519_key
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
/bin/echo "SSH server starting on host ${APP_RUN_HOST} port ${APP_PORT}."

DEADLINE=$(( $(date +%s) + INK_INIT_HOURS * 3600 ))
echo "[INK] enode init ${INK_INIT_HOURS}h, deadline: $(date -d @${DEADLINE} '+%F %T')"

# 看门狗：到期直接关闭 sshd，不做自动续期。
# INK_INIT_HOURS 由 shell.sh 从 job_time.walltime 注入（默认 24h）。
watchdog() {
    local app_pid=$1
    while kill -0 "$app_pid" 2>/dev/null; do
        now=$(date +%s)
        if (( now >= DEADLINE )); then
            echo "[INK] 已到期，正常关闭 sshd。"
            kill -TERM "$app_pid" 2>/dev/null
            break
        fi
        sleep "$INK_CHECK_INTERVAL"
    done
}

nohup /usr/sbin/sshd -D -f ${APP_PATH}/sshd_config > sshd.log 2>&1 &
SSHD_PID=$!

trap 'kill -TERM ${SSHD_PID} 2>/dev/null' INT TERM

watchdog "${SSHD_PID}" &
WATCHDOG_PID=$!

wait "${SSHD_PID}"
EXIT_CODE=$?
kill "${WATCHDOG_PID}" 2>/dev/null
exit ${EXIT_CODE}

