#!/bin/bash
set -euo pipefail

ROOTBROWSE_PORT="${ROOTBROWSE_PORT:-2000}"
AUTHORIZED_KEYS_SOURCE="${AUTHORIZED_KEYS_SOURCE:-}"
PRELOAD_SCRIPTS="${PRELOAD_SCRIPTS:-}"
PRELOAD_SCRIPT_DIRS="${PRELOAD_SCRIPT_DIRS:-}"

run_preload_script() {
    local script="$1"
    if [[ ! -f "$script" ]]; then
        echo "Preload script not found: $script" >&2
        exit 1
    fi
    echo "Running preload script: $script"
    bash "$script"
}

run_preload_scripts() {
    local scripts="$1"
    local directories="$2"
    local directory
    local script

    if [[ -n "$directories" ]]; then
        IFS=',' read -ra dir_list <<< "$directories"
        for directory in "${dir_list[@]}"; do
            directory="${directory#"${directory%%[![:space:]]*}"}"
            directory="${directory%"${directory##*[![:space:]]}"}"
            if [[ -z "$directory" ]]; then
                continue
            fi
            if [[ ! -d "$directory" ]]; then
                echo "Preload script directory not found: $directory" >&2
                exit 1
            fi
            while IFS= read -r script; do
                run_preload_script "$script"
            done < <(find "$directory" -maxdepth 1 -type f ! -name '.*' | sort)
        done
    fi

    if [[ -n "$scripts" ]]; then
        IFS=',' read -ra script_list <<< "$scripts"
        for script in "${script_list[@]}"; do
            script="${script#"${script%%[![:space:]]*}"}"
            script="${script%"${script##*[![:space:]]}"}"
            if [[ -z "$script" ]]; then
                continue
            fi
            run_preload_script "$script"
        done
    fi
}

mkdir -p /run/sshd /root/.ssh
chmod 700 /root/.ssh

run_preload_scripts "$PRELOAD_SCRIPTS" "$PRELOAD_SCRIPT_DIRS"

ssh-keygen -A

if [[ -n "$AUTHORIZED_KEYS_SOURCE" ]]; then
    if [[ ! -f "$AUTHORIZED_KEYS_SOURCE" ]]; then
        echo "Authorized keys source not found: $AUTHORIZED_KEYS_SOURCE" >&2
        exit 1
    fi
    cp "$AUTHORIZED_KEYS_SOURCE" /root/.ssh/authorized_keys
    chmod 600 /root/.ssh/authorized_keys
fi

sed -i \
    -e "s/^#\?Port .*/Port ${ROOTBROWSE_PORT}/g" \
    -e 's/^#\?PermitRootLogin .*/PermitRootLogin yes/' \
    -e 's/^#\?PasswordAuthentication .*/PasswordAuthentication no/' \
    /etc/ssh/sshd_config

if ! grep -q '^AllowUsers root$' /etc/ssh/sshd_config; then
    echo 'AllowUsers root' >> /etc/ssh/sshd_config
fi

exec /usr/sbin/sshd -D -e
