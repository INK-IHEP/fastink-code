#!/bin/bash
set -euo pipefail

FASTINK_ENABLE_NGINX=${FASTINK_ENABLE_NGINX:-false}
FASTINK_ENABLE_XROOTD=${FASTINK_ENABLE_XROOTD:-false}
FASTINK_ENABLE_LOCAL_HTCONDOR=${FASTINK_ENABLE_LOCAL_HTCONDOR:-false}
FASTINK_ENABLE_HOST_SLURM_CLIENT=${FASTINK_ENABLE_HOST_SLURM_CLIENT:-false}
FASTINK_HOST_NAME=${FASTINK_HOST_NAME:-localhost}

# Shared account-view normalization (also used by sync-accounts.sh).
source "${FASTINK_ACCOUNTS_LIB:-/usr/local/lib/fastink-accounts.sh}"

ETC_INIT_DIR=/work/etc-init

copy_if_missing() {
  local source_path="$1"
  local target_path="$2"
  if [[ ! -s "$target_path" ]]; then
    local tmp_file
    tmp_file="$(mktemp "$(dirname "$target_path")/.accounts-copy.XXXXXX")"
    cp "$source_path" "$tmp_file"
    chmod --reference="$source_path" "$tmp_file" 2>/dev/null || true
    mv -f "$tmp_file" "$target_path"
  fi
}

mkdir -p /work/etc-init
# The accounts-sync container writes the same directory on a running stack;
# serialize both writers on a shared lock file. The lock file lives inside
# the volume but is ignored by the account-view hashes (only the four
# account files are hashed).
exec 9>"$ETC_INIT_DIR/.accounts.lock"
flock -w 60 9 || { echo "Could not lock $ETC_INIT_DIR/.accounts.lock" >&2; exit 1; }

copy_if_missing /host-etc/passwd /work/etc-init/passwd
copy_if_missing /host-etc/group /work/etc-init/group
copy_if_missing /host-etc/shadow /work/etc-init/shadow
copy_if_missing /host-etc/gshadow /work/etc-init/gshadow

apply_fixed_service_accounts /work/etc-init

chmod 644 /work/etc-init/passwd /work/etc-init/group
chmod 600 /work/etc-init/shadow /work/etc-init/gshadow

flock -u 9
exec 9>&-

mkdir -p /work/keys/ssh-client
PRIVATE_KEY=/work/keys/ssh-client/id_rsa
PUBLIC_KEY=/work/keys/ssh-client/id_rsa.pub
AUTHORIZED_KEYS=/work/keys/rootbrowse_authorized_keys

if [[ -f "$PRIVATE_KEY" && ! -s "$PUBLIC_KEY" ]]; then
  ssh-keygen -y -f "$PRIVATE_KEY" > "$PUBLIC_KEY"
elif [[ ! -e "$PRIVATE_KEY" && ! -e "$PUBLIC_KEY" ]]; then
  ssh-keygen -q -t rsa -b 4096 -N '' -f "$PRIVATE_KEY" >/dev/null 2>&1
elif [[ -s "$PUBLIC_KEY" && ! -e "$PRIVATE_KEY" ]]; then
  echo "SSH private key not found: $PRIVATE_KEY" >&2
  exit 1
fi

chmod 600 "$PRIVATE_KEY"
chmod 644 "$PUBLIC_KEY"

if [[ ! -s "$AUTHORIZED_KEYS" ]]; then
  cp "$PUBLIC_KEY" "$AUTHORIZED_KEYS"
fi
chmod 600 "$AUTHORIZED_KEYS"

if [[ "$FASTINK_ENABLE_NGINX" == "true" ]]; then
  mkdir -p /work/nginx
  CERT_PATH=/work/nginx/cert.pem
  KEY_PATH=/work/nginx/key.pem
  if [[ ! -s "$CERT_PATH" || ! -s "$KEY_PATH" ]]; then
    openssl req \
      -x509 \
      -nodes \
      -newkey rsa:2048 \
      -sha256 \
      -days 3650 \
      -keyout "$KEY_PATH" \
      -out "$CERT_PATH" \
      -subj "/CN=${FASTINK_HOST_NAME}" >/dev/null 2>&1
  fi
  chmod 644 "$CERT_PATH"
  chmod 600 "$KEY_PATH"
fi

if [[ "$FASTINK_ENABLE_XROOTD" == "true" ]]; then
  mkdir -p /work/xrootd
  SSS_KEYTAB=/work/xrootd/sss.keytab
  KRB5_KEYTAB=/work/xrootd/krb5.keytab
  if [[ -e "$SSS_KEYTAB" && ! -s "$SSS_KEYTAB" ]]; then
    rm -f "$SSS_KEYTAB"
  fi
  if [[ ! -s "$SSS_KEYTAB" ]]; then
    umask 077
    printf "y\n" | xrdsssadmin -g anygroup -u anybody -k fastink+ add "$SSS_KEYTAB" >/dev/null 2>&1
  fi
  if [[ ! -e "$KRB5_KEYTAB" ]]; then
    : > "$KRB5_KEYTAB"
  fi
  if [[ -s "$SSS_KEYTAB" ]]; then
    chown 379:978 "$SSS_KEYTAB"
    chmod 400 "$SSS_KEYTAB"
  else
    chmod 600 "$SSS_KEYTAB"
  fi
  chown 379:978 "$KRB5_KEYTAB"
  chmod 600 "$KRB5_KEYTAB"
fi
