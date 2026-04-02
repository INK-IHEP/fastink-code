#!/bin/bash
set -euo pipefail

FASTINK_ENABLE_NGINX=${FASTINK_ENABLE_NGINX:-false}
FASTINK_ENABLE_XROOTD=${FASTINK_ENABLE_XROOTD:-false}
FASTINK_HOST_NAME=${FASTINK_HOST_NAME:-localhost}

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
  if [[ ! -s "$SSS_KEYTAB" ]]; then
    printf "y\n" | xrdsssadmin -g anygroup -u anybody -k fastink+ add "$SSS_KEYTAB" >/dev/null 2>&1
  fi
  if [[ ! -e "$KRB5_KEYTAB" ]]; then
    : > "$KRB5_KEYTAB"
  fi
  if [[ -s "$SSS_KEYTAB" ]]; then
    chmod 400 "$SSS_KEYTAB"
  else
    chmod 600 "$SSS_KEYTAB"
  fi
  chmod 600 "$KRB5_KEYTAB"
fi
