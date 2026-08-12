#!/bin/bash
set -euo pipefail

FASTINK_ENABLE_NGINX=${FASTINK_ENABLE_NGINX:-false}
FASTINK_ENABLE_XROOTD=${FASTINK_ENABLE_XROOTD:-false}
FASTINK_ENABLE_LOCAL_HTCONDOR=${FASTINK_ENABLE_LOCAL_HTCONDOR:-false}
FASTINK_HOST_NAME=${FASTINK_HOST_NAME:-localhost}

copy_if_missing() {
  local source_path="$1"
  local target_path="$2"
  if [[ ! -s "$target_path" ]]; then
    cp "$source_path" "$target_path"
  fi
}

ensure_named_entry() {
  local entry_name="$1"
  local source_path="$2"
  local target_path="$3"
  if grep -q "^${entry_name}:" "$target_path"; then
    return
  fi
  local line
  line="$(grep "^${entry_name}:" "$source_path" || true)"
  if [[ -n "$line" ]]; then
    printf '%s\n' "$line" >> "$target_path"
  fi
}

upsert_named_entry() {
  local entry_name="$1"
  local target_path="$2"
  local replacement_line="$3"
  local tmp_file
  tmp_file="$(mktemp)"
  awk -F: -v entry_name="$entry_name" -v replacement_line="$replacement_line" '
    BEGIN { replaced = 0 }
    $1 == entry_name {
      if (!replaced) {
        print replacement_line
        replaced = 1
      }
      next
    }
    { print }
    END {
      if (!replaced) {
        print replacement_line
      }
    }
  ' "$target_path" > "$tmp_file"
  cat "$tmp_file" > "$target_path"
  rm -f "$tmp_file"
}

assert_fixed_xrootd_identity_is_safe() {
  local passwd_file="$1"
  local group_file="$2"
  local uid_owner
  local gid_owner

  uid_owner="$(awk -F: '$3 == "379" { print $1; exit }' "$passwd_file")"
  gid_owner="$(awk -F: '$3 == "978" { print $1; exit }' "$group_file")"

  if [[ -n "$uid_owner" && "$uid_owner" != "xrootd" ]]; then
    echo "Conflicting host account state: uid 379 is already owned by '$uid_owner', not 'xrootd'." >&2
    echo "FastINK generic xrootd currently requires fixed uid/gid 379:978. Resolve the host conflict before continuing." >&2
    exit 1
  fi

  if [[ -n "$gid_owner" && "$gid_owner" != "xrootd" ]]; then
    echo "Conflicting host account state: gid 978 is already owned by '$gid_owner', not 'xrootd'." >&2
    echo "FastINK generic xrootd currently requires fixed uid/gid 379:978. Resolve the host conflict before continuing." >&2
    exit 1
  fi
}

warn_if_xrootd_identity_will_be_normalized() {
  local passwd_file="$1"
  local group_file="$2"
  local xrootd_passwd_ids
  local xrootd_group_id

  xrootd_passwd_ids="$(awk -F: '$1 == "xrootd" { print $3 ":" $4; exit }' "$passwd_file")"
  xrootd_group_id="$(awk -F: '$1 == "xrootd" { print $3; exit }' "$group_file")"

  if [[ -n "$xrootd_passwd_ids" && "$xrootd_passwd_ids" != "379:978" ]]; then
    echo "WARNING: Existing host xrootd user identity '$xrootd_passwd_ids' will be normalized to '379:978' inside FastINK etc-init." >&2
  fi

  if [[ -n "$xrootd_group_id" && "$xrootd_group_id" != "978" ]]; then
    echo "WARNING: Existing host xrootd group gid '$xrootd_group_id' will be normalized to '978' inside FastINK etc-init." >&2
  fi
}

assert_fixed_condor_identity_is_safe() {
  local passwd_file="$1"
  local group_file="$2"
  local uid_owner
  local gid_owner

  uid_owner="$(awk -F: '$3 == "64" { print $1; exit }' "$passwd_file")"
  gid_owner="$(awk -F: '$3 == "64" { print $1; exit }' "$group_file")"

  if [[ -n "$uid_owner" && "$uid_owner" != "condor" ]]; then
    echo "Conflicting host account state: uid 64 is already owned by '$uid_owner', not 'condor'." >&2
    echo "FastINK local HTCondor currently requires fixed uid/gid 64:64. Resolve the host conflict before continuing." >&2
    exit 1
  fi

  if [[ -n "$gid_owner" && "$gid_owner" != "condor" ]]; then
    echo "Conflicting host account state: gid 64 is already owned by '$gid_owner', not 'condor'." >&2
    echo "FastINK local HTCondor currently requires fixed uid/gid 64:64. Resolve the host conflict before continuing." >&2
    exit 1
  fi
}

warn_if_condor_identity_will_be_normalized() {
  local passwd_file="$1"
  local group_file="$2"
  local condor_passwd_ids
  local condor_group_id

  condor_passwd_ids="$(awk -F: '$1 == "condor" { print $3 ":" $4; exit }' "$passwd_file")"
  condor_group_id="$(awk -F: '$1 == "condor" { print $3; exit }' "$group_file")"

  if [[ -n "$condor_passwd_ids" && "$condor_passwd_ids" != "64:64" ]]; then
    echo "WARNING: Existing host condor user identity '$condor_passwd_ids' will be normalized to '64:64' inside FastINK etc-init." >&2
  fi

  if [[ -n "$condor_group_id" && "$condor_group_id" != "64" ]]; then
    echo "WARNING: Existing host condor group gid '$condor_group_id' will be normalized to '64' inside FastINK etc-init." >&2
  fi
}

mkdir -p /work/etc-init
copy_if_missing /host-etc/passwd /work/etc-init/passwd
copy_if_missing /host-etc/group /work/etc-init/group
copy_if_missing /host-etc/shadow /work/etc-init/shadow
copy_if_missing /host-etc/gshadow /work/etc-init/gshadow

upsert_named_entry sshd /work/etc-init/passwd "sshd:x:74:74:Privilege-separated SSH:/usr/share/empty.sshd:/usr/sbin/nologin"
upsert_named_entry sshd /work/etc-init/group "sshd:x:74:"
upsert_named_entry sshd /work/etc-init/shadow "sshd:!!:20545::::::"
upsert_named_entry sshd /work/etc-init/gshadow "sshd:!::"

assert_fixed_xrootd_identity_is_safe /work/etc-init/passwd /work/etc-init/group
warn_if_xrootd_identity_will_be_normalized /work/etc-init/passwd /work/etc-init/group
upsert_named_entry xrootd /work/etc-init/passwd "xrootd:x:379:978:XRootD runtime user:/var/spool/xrootd:/sbin/nologin"
upsert_named_entry xrootd /work/etc-init/group "xrootd:x:978:"
upsert_named_entry xrootd /work/etc-init/shadow "xrootd:!!:20460::::::"
upsert_named_entry xrootd /work/etc-init/gshadow "xrootd:!::"

if [[ "$FASTINK_ENABLE_LOCAL_HTCONDOR" == "true" ]]; then
  assert_fixed_condor_identity_is_safe /work/etc-init/passwd /work/etc-init/group
  warn_if_condor_identity_will_be_normalized /work/etc-init/passwd /work/etc-init/group
  upsert_named_entry condor /work/etc-init/passwd "condor:x:64:64:Owner of HTCondor Daemons:/var/lib/condor:/sbin/nologin"
  upsert_named_entry condor /work/etc-init/group "condor:x:64:"
  upsert_named_entry condor /work/etc-init/shadow "condor:!!:20460::::::"
  upsert_named_entry condor /work/etc-init/gshadow "condor:!::"
fi

chmod 644 /work/etc-init/passwd /work/etc-init/group
chmod 600 /work/etc-init/shadow /work/etc-init/gshadow

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
