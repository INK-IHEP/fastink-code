#!/bin/bash
# Shared account-view helpers for the FastINK deploy images.
#
# The container account view (etc-init) is a copy of the host's
# passwd/group/shadow/gshadow with a few fixed service identities
# normalized on top (sshd, xrootd, optionally condor and slurm).
#
# Both the one-shot init container (init-assets.sh) and the recurring
# accounts-sync container (sync-accounts.sh) must produce exactly the same
# view, so the normalization logic lives here instead of being duplicated.

upsert_named_entry() {
  local entry_name="$1"
  local target_path="$2"
  local replacement_line="$3"
  local tmp_file
  # Keep the temporary file next to the target: same filesystem, so the
  # final mv is a real rename(2) rather than a cross-device copy+unlink.
  tmp_file="$(mktemp "$(dirname "$target_path")/.accounts-upsert.XXXXXX")"
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
  # Publish with a rename instead of truncating the target in place, so a
  # concurrent reader never sees a half-written account database. Keep the
  # target's mode; ownership stays root (the writer is root).
  if [[ -e "$target_path" ]]; then
    chmod --reference="$target_path" "$tmp_file" 2>/dev/null || true
  fi
  mv -f "$tmp_file" "$target_path"
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

# Resolve a host account file inside $FASTINK_HOST_ETC, following symlinks
# without escaping the mounted host directory. Absolute links into /etc are
# rewritten to the same path under $FASTINK_HOST_ETC (the directory mount
# keeps the host layout; an absolute /etc/... link would otherwise resolve
# against the container's own /etc).
resolve_host_account_file() {
  local name="${1:?account file name required}"
  local host_etc="${FASTINK_HOST_ETC:-/host-etc}"
  local path="$host_etc/$name"
  local link hops=0

  while [[ -L "$path" ]]; do
    hops=$((hops + 1))
    if (( hops > 10 )); then
      echo "[accounts-sync] too many symlink levels resolving $name" >&2
      return 1
    fi
    link="$(readlink "$path")"
    case "$link" in
      /etc/*) path="$host_etc/${link#/etc/}" ;;
      /*)
        echo "[accounts-sync] refusing symlink target outside /etc: $link" >&2
        return 1
        ;;
      *) path="$(dirname "$path")/$link" ;;
    esac
  done

  # Normalize lexically and make sure the final path stays inside the
  # mounted host directory (a relative link with '..' must not escape it).
  local root resolved
  root="$(realpath -m "$host_etc")"
  resolved="$(realpath -m "$path")"
  case "$resolved" in
    "$root"/*) ;;
    *)
      echo "[accounts-sync] refusing path outside $host_etc: $path" >&2
      return 1
      ;;
  esac
  # Empty account files are legal (e.g. an empty /etc/gshadow); only a
  # missing file is fatal.
  if [[ ! -e "$resolved" ]]; then
    echo "[accounts-sync] missing host file: $resolved" >&2
    return 1
  fi
  printf '%s\n' "$resolved"
}

# Normalize the fixed service identities in an account-view directory.
# Expects the four files (passwd, group, shadow, gshadow) to already exist
# under $etc_dir.
apply_fixed_service_accounts() {
  local etc_dir="${1:?account-view directory required}"
  local enable_condor="${FASTINK_ENABLE_LOCAL_HTCONDOR:-false}"
  local enable_slurm="${FASTINK_ENABLE_HOST_SLURM_CLIENT:-false}"

  upsert_named_entry sshd "$etc_dir/passwd" "sshd:x:74:74:Privilege-separated SSH:/usr/share/empty.sshd:/usr/sbin/nologin"
  upsert_named_entry sshd "$etc_dir/group" "sshd:x:74:"
  upsert_named_entry sshd "$etc_dir/shadow" "sshd:!!:20545::::::"
  upsert_named_entry sshd "$etc_dir/gshadow" "sshd:!::"

  assert_fixed_xrootd_identity_is_safe "$etc_dir/passwd" "$etc_dir/group"
  warn_if_xrootd_identity_will_be_normalized "$etc_dir/passwd" "$etc_dir/group"
  upsert_named_entry xrootd "$etc_dir/passwd" "xrootd:x:379:978:XRootD runtime user:/var/spool/xrootd:/sbin/nologin"
  upsert_named_entry xrootd "$etc_dir/group" "xrootd:x:978:"
  upsert_named_entry xrootd "$etc_dir/shadow" "xrootd:!!:20460::::::"
  upsert_named_entry xrootd "$etc_dir/gshadow" "xrootd:!::"

  if [[ "$enable_condor" == "true" ]]; then
    assert_fixed_condor_identity_is_safe "$etc_dir/passwd" "$etc_dir/group"
    warn_if_condor_identity_will_be_normalized "$etc_dir/passwd" "$etc_dir/group"
    upsert_named_entry condor "$etc_dir/passwd" "condor:x:64:64:Owner of HTCondor Daemons:/var/lib/condor:/sbin/nologin"
    upsert_named_entry condor "$etc_dir/group" "condor:x:64:"
    upsert_named_entry condor "$etc_dir/shadow" "condor:!!:20460::::::"
    upsert_named_entry condor "$etc_dir/gshadow" "condor:!::"
  fi

  if [[ "$enable_slurm" == "true" ]]; then
    upsert_named_entry slurm "$etc_dir/passwd" "slurm:x:58002:58002:Slurm workload manager:/var/lib/slurm:/sbin/nologin"
    upsert_named_entry slurm "$etc_dir/group" "slurm:x:58002:"
    upsert_named_entry slurm "$etc_dir/shadow" "slurm:!!:20460::::::"
    upsert_named_entry slurm "$etc_dir/gshadow" "slurm:!::"
  fi
}
