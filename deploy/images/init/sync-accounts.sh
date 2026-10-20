#!/bin/bash
# Rebuild the container account view from the host's account database.
#
# Host /etc/passwd|group|shadow|gshadow is mounted read-only at
# $FASTINK_HOST_ETC (default /host-etc); the merged view is written to
# $FASTINK_ETC_INIT (default /etc-init), which service containers mount at
# /etc-init. Host is the source of truth: users added, changed or removed on
# the host are reflected on the next run.
#
# The fixed service identities (sshd, xrootd, optionally condor/slurm) are
# re-applied by apply_fixed_service_accounts, exactly like the init
# container, so container services keep their pinned uid/gid.
#
# Each file is published with an atomic rename inside the target directory;
# the four files are replaced one at a time, so a reader could observe a mixed
# generation only inside the few milliseconds between the renames. If the host
# state is unsafe (e.g. uid 379 already owned by another account) the script
# exits non-zero before publishing anything.
set -euo pipefail

ACCOUNTS_LIB="${FASTINK_ACCOUNTS_LIB:-/usr/local/lib/fastink-accounts.sh}"
# shellcheck source=/dev/null
source "$ACCOUNTS_LIB"

HOST_ETC="${FASTINK_HOST_ETC:-/host-etc}"
ETC_INIT="${FASTINK_ETC_INIT:-/etc-init}"
FILES=(passwd group shadow gshadow)

for file in "${FILES[@]}"; do
  resolve_host_account_file "$file" >/dev/null || exit 1
done

mkdir -p "$ETC_INIT"
# Serialize with the one-shot init container, which may write the same
# volume during a redeploy while this loop is still running.
exec 9>"$ETC_INIT/.accounts.lock"
flock -w 60 9 || { echo "[accounts-sync] could not acquire $ETC_INIT/.accounts.lock" >&2; exit 1; }

tmp_dir="$(mktemp -d "$ETC_INIT/.accounts-sync.XXXXXX")"
trap 'rm -rf "$tmp_dir"' EXIT

for file in "${FILES[@]}"; do
  cp "$(resolve_host_account_file "$file")" "$tmp_dir/$file"
done
chmod 644 "$tmp_dir/passwd" "$tmp_dir/group"
chmod 600 "$tmp_dir/shadow" "$tmp_dir/gshadow"

apply_fixed_service_accounts "$tmp_dir"

# Publish one file at a time; same-filesystem renames are atomic.
for file in "${FILES[@]}"; do
  mv -f "$tmp_dir/$file" "$ETC_INIT/$file"
done

trap - EXIT
rm -rf "$tmp_dir"
flock -u 9
exec 9>&-
