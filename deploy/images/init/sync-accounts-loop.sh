#!/bin/bash
# Long-running loop for the fastink-accounts-sync container.
#
# Re-runs sync-accounts.sh whenever the host account files change (or the
# published view is missing), sleeping SLEEP_TIME seconds between checks.
# The IHEP deployment uses the same pattern with useradd/usermod against a
# shared account volume; this is the host-/etc variant for the open-source
# deployment.
set -u

HOST_ETC="${FASTINK_HOST_ETC:-/host-etc}"
ETC_INIT="${FASTINK_ETC_INIT:-/etc-init}"
SLEEP_TIME="${SLEEP_TIME:-60}"
if ! [[ "$SLEEP_TIME" =~ ^[0-9]+$ ]] || (( SLEEP_TIME < 5 )); then
  echo "[accounts-sync] invalid SLEEP_TIME='$SLEEP_TIME'; falling back to 60s" >&2
  SLEEP_TIME=60
fi
STATE_FILE="${FASTINK_SYNC_STATE_FILE:-/tmp/.fastink-accounts-sync.state}"
SYNC_SCRIPT="${FASTINK_SYNC_SCRIPT:-/usr/local/bin/sync-accounts.sh}"
ACCOUNTS_LIB="${FASTINK_ACCOUNTS_LIB:-/usr/local/lib/fastink-accounts.sh}"
# shellcheck source=/dev/null
source "$ACCOUNTS_LIB"
FILES=(passwd group shadow gshadow)

log() {
  printf '[accounts-sync] %s %s\n' "$(date -Is)" "$*"
}

host_state_hash() {
  {
    for file in "${FILES[@]}"; do
      resolved="$(resolve_host_account_file "$file" 2>/dev/null)" || continue
      sha256sum "$resolved"
    done
    printf '%s %s\n' \
      "${FASTINK_ENABLE_LOCAL_HTCONDOR:-false}" \
      "${FASTINK_ENABLE_HOST_SLURM_CLIENT:-false}"
  } | sha256sum | awk '{print $1}'
}

target_view_complete() {
  local file
  for file in "${FILES[@]}"; do
    [[ -s "$ETC_INIT/$file" ]] || return 1
  done
  return 0
}

view_state_hash() {
  {
    for file in "${FILES[@]}"; do
      if [[ -e "$ETC_INIT/$file" ]]; then
        sha256sum "$ETC_INIT/$file"
      fi
    done
  } | sha256sum | awk '{print $1}'
}

# Stop promptly on docker stop/down instead of burning the 10s grace period
# (bash runs as PID 1, where SIGTERM is otherwise ignored).
trap 'log "stopping"; exit 0' TERM INT

log "started (interval ${SLEEP_TIME}s, host ${HOST_ETC}, target ${ETC_INIT})"

while true; do
  current_hash="$(host_state_hash)"
  previous_host_hash="$(awk '{print $1}' "$STATE_FILE" 2>/dev/null)"
  current_view_hash="$(view_state_hash)"
  previous_view_hash="$(awk '{print $2}' "$STATE_FILE" 2>/dev/null)"

  # Re-sync when the host changed, when the published view no longer matches
  # what was last written (manual edit, or a sync interrupted between the
  # per-file renames), or when the view is incomplete.
  if [[ "$current_hash" != "$previous_host_hash" ]] \
      || [[ "$current_view_hash" != "$previous_view_hash" ]] \
      || ! target_view_complete; then
    if "$SYNC_SCRIPT"; then
      printf '%s %s' "$current_hash" "$(view_state_hash)" > "$STATE_FILE"
      log "account view refreshed (host state ${current_hash:0:12})"
    else
      log "account sync failed; keeping the previous account view"
    fi
  fi

  sleep "$SLEEP_TIME" &
  wait $!
done
