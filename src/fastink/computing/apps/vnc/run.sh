#!/bin/bash
#
# Per-app entrypoint invoked by ../../shell.sh after the shared setup.
#
# vnc is the reference member of the VNC family (vnc / asic / asicbm /
# ink_special).  The whole session -- port allocation, websockify bootstrap,
# stale-session cleanup, vncserver, the FS-based OTP listener, noVNC and the
# deadline watchdog -- lives in ink_run_vnc_session() in apps/shell.sh so
# that it exists exactly once across all four job types.
#
# Nothing below is vnc-specific; family members that need extra environment
# set it before calling the runner (see npu/run.sh for an example).

# Fail loudly rather than silently if the shared runner is missing.  A bare
# "command not found" here is exactly how a previous refactor went unnoticed
# in production for weeks; an explicit message names the real cause.
declare -F ink_run_vnc_session >/dev/null || {
    echo "[FATAL] ink_run_vnc_session is not defined by shell.sh -- fastink-code and this app's" >&2
    echo "[FATAL] plugin are out of sync; redeploy fastink-code first." >&2
    exit 127
}

ink_run_vnc_session
