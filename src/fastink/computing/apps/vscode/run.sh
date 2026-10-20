#!/bin/bash
#
# Per-app entrypoint invoked by ../../shell.sh after the shared setup
# (INKPATH, kerberos, aklog, watchdog defaults, get_free_port). No
# positional arguments -- ports and paths are resolved by the runner.
#
# vscode is the reference member of the code-server family (vscode /
# compile / npu).  The whole session -- password generation, config.yaml,
# the ink-heartbeat extension install, code-server launch and the deadline
# watchdog -- lives in ink_run_codeserver_session() in apps/shell.sh so that
# it exists exactly once across all three job types.
#
# Nothing below is vscode-specific; family members that need extra
# environment set it before calling the runner (see npu/run.sh).

# Fail loudly rather than silently if the shared runner is missing.  A bare
# "command not found" here is exactly how a previous refactor went unnoticed
# in production for weeks; an explicit message names the real cause.
declare -F ink_run_codeserver_session >/dev/null || {
    echo "[FATAL] ink_run_codeserver_session is not defined by shell.sh -- fastink-code and this app's" >&2
    echo "[FATAL] plugin are out of sync; redeploy fastink-code first." >&2
    exit 127
}

ink_run_codeserver_session
