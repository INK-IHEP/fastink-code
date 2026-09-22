#!/bin/bash
#
# Per-app entrypoint invoked by ../../shell.sh after the shared setup
# (INKPATH, kerberos, aklog, watchdog defaults, get_free_port). No
# positional arguments -- ports and paths are resolved here.

# App-specific env (was scripts/vscode/shell.sh)
export VSCODE_BIN="${VSCODE_BIN:-/usr/bin/code-server}"
export NODE_OPTIONS="${NODE_OPTIONS:---max-old-space-size=5120}"

APP_PATH="${APP_PATH:-$(/bin/pwd)}"
APP_PORT="${APP_PORT:-$(get_free_port)}"
APP_PASSWD=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
APP_CONFIG_FILE="config.yaml"
APP_LOGIN_INFO="app_login.info"
APP_RUN_HOST="`/bin/hostname | /bin/awk -F '.' '{print $1}'`"
DEADLINE=$(( $(date +%s) + INK_INIT_HOURS * 3600 ))

# 心跳文件：由 ink-heartbeat 扩展在“窗口聚焦且有真实交互”时刷新
INK_HEARTBEAT_FILE="${INK_HEARTBEAT_FILE:-${APP_PATH}/.ink_heartbeat}"

# vscode 活跃判定（权威信号）：读心跳文件 mtime。
# 扩展只在“窗口聚焦 且 最近有编辑/选区/滚动”时写该文件：
#   人真在操作 -> mtime 新鲜 -> 活跃；
#   标签页挂后台/前台不动 -> 窗口失焦或无事件 -> mtime 不更新 -> idle 增长 
# 不看连接/流量，避免被挂着的标签页心跳误判。
is_active() {
    [[ -f "$INK_HEARTBEAT_FILE" ]] || return 1     # 没有心跳 -> 不活跃
    local mtime now
    mtime=$(stat -c %Y "$INK_HEARTBEAT_FILE" 2>/dev/null) || return 1
    now=$(date +%s)
    (( now - mtime < INK_ACTIVE_IDLE_SEC ))
}

# 看门狗：$1 = code-server 主进程 PID
# 到期前：不干预。到期后转入看护模式：定期检测，只要用户仍在使用就保持运行，
# 一旦不再使用则立即关闭（不再固定延长时长）。
watchdog() {
    local app_pid=$1
    while kill -0 "$app_pid" 2>/dev/null; do
        now=$(date +%s)
        if (( now >= DEADLINE )); then
            if is_active; then
                echo "[INK] 已到期但用户仍在使用，保持运行。"
            else
                echo "[INK] 已到期且用户不再使用，正常关闭 VSCode。"
                kill -TERM "$app_pid" 2>/dev/null
                break
            fi
        fi
        sleep "$INK_CHECK_INTERVAL"
    done
}

(
umask 077
cat > "${APP_CONFIG_FILE}" << EOL
bind-addr: 0.0.0.0:${APP_PORT}
auth: password
password: ${APP_PASSWD}
EOL
)

/bin/echo "{\"HOST\": \"${APP_RUN_HOST}\", \"PORT\": \"${APP_PORT}\", \"PASSWD\": \"${APP_PASSWD}\"}" > ${APP_LOGIN_INFO}

echo "[INK] VSCode 初始时长 ${INK_INIT_HOURS} 小时，初始截止：$(date -d @${DEADLINE} '+%F %T')"

INK_VSCODE_EXT_SHARED="/cvmfs/common.ihep.ac.cn/software/ink/soft/extensions/ink-heartbeat-ext"
INK_EXT_DIR="${HOME}/.local/share/code-server/extensions/ink.ink-heartbeat-1.0.0"
mkdir -p "$INK_EXT_DIR"
cp -rf "${INK_VSCODE_EXT_SHARED}/." "$INK_EXT_DIR/"
export INK_HEARTBEAT_FILE

# 后台启动 code-server（加载心跳扩展，使用默认扩展目录），看门狗守护其 PID
${VSCODE_BIN} --config ${APP_PATH}/${APP_CONFIG_FILE} 2>&1 &
APP_PID=$!

trap 'kill -TERM ${APP_PID} 2>/dev/null' INT TERM

watchdog "${APP_PID}" &
WATCHDOG_PID=$!

wait "${APP_PID}"
EXIT_CODE=$?
kill "${WATCHDOG_PID}" 2>/dev/null
exit ${EXIT_CODE}