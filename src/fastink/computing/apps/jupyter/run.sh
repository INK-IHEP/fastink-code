#!/bin/bash
#
# Per-app entrypoint invoked by ../../shell.sh after the shared setup.

# App-specific env (was scripts/jupyter/shell.sh)
export JUPYTER_BIN="${JUPYTER_BIN:-/cvmfs/slurm.ihep.ac.cn/alma9/anaconda3/envs/jupyter/bin}"
_CVMFS_IPYK="/cvmfs/common.ihep.ac.cn/software/ipykernel"
export JUPYTER_PATH=\
"${_CVMFS_IPYK}/fermiPy/fermiPy_1_4_0/share/jupyter/:"\
"${_CVMFS_IPYK}/Julia/Julia_1_11_5/share/jupyter/:"\
"${_CVMFS_IPYK}/ROOT/ROOT_6_34_4/share/jupyter/:"\
"/cvmfs/slurm.ihep.ac.cn/alma9/junokernel/share/jupyter/:"\
"${JUPYTER_PATH:-}"

APP_PORT="${APP_PORT:-$(get_free_port)}"
APP_CONFIG_FILE="jupyter_config.py"
APP_LOGIN_INFO="app_login.info"
APP_RUN_HOST="`/bin/hostname | /bin/awk -F '.' '{print $1}'`"
APP_TOKEN=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
DEADLINE=$(( $(date +%s) + INK_INIT_HOURS * 3600 ))
BASE_URL="/jupyter/${APP_RUN_HOST}/${APP_PORT}/"

# 判定用的 Python：优先 jupyter 自带，其次系统 python3（只用标准库 urllib/json/os）。
# 不用用户 PATH 里的 python，避免环境差异。
_pick_python() {
    local c
    for c in "${JUPYTER_BIN}/python3" "${JUPYTER_BIN}/python" /usr/bin/python3 /bin/python3; do
        [[ -x "$c" ]] && { echo "$c"; return 0; }
    done
    command -v python3 2>/dev/null
}
INK_PYTHON="$(_pick_python)"

# jupyter 活跃判定：全部使用操作系统级信号，不依赖 Jupyter HTTP API 的时间戳。
# HTTP API 的 terminal last_activity 由 WebSocket 心跳（约 90s）驱动，标签页挂着
# 不动也会持续刷新，不可用。操作系统信号不经过 WebSocket，不会被心跳污染：
#   1) kernel busy —— /api/kernels 的 execution_state，代码 cell 正在跑
#   2) notebook mtime —— 用户在 notebook 里打字，自动保存会刷新 .ipynb 的 mtime
#   3) PTY atime —— 用户在终端里有键盘输入（read(2) 更新 slave PTY 的 atime）
#   4) PTY 子进程 —— 终端里有程序在跑（shell 有子进程，说明不是空等）
# 信号 3/4 通过 /proc/PID/fd 找到 jupyter-server 持有的所有 PTY slave，
# 再读其 atime 和对应 shell 的子进程列表来判断。
is_active() {
    APP_TOKEN="${APP_TOKEN}" APP_PORT="${APP_PORT}" BASE_URL="${BASE_URL}" \
    NOTEBOOK_DIR="${HOME}" INK_ACTIVE_IDLE_SEC="${INK_ACTIVE_IDLE_SEC}" \
    APP_PID="${APP_PID}" \
    "${INK_PYTHON}" - <<'PY' 2>/dev/null
import os, sys, json, time, datetime, urllib.request, stat

port = os.environ["APP_PORT"]; base = os.environ["BASE_URL"]; tok = os.environ["APP_TOKEN"]
win = int(os.environ.get("INK_ACTIVE_IDLE_SEC", "1800"))
ndir = os.path.expanduser(os.environ.get("NOTEBOOK_DIR", "~"))
app_pid = int(os.environ.get("APP_PID", "0"))
now_ts = time.time()

def api(path):
    url = f"http://127.0.0.1:{port}{base}api/{path}"
    req = urllib.request.Request(url, headers={"Authorization": f"token {tok}"})
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.load(r)

# --- 信号 1: kernel busy (来自 HTTP API, 但 busy 状态不会被心跳误触发) ---
try:
    for k in api("kernels"):
        if k.get("execution_state") == "busy":
            sys.exit(0)
except Exception:
    pass  # server 未就绪时跳过，继续看其他信号

# --- 信号 2: notebook 文件 mtime (操作系统级, 自动保存会刷新) ---
try:
    for s in api("sessions"):
        p = s.get("path")
        if not p:
            continue
        try:
            if now_ts - os.stat(os.path.join(ndir, p)).st_mtime <= win:
                sys.exit(0)
        except OSError:
            pass
except Exception:
    pass

# --- 信号 3 + 4: 枚举 jupyter-server 进程树持有的所有 PTY slave ---
# 收集整个进程树 (jupyter-server 及其所有子孙)
def all_pids_in_tree(root):
    """BFS 收集 root 及其所有子孙 PID"""
    result = set()
    queue = [root]
    while queue:
        pid = queue.pop()
        if pid in result:
            continue
        result.add(pid)
        try:
            children = open(f"/proc/{pid}/task/{pid}/children").read().split()
            queue.extend(int(c) for c in children)
        except OSError:
            pass
    return result

if app_pid > 0:
    tree = all_pids_in_tree(app_pid)
    pty_slaves = set()
    for pid in tree:
        fd_dir = f"/proc/{pid}/fd"
        try:
            for fd in os.listdir(fd_dir):
                try:
                    target = os.readlink(f"{fd_dir}/{fd}")
                    # PTY slave 形如 /dev/pts/N
                    if target.startswith("/dev/pts/") and target[9:].isdigit():
                        pty_slaves.add(target)
                except OSError:
                    pass
        except OSError:
            pass

    for pty in pty_slaves:
        try:
            st = os.stat(pty)
            # 信号 3: PTY atime 新鲜 —— 用户最近有键盘输入
            if now_ts - st.st_atime <= win:
                sys.exit(0)
        except OSError:
            pass

    # 信号 4: 任一 PTY slave 对应的 shell 进程有子进程 —— 终端里有程序在跑
    # 找持有 PTY slave 的直接 shell 进程（通常是 bash/sh), 看其 children
    for pid in tree:
        try:
            children_raw = open(f"/proc/{pid}/task/{pid}/children").read().split()
            if not children_raw:
                continue
            # 确认该进程持有 PTY slave (是终端 shell 而非无关进程）
            fd_dir = f"/proc/{pid}/fd"
            has_pty = False
            try:
                for fd in os.listdir(fd_dir):
                    try:
                        t = os.readlink(f"{fd_dir}/{fd}")
                        if t.startswith("/dev/pts/") and t[9:].isdigit():
                            has_pty = True
                            break
                    except OSError:
                        pass
            except OSError:
                pass
            if has_pty:
                sys.exit(0)  # shell 有子进程，说明有命令在运行
        except OSError:
            pass

sys.exit(1)
PY
}

# 看门狗：$1 = jupyter 主进程 PID
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
                echo "[INK] 已到期且用户不再使用，正常关闭 Jupyter。"
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
c.ServerApp.ip = '0.0.0.0'
c.ServerApp.port = ${APP_PORT}
c.ServerApp.port_retries = 0
c.ServerApp.token = "${APP_TOKEN}"
c.ServerApp.open_browser = False
c.ServerApp.base_url = "/jupyter/${APP_RUN_HOST}/${APP_PORT}/"
c.ServerApp.allow_origin = '*'
c.ServerApp.disable_check_xsrf = True
EOL
)

/bin/echo "{\"HOST\": \"${APP_RUN_HOST}\", \"PORT\": \"${APP_PORT}\", \"TOKEN\": \"${APP_TOKEN}\"}" > ${APP_LOGIN_INFO}
unset PYTHONPATH


echo "[INK] Jupyter 初始时长 ${INK_INIT_HOURS} 小时，初始截止：$(date -d @${DEADLINE} '+%F %T')"

# 后台启动 jupyter，看门狗守护其 PID
${JUPYTER_BIN}/jupyter-lab --config=${APP_CONFIG_FILE} --notebook-dir=~ 2>&1 &
APP_PID=$!
export APP_PID

trap 'kill -TERM ${APP_PID} 2>/dev/null' INT TERM

watchdog "${APP_PID}" &
WATCHDOG_PID=$!

wait "${APP_PID}"
EXIT_CODE=$?
kill "${WATCHDOG_PID}" 2>/dev/null
exit ${EXIT_CODE}