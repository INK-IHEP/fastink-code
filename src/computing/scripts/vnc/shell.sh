#!/bin/bash

function get_free_port() {
    while true; do
        PORT=$(shuf -i 49152-65535 -n 1)
        if ! /usr/sbin/ss -ltn | grep -q ":$PORT\b"; then
            echo $PORT
            break
        fi
    done
}

APP_PATH="`/bin/pwd`"
if [ -f "${APP_PATH}/krb5cc_${UID}" ]; then
  export KRB5CCNAME="${APP_PATH}/krb5cc_${UID}"
  /usr/bin/aklog
fi

export PATH="$HOME/.local/bin:$PATH"

PYBIN="$(command -v python3 || command -v python || true)"
if ! command -v websockify >/dev/null 2>&1; then
  if [ -n "$PYBIN" ]; then
    "$PYBIN" -m pip install --user --quiet websockify || {
      echo "[WARN] pip 安装 websockify 失败；将改用 'python -m websockify' 方式。" >&2
    }
    hash -r
  else
    echo "[WARN] 未找到 python, 无法安装 websockify。" >&2
  fi
fi

if command -v websockify >/dev/null 2>&1; then
  export WEBSOCKIFY="$(command -v websockify)"
fi

if grep -q "CentOS Linux release 7.9" /etc/redhat-release; then
    export PATH=/cvmfs/slurm.ihep.ac.cn/centos7.9/anaconda3/envs/ink/bin:$PATH
    export LD_LIBRARY_PATH=/cvmfs/slurm.ihep.ac.cn/centos7.9/anaconda3/envs/ink/lib:$LD_LIBRARY_PATH
fi

APP_PORT=$(get_free_port)
LISTEN_PORT=$(get_free_port)
./run.sh ${APP_PORT} ${LISTEN_PORT}