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

# Configure port and path
APP_PORT=$(get_free_port)
APP_PATH="`/bin/pwd`"

if [ -f "${APP_PATH}/krb5cc_${UID}" ]; then
  export KRB5CCNAME="${APP_PATH}/krb5cc_${UID}"
fi

if command -v /usr/bin/aklog >/dev/null 2>&1 && klist -s 2>/dev/null; then
  /usr/bin/aklog
fi

# Configure Jupyter environment
export JUPYTER_BIN="/cvmfs/slurm.ihep.ac.cn/alma9/anaconda3/envs/jupyter/bin"
CVMFS_IPYK="/cvmfs/common.ihep.ac.cn/software/ipykernel"
export JUPYTER_PATH=\
"${CVMFS_IPYK}/fermiPy/fermiPy_1_4_0/share/jupyter/:"\
"${CVMFS_IPYK}/Julia/Julia_1_11_5/share/jupyter/:"\
"${CVMFS_IPYK}/ROOT/ROOT_6_34_4/share/jupyter/:"\
"/cvmfs/slurm.ihep.ac.cn/alma9/junokernel/share/jupyter/:"\
"$JUPYTER_PATH"

export INK_INIT_HOURS=${INK_INIT_HOURS:-24}             # 初始时长（小时）
export INK_CHECK_INTERVAL=${INK_CHECK_INTERVAL:-900}    # 看门狗轮询间隔（秒）
export INK_ACTIVE_IDLE_SEC=${INK_ACTIVE_IDLE_SEC:-1800} # 无活动超过此分钟数视为空闲（秒）

./run.sh ${APP_PORT}

