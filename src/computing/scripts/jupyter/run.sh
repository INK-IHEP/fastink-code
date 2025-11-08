#!/bin/bash

APP_PORT=${1}
APP_CONFIG_FILE="jupyter_config.py"
APP_LOGIN_INFO="app_login.info"
APP_RUN_HOST="`/bin/hostname | /bin/awk -F '.' '{print $1}'`"
APP_TOKEN=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")

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
CVMFS_IPYK="/cvmfs/common.ihep.ac.cn/software/ipykernel"
export JUPYTER_PATH=\
"${CVMFS_IPYK}/fermiPy/fermiPy_1_4_0/share/jupyter/:"\
"${CVMFS_IPYK}/Julia/Julia_1_11_5/share/jupyter/:"\
"${CVMFS_IPYK}/ROOT/ROOT_6_34_4/share/jupyter/:"\
"/cvmfs/slurm.ihep.ac.cn/alma9/junokernel/share/jupyter/:"\
"$JUPYTER_PATH"

unset PYTHONPATH

#/cvmfs/slurm.ihep.ac.cn/alma9/anaconda3/envs/jupyter/bin/jupyter-lab --config=${APP_CONFIG_FILE} --notebook-dir=~ &> start-jupyterlab-token.sh.out
/cvmfs/slurm.ihep.ac.cn/alma9/anaconda3/envs/jupyter/bin/jupyter-lab --config=${APP_CONFIG_FILE} --notebook-dir=~ 2>&1