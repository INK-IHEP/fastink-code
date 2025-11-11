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

# set the jupyter env
APP_TOKEN=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
APP_CONFIG_FILE="jupyter_config.py"
APP_PORT=$(get_free_port)
APP_PATH="`/bin/pwd`"
APP_LOGIN_INFO="app_login.info"
APP_RUN_HOST="`/bin/hostname | /bin/awk -F '.' '{print $1}'`"
export KRB5CCNAME="${APP_PATH}/krb5cc_${UID}"
/usr/bin/aklog


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

# unset PYTHONPATH to avoid conflict with jupyter
unset PYTHONPATH
#export PYTHONPATH=/cvmfs/slurm.ihep.ac.cn/alma9/anaconda3/envs/jupyter/lib/python3.9/site-packages:$PYTHONPATH
# config_file="$HOME/.ink/ENV/jupyter.ini"

# Check if the file exists
# if [[ -f "$config_file" ]]; then
#     # Read the file content and remove possible newlines
#     content=$(cat "$config_file" | tr -d '\n')
    
#     # Check if the content equals "lastest"
#     if [[ "$content" == "lastest" ]]; then
#          export JUPYTER_PATH=/cvmfs/slurm.ihep.ac.cn/alma9/junokernel/share/jupyter/:$JUPYTER_PATH
#     fi
# fi

/cvmfs/slurm.ihep.ac.cn/alma9/anaconda3/envs/jupyter/bin/jupyter-lab --config=${APP_CONFIG_FILE} --notebook-dir=~ &> start-jupyterlab-token.sh.out