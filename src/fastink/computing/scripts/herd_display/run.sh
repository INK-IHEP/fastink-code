#!/bin/bash

APP_PORT=${1}
HERD_ROOT=${2}
HERD_XML=${3}
HERD_BIN=${4}
APP_LOGIN_INFO="app_login.info"
APP_RUN_HOST="`/bin/hostname | /bin/awk -F '.' '{print $1}'`"

/bin/echo "{\"HOST\": \"${APP_RUN_HOST}\", \"PORT\": \"${APP_PORT}\", \"xml\": \"${HERD_XML}\", \"root\": \"${HERD_ROOT}\"}" > ${APP_LOGIN_INFO}

/bin/bash ${HERD_BIN} ${APP_PORT} ${HERD_XML} ${HERD_ROOT} ${APP_RUN_HOST} 2>&1
