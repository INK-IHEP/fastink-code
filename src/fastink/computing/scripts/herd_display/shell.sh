#!/bin/bash

HPORT=${1}
HXML=${2}
HROOT=${3}

APP_RUN_HOST="`/bin/hostname | /bin/awk -F '.' '{print $1}'`"



/bin/echo "{\"HOST\": \"${APP_RUN_HOST}\", \"PORT\": \"${APP_PORT}\", \"xml\": \"${HXML}\", \"root\": \"${HROOT}\"}}" > ${APP_LOGIN_INFO}

${HERD_BIN} ${HPORT} ${HXML} ${HROOT}  2>&1

