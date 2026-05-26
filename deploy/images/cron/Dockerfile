ARG BASE_IMAGE=fastink-server:latest
FROM ${BASE_IMAGE}

WORKDIR /opt/fastink-cron

COPY deploy/images/cron/entrypoint.sh /usr/local/bin/fastink-cron-entrypoint.sh
COPY deploy/images/cron/cron.ini /opt/fastink-cron/cron.ini
COPY deploy/images/cron/jobs /opt/fastink-cron/jobs

RUN pip3.12 install --no-cache-dir \
    aiohttp \
    elasticsearch==7.13.4 \
    pymysql \
    pytz \
    pyyaml \
    redis

RUN chmod +x /usr/local/bin/fastink-cron-entrypoint.sh

ENV FASTINK_CRON_BASE_DIR=/opt/fastink-cron
ENV FASTINK_CRON_CONFIG=/opt/fastink-cron/cron.ini
ENV FASTINK_CRON_LOG_DIR=/var/log/fastink-cron

ENTRYPOINT ["/usr/local/bin/fastink-cron-entrypoint.sh"]
