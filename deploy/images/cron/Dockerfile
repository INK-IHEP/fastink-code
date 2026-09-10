ARG BASE_IMAGE=fastink-server:latest
FROM ${BASE_IMAGE}

WORKDIR /opt/fastink-cron

COPY deploy/images/cron/runner.py /opt/fastink-cron/runner.py
COPY deploy/images/cron/cron.yaml /opt/fastink-cron/cron.yaml
COPY deploy/images/cron/entrypoint.sh /usr/local/bin/fastink-cron-entrypoint.sh

RUN pip3.12 install --no-cache-dir \
    aiohttp \
    "concurrent-log-handler>=0.9.29" \
    elasticsearch==7.13.4 \
    pymysql \
    pytz \
    pyyaml \
    redis

RUN chmod +x /usr/local/bin/fastink-cron-entrypoint.sh

ENV FASTINK_CRON_BASE_DIR=/opt/fastink-cron
ENV FASTINK_CRON_CONFIG=/opt/fastink-cron/cron.yaml
ENV FASTINK_CRON_CONFIG_OVERLAY=
ENV FASTINK_CRON_LOG_DIR=/ink/log

ENTRYPOINT ["/usr/local/bin/fastink-cron-entrypoint.sh"]
