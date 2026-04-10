ARG BASE_IMAGE=dockerhub.ihep.ac.cn/htcondor/base:24.0.2-el9

FROM ${BASE_IMAGE}
ARG BASE_IMAGE

RUN rm -f /root/config/pre-exec.sh
COPY deploy/images/htcondor/config/ /root/config/
RUN chmod +x /root/config/pre-exec.sh && \
    mkdir -p /var/log/condor/PoolLog && \
    cp -a /etc /etc-init && \
    rm -f /etc/passwd /etc/group /etc/shadow /etc/gshadow && \
    ln -s /etc-init/passwd /etc/passwd && \
    ln -s /etc-init/group /etc/group && \
    ln -s /etc-init/shadow /etc/shadow && \
    ln -s /etc-init/gshadow /etc/gshadow

EXPOSE 8080
EXPOSE 9618

CMD ["/start.sh"]
