ARG BASE_IMAGE=htcondor/base:23.0.28-el9

FROM ${BASE_IMAGE}
ARG BASE_IMAGE

RUN dnf install -y --allowerasing \
        coreutils \
        findutils \
        gawk \
        grep \
        hostname \
        iproute \
        krb5-workstation \
        lsof \
        openssh-clients \
        openssh-server \
        procps-ng \
        python3 \
        screen \
        sed \
        shadow-utils \
        sudo \
        util-linux \
        which && \
    dnf clean all && \
    rm -rf /var/cache/dnf

RUN rm -f /root/config/pre-exec.sh
COPY deploy/images/htcondor/config/ /root/config/
RUN chmod +x /root/config/pre-exec.sh && \
    mkdir -p /var/log/condor/PoolLog /var/lock/condor && \
    chown -R condor:condor /var/log/condor /var/lib/condor /var/lock/condor && \
    cp -a /etc /etc-init && \
    rm -f /etc/passwd /etc/group /etc/shadow /etc/gshadow && \
    ln -s /etc-init/passwd /etc/passwd && \
    ln -s /etc-init/group /etc/group && \
    ln -s /etc-init/shadow /etc/shadow && \
    ln -s /etc-init/gshadow /etc/gshadow

EXPOSE 8080
EXPOSE 9618

CMD ["/start.sh"]
