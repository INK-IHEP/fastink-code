# Base image defaults to the site registry (build mode without docker.io
# access); override with --build-arg BASE_IMAGE=<any registry>/almalinux:9.
ARG BASE_IMAGE=dockerhub.ihep.ac.cn/ink/base-almalinux:9

FROM ${BASE_IMAGE}

RUN rm -f /etc/yum.repos.d/almalinux-*repo
COPY deploy/images/repos/alma.repo /etc/yum.repos.d/almalinux.repo
COPY deploy/images/repos/epel.repo /etc/yum.repos.d/epel.repo

RUN dnf -y update && \
    dnf install -y --allowerasing \
        openssh-server \
        openssh-clients \
        openssl \
        xrootd-server && \
    dnf clean all && \
    rm -rf /var/cache/dnf

COPY deploy/images/init/init-assets.sh /usr/local/bin/init-assets.sh
COPY deploy/images/init/accounts-lib.sh /usr/local/lib/fastink-accounts.sh
COPY deploy/images/init/sync-accounts.sh /usr/local/bin/sync-accounts.sh
COPY deploy/images/init/sync-accounts-loop.sh /usr/local/bin/sync-accounts-loop.sh
RUN chmod +x /usr/local/bin/init-assets.sh \
             /usr/local/bin/sync-accounts.sh \
             /usr/local/bin/sync-accounts-loop.sh

ENTRYPOINT ["/usr/local/bin/init-assets.sh"]
