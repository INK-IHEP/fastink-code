FROM almalinux:9.6

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
RUN chmod +x /usr/local/bin/init-assets.sh

ENTRYPOINT ["/usr/local/bin/init-assets.sh"]
