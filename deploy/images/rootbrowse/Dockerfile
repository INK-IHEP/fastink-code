FROM almalinux:9.6

RUN rm -f /etc/yum.repos.d/almalinux-*repo
COPY deploy/images/repos/alma.repo /etc/yum.repos.d/almalinux.repo
COPY deploy/images/repos/epel.repo /etc/yum.repos.d/epel.repo
COPY deploy/images/repos/afs.repo /etc/yum.repos.d/afs.repo

RUN dnf -y update && \
    dnf install -y --allowerasing \
        glibc-langpack-en \
        iproute \
        keyutils \
        krb5-workstation \
        kstart \
        openssh-clients \
        openssh-server \
        procps-ng \
        sudo \
        screen \
        git \
        make \
        cmake \
        gcc-c++ \
        gcc \
        binutils \
        libX11-devel \
        libXpm-devel \
        libXft-devel \
        libXext-devel \
        openssl-devel \
        openafs-krb5 \
        tcsh \
        xrootd-client \
        xrootd-client-devel \
        xrootd-libs-devel \
        tbb \
        tbb-devel \
        xxhash \
        xxhash-devel && \
    mkdir -p /usr/vice/etc && \
    dnf clean all && \
    rm -rf /var/cache/dnf

ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

COPY deploy/images/rootbrowse/start-rootbrowse.sh /usr/local/bin/start-rootbrowse.sh
COPY deploy/images/rootbrowse/container-healthcheck.sh /usr/local/bin/container-healthcheck.sh

RUN chmod +x /usr/local/bin/start-rootbrowse.sh /usr/local/bin/container-healthcheck.sh

RUN cp -a /etc /etc-init && \
    rm -f /etc/passwd /etc/group /etc/shadow /etc/gshadow && \
    ln -s /etc-init/passwd /etc/passwd && \
    ln -s /etc-init/group /etc/group && \
    ln -s /etc-init/shadow /etc/shadow && \
    ln -s /etc-init/gshadow /etc/gshadow
VOLUME /etc-init

EXPOSE 2000

CMD ["/usr/local/bin/start-rootbrowse.sh"]
