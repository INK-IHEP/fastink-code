FROM almalinux:9.6

RUN rm -f /etc/yum.repos.d/almalinux-*repo
COPY deploy/images/repos/alma.repo /etc/yum.repos.d/almalinux.repo
COPY deploy/images/repos/epel.repo /etc/yum.repos.d/epel.repo
COPY deploy/images/repos/condor.repo /etc/yum.repos.d/condor.repo
COPY deploy/images/repos/afs.repo /etc/yum.repos.d/afs.repo
COPY deploy/images/repos/slurm.repo /etc/yum.repos.d/slurm.repo

RUN dnf -y update && \
    dnf install -y --allowerasing \
        git \
        keyutils \
        krb5-workstation \
        kstart \
        openssh-clients \
        procps-ng \
        python3.12 \
        python3.12-pip \
        sudo \
        tcsh \
        vim-minimal \
        wget \
        which \
        xrootd-client \
        zip \
        zsh \
        condor \
        openafs-krb5 \
        slurm && \
    mkdir -p /usr/vice/etc && \
    dnf clean all && \
    rm -rf /var/cache/dnf

RUN alternatives --install /usr/bin/python python /usr/bin/python3.12 1 && \
    alternatives --set python /usr/bin/python3.12 && \
    alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1 && \
    alternatives --set python3 /usr/bin/python3.12

WORKDIR /ink

COPY README.md setup.py /ink/
COPY bin /ink/bin
COPY src /ink/src
COPY tools /ink/tools

RUN pip3.12 install --upgrade pip && \
    pip3.12 install -e /ink

COPY deploy/images/server/start-ink.sh /usr/local/bin/start-ink.sh
RUN chmod +x /usr/local/bin/start-ink.sh

RUN cp -a /etc /etc-init && \
    rm -f /etc/passwd /etc/group /etc/shadow /etc/gshadow && \
    ln -s /etc-init/passwd /etc/passwd && \
    ln -s /etc-init/group /etc/group && \
    ln -s /etc-init/shadow /etc/shadow && \
    ln -s /etc-init/gshadow /etc/gshadow
VOLUME /etc-init

ENV INK_CONFIG_FILE=/ink/config.yml
ENV INK_PRODUCTION=false
ENV WORKERS=4
ENV HOST=0.0.0.0
ENV PORT=8000
ENV INIT_DATABASE_ON_START=true
ENV INSTALL_EDITABLE=false

EXPOSE 8000

CMD ["/usr/local/bin/start-ink.sh"]
