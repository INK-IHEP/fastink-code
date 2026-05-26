services:
  fastink-htcondor:
    image: ${htcondor_image}
    restart: unless-stopped
    privileged: true
    hostname: fastink-htcondor
    environment:
      CONDOR_HOST: fastink-htcondor
    volumes:
      - ${etc_init_dir}:/etc-init
      - /cvmfs:/cvmfs:ro
      - ${htcondor_local_conf_host_path}:/etc/condor/config.d/90-fastink-local.conf:ro
${htcondor_krb5_conf_mount_block}
${htcondor_extra_mounts_block}
    healthcheck:
      test: ["CMD-SHELL", "condor_status -pool 127.0.0.1 >/dev/null 2>&1 && condor_q -pool 127.0.0.1 >/dev/null 2>&1"]
      interval: 30s
      timeout: 10s
      retries: 10
      start_period: 30s
