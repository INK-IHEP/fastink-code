services:
  fastink-xrootd:
    image: ${xrootd_image}
    restart: unless-stopped
    cap_add:
      - CAP_SETGID
      - CAP_SETUID
      - CAP_DAC_OVERRIDE
      - SYS_ADMIN
    volumes:
      - ${etc_init_dir}:/etc-init
      - ${xrootd_data_dir}:/xrootd
      - ${xrootd_conf_path}:/opt/fastink-xrootd/xrootd-proxy.cfg:ro
      - ${xrootd_sss_keytab_host_path}:${xrootd_sss_keytab_container_path}:ro
      - ${xrootd_krb5_keytab_host_path}:${xrootd_krb5_keytab_container_path}:ro
      - ${xrootd_vo_list_host_path}:${xrootd_vo_list_container_path}:ro
${xrootd_extra_mounts_block}
    environment:
      XC_ENABLE_MULTIUSER: "1"
    command: >-
      sh -c "
      if [ ! -f /etc/xrootd/xrootd-proxy.cfg ]; then
        cp /opt/fastink-xrootd/xrootd-proxy.cfg /etc/xrootd/xrootd-proxy.cfg;
      fi;
      if [ -s /etc/xrootd/sss.keytab ]; then
        grep -q 'sec.protocol sss' /etc/xrootd/xrootd-proxy.cfg || \
          sed -i '/sec.protocol unix/a\    sec.protocol sss -s /etc/xrootd/sss.keytab -c /etc/xrootd/sss.keytab -r 60 -g -p unix' /etc/xrootd/xrootd-proxy.cfg;
        sed -i 's/sec.protbind \* only unix/sec.protbind * only unix sss/' /etc/xrootd/xrootd-proxy.cfg;
      fi;
      sleep 5 && /srv/run.sh
      "
    ports:
      - "${xrootd_port}:1098"
    healthcheck:
      test: ["CMD", "xrdfs", "root://localhost:1098", "ls", "/home"]
      interval: 60s
      timeout: 5s
      retries: 3
      start_period: 10s
