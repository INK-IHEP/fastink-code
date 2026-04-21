services:
  fastink-redis-cron:
    image: ${cron_image}
    hostname: fastink-redis-cron
    restart: unless-stopped
    depends_on:
      fastink-redis:
        condition: service_started
      fastink-server:
        condition: service_healthy
    environment:
      TZ: ${timezone}
      REDIS_HOST: fastink-redis
      REDIS_PORT: 6379
      REDIS_PASSWORD: ${redis_password_yaml}
      MySQL_HOST: fastink-db
      MySQL_PORT: 3306
      MySQL_USER: ${db_user_yaml}
      MySQL_PASSWORD: ${db_password_yaml}
      MySQL_DATABASE: ${db_name_yaml}
      INK_CONFIG_FILE: /ink/config.yml
      SOURCE_COMMIT_SHA: ${source_commit_sha}
      SOURCE_COMMIT_DATE: ${source_commit_date}
      SOURCE_COMMIT_TAG: ${source_commit_tag}
      FASTINK_CRON_BASE_DIR: /opt/fastink-cron
      FASTINK_CRON_CONFIG: /opt/fastink-cron/cron.ini
      PLUGIN_PIP_PACKAGES: ${plugin_pip_packages}
      PLUGIN_EDITABLE_DIRS: ${plugin_editable_dirs}
      PRELOAD_SCRIPT_DIRS: ${cron_preload_script_dirs}
      PRELOAD_SCRIPTS: ${cron_preload_scripts}
    volumes:
      - ${config_path}:/ink/config.yml:ro
      - ${etc_init_dir}:/etc-init
      - ${plugins_dir}:/plugins
      - ${preload_cron_dir}:/opt/preload/cron:ro
      - ${cron_condor_conf_host_path}:/etc/condor/config.d/ink.conf:ro
${cron_krb5_conf_mount_block}
${cron_slurm_mounts_block}
${cron_extra_mounts_block}

  fastink-rootbrowse:
    image: ${rootbrowse_image}
    hostname: fastink-rootbrowse
    restart: unless-stopped
    depends_on:
      fastink-server:
        condition: service_healthy
    environment:
      ROOTBROWSE_PORT: ${rootbrowse_container_port}
      AUTHORIZED_KEYS_SOURCE: ${rootbrowse_authorized_keys_container_path}
      PRELOAD_SCRIPT_DIRS: ${rootbrowse_preload_script_dirs}
      PRELOAD_SCRIPTS: ${rootbrowse_preload_scripts}
    volumes:
      - ${etc_init_dir}:/etc-init
      - ${rootbrowse_authorized_keys_host_path}:${rootbrowse_authorized_keys_container_path}:ro
      - ${preload_rootbrowse_dir}:/opt/preload/rootbrowse:ro
${rootbrowse_extra_mounts_block}
    ports:
      - "${rootbrowse_port}:${rootbrowse_container_port}"
    tmpfs:
      - /dev/shm:rw,exec,nosuid,nodev,size=1g
    healthcheck:
      test: ["CMD", "/usr/local/bin/container-healthcheck.sh"]
      interval: 30s
      timeout: 5s
      retries: 1
