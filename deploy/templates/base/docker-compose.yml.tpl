services:
  fastink-db:
    image: {{ db_image }}
    hostname: fastink-db
    restart: unless-stopped
    logging:
      driver: json-file
      options:
        max-size: "100m"
        max-file: "3"
    environment:
      MYSQL_ROOT_PASSWORD: {{ db_root_password }}
      MYSQL_DATABASE: {{ db_name }}
      MYSQL_USER: {{ db_user }}
      MYSQL_PASSWORD: {{ db_password }}
      TZ: {{ timezone | to_yaml }}
    volumes:
      - {{ db_data_dir }}:/var/lib/mysql
    healthcheck:
      test: ["CMD-SHELL", "mariadb-admin ping -h 127.0.0.1 -uroot -p{{ db_root_password }}"]
      interval: 10s
      timeout: 5s
      retries: 10
      start_period: 20s

  fastink-redis:
    image: {{ redis_image }}
    hostname: fastink-redis
    restart: unless-stopped
    logging:
      driver: json-file
      options:
        max-size: "100m"
        max-file: "3"
    command: ["redis-server", "--requirepass", {{ redis_password | to_yaml }}, "--appendonly", "yes"]
    volumes:
      - {{ redis_data_dir }}:/data

  fastink-server:
    image: {{ server_image }}
    hostname: fastink-server
    restart: unless-stopped
    logging:
      driver: json-file
      options:
        max-size: "100m"
        max-file: "3"
    depends_on:
      fastink-db:
        condition: service_healthy
      fastink-redis:
        condition: service_started
    environment:
      INK_CONFIG_FILE: /ink/config.yml
      INK_PRODUCTION: {{ ink_production }}
      WORKERS: {{ workers }}
      INIT_DATABASE_ON_START: {{ init_database }}
      FASTINK_ADMIN_USERNAME: {{ admin_username | to_yaml }}
      INK_FILE_SHARE_JWT_SECRET: {{ file_share_jwt_secret | to_yaml }}
      SOURCE_COMMIT_SHA: {{ source_commit_sha }}
      SOURCE_COMMIT_DATE: {{ source_commit_date }}
      SOURCE_COMMIT_TAG: {{ source_commit_tag }}
      PLUGIN_PIP_PACKAGES: {{ plugin_pip_packages | to_yaml }}
      PLUGIN_EDITABLE_DIRS: {{ plugin_editable_dirs | to_yaml }}
      PRELOAD_SCRIPT_DIRS: {{ server_preload_script_dirs | to_yaml }}
      PRELOAD_SCRIPTS: {{ server_preload_scripts | to_yaml }}
    volumes:
      - {{ config_path }}:/ink/config.yml:ro
      - {{ server_log_dir }}:/ink/log
      - {{ etc_init_dir }}:/etc-init
      - {{ tmp_dir }}:/tmp/ink
      - {{ plugins_dir }}:/plugins
      - {{ server_ssh_dir_host_path }}:{{ server_ssh_dir_container_path }}:ro
      - {{ server_condor_conf_host_path }}:/etc/condor/config.d/ink.conf:ro
      - {{ preload_server_dir }}:/opt/preload/server:ro
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://127.0.0.1:8000/health"]
      interval: 15s
      timeout: 5s
      retries: 10
      start_period: 20s

{% if enable_accounts_sync == "true" %}
  # Keeps the container account view in sync with the host's account
  # database: users added/changed/removed on the host become visible to the
  # service containers on the next cycle (see deploy/images/init/).
  fastink-accounts-sync:
    image: {{ init_image }}
    hostname: fastink-accounts-sync
    restart: unless-stopped
    logging:
      driver: json-file
      options:
        max-size: "100m"
        max-file: "3"
    entrypoint:
      - /bin/sh
      - -c
      # The init image is published on release tags; until that image is
      # pulled this compose file may reference a script it does not contain.
      # Idle with a clear log instead of entering a restart loop.
      - |
        if [ ! -x /usr/local/bin/sync-accounts-loop.sh ]; then
          echo "[accounts-sync] init image predates the account sync script; idling until a release image is published"
          exec sleep infinity
        fi
        exec /usr/local/bin/sync-accounts-loop.sh
    environment:
      SLEEP_TIME: "{{ accounts_sync_interval }}"
      FASTINK_ENABLE_LOCAL_HTCONDOR: "{{ enable_local_htcondor }}"
      FASTINK_ENABLE_HOST_SLURM_CLIENT: "{{ enable_host_slurm_client }}"
    volumes:
      # Read-only host account database; writable merged view consumed by
      # the service containers at /etc-init.
      - /etc:/host-etc:ro
      - {{ etc_init_dir }}:/etc-init
{% endif %}
