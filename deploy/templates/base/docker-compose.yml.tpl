services:
  fastink-db:
    image: mariadb:10.5
    restart: unless-stopped
    environment:
      MYSQL_ROOT_PASSWORD: ${db_root_password}
      MYSQL_DATABASE: ${db_name}
      MYSQL_USER: ${db_user}
      MYSQL_PASSWORD: ${db_password}
      TZ: ${timezone}
    volumes:
      - ${db_data_dir}:/var/lib/mysql
    healthcheck:
      test: ["CMD-SHELL", "mariadb-admin ping -h 127.0.0.1 -uroot -p${db_root_password}"]
      interval: 10s
      timeout: 5s
      retries: 10
      start_period: 20s

  fastink-redis:
    image: redis:7-alpine
    restart: unless-stopped
    command: ["redis-server", "--requirepass", ${redis_password_yaml}, "--appendonly", "yes"]
    volumes:
      - ${redis_data_dir}:/data

  fastink-server:
    image: ${server_image}
    restart: unless-stopped
    depends_on:
      fastink-db:
        condition: service_healthy
      fastink-redis:
        condition: service_started
    environment:
      INK_CONFIG_FILE: /ink/config.yml
      INK_PRODUCTION: ${ink_production}
      WORKERS: ${workers}
      INIT_DATABASE_ON_START: ${init_database}
      PLUGIN_PIP_PACKAGES: ${plugin_pip_packages}
      PLUGIN_EDITABLE_DIRS: ${plugin_editable_dirs}
      PRELOAD_SCRIPT_DIRS: ${server_preload_script_dirs}
      PRELOAD_SCRIPTS: ${server_preload_scripts}
    volumes:
      - ${config_path}:/ink/config.yml:ro
      - ${etc_init_dir}:/etc-init
      - ${tmp_dir}:/tmp/ink
      - ${plugins_dir}:/plugins
      - ${server_ssh_dir_host_path}:${server_ssh_dir_container_path}:ro
      - ${preload_server_dir}:/opt/preload/server:ro
${server_extra_mounts_block}
${server_port_block}
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://127.0.0.1:8000/health"]
      interval: 15s
      timeout: 5s
      retries: 10
      start_period: 20s
