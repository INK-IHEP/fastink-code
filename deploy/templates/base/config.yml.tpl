common:
  krb5_enabled: ${krb5_enabled}
  security_access: ${security_access}
  ip_whitelist_access: ${ip_whitelist_access}
  log_level: INFO
  log_path: /ink/ink.log

database:
  host: fastink-db
  port: 3306
  user: ${db_user_yaml}
  password: ${db_password_yaml}
  dbname: ${db_name_yaml}

redis:
  host: fastink-redis
  port: 6379
  password: ${redis_password_yaml}

auth:
  type: ${auth_type}
  issuer: ""
  client_id: null
  client_secret: ""

security:
  ip_whitelist:
    - 127.0.0.1
    - 172.16.0.0/12
    - 192.168.0.0/16
    - 10.0.0.0/8
  ip_controlled_routers:
    - /api/v1/
    - /api/v2/auth/get_token
    - /api/v2/fs/shared_file
    - /api/v2/service/access_shared_rootfile
  skip_routers:
    - /api/v1/
    - /api/v2/auth/
    - /health
    - /version

storage:
  xrd_host: ${xrd_host}
  fs_backend: ${fs_backend}
  max_file_size: ${max_file_size}

computing:
  site: ${site}
  cluster_list:
${cluster_list_block}
  iptables_jobtype: []
  noenv_jobtype:
${noenv_jobtype_block}
  schedd_host: ${schedd_host}
  cm_host: ${cm_host}
  gateway_node: ${gateway_node}
  cluster_scripts: ${cluster_scripts}
  interactive_job_time_limit: "24:00:00"
  nginx_node: ${public_base_url_yaml}
  ink_dir: ${ink_dir}
  start_keywords:
${start_keywords_block}

crond:
  submit_workers: []
  async_submit_retries: 3

jobtype:
${jobtype_defaults_block}

app:
  plugins: ${app_plugins}

plugins:
  router_plugins: ${router_plugins}

unified_plugins:
  packages: ${unified_plugin_packages}

service:
  service_node: ${service_node_yaml}
  service_port: ${service_port}
  ink_dir: ${ink_dir}
  monitor_url: ${public_base_url_yaml}
  job_monitor_url: ${public_base_url_yaml}
