# =============================================================================
# FastINK Application Configuration — Canonical Defaults
# =============================================================================
#
# This file is the single source of truth for ALL config keys.
# Every key that get_config() reads at runtime must have a default here.
#
# HOW TO ADD A NEW CONFIG KEY
# ===========================
#
#   Does the value need to be provided by the person running `fastinkctl deploy`?
#   (e.g. database password, hostname, port number)
#     YES → 1. Add {{ var }} placeholder here
#            2. Add it to the questionnaire: deploy/lib/questionnaire.py
#            3. Add a mapping entry: deploy/lib/render.py:build_mapping()
#     NO  → Just hardcode the default value directly in this file.
#
#   Will different sites (IHEP, HAI, HEPS) need different values?
#     YES → Put the generic default here, then override it in the
#           site overlay (fastink-dev/overlay/config.ihep.yml).
#
# LAYERING (deep-merge order)
# ===========================
#   this template (after rendering)
#     → + site overlay (e.g. fastink-dev/overlay/config.ihep.yml)
#       → + dev overlay (e.g. .vscode/.generated/config.dev.overlay.yml)
#         = final config.yml
#
# ⚠  Overlays must ONLY override keys that already exist here.
#    Never add a new key exclusively in an overlay.
# =============================================================================

common:
  krb5_enabled: {{ krb5_enabled }}
  security_access: false
  ip_whitelist_access: false
  log_level: INFO
  log_format: "%(asctime)s - %(name)s - %(levelname)s - %(module)s.%(funcName)s (line %(lineno)d): %(message)s"
  log_datefmt: "%Y-%m-%d %H:%M:%S"
  log_path: /ink/ink.log

database:
  host: fastink-db
  port: 3306
  user: {{ db_user | to_yaml }}
  password: {{ db_password | to_yaml }}
  dbname: {{ db_name | to_yaml }}

redis:
  host: fastink-redis
  port: 6379
  password: {{ redis_password | to_yaml }}

auth:
  type: {{ auth_type | to_yaml }}
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
  xrd_host: {{ xrd_host | to_yaml }}
  fs_backend: xrootd
  max_file_size: 2147483648

computing:
  site: generic
  cluster_list:
{% for item in cluster_list %}
    - {{ item | to_yaml }}
{% endfor %}
  iptables_jobtype: []
  noenv_jobtype:
{% for item in noenv_jobtype %}
    - {{ item | to_yaml }}
{% endfor %}
  schedd_host: {{ schedd_host | to_yaml }}
  cm_host: {{ cm_host | to_yaml }}
  gateway_node: localhost
  cluster_scripts: /ink/src/fastink/computing/scripts
  interactive_job_time_limit: "24:00:00"
  nginx_node: {{ public_base_url | to_yaml }}
  ink_dir: /home/{username}
  start_keywords:
{% for keyword in start_keywords %}
    - {{ keyword | to_yaml }}
{% endfor %}

crond:
  submit_workers: []
  async_submit_retries: 3
  retry_delay_seconds: 10

job_time:
  walltime: 24
  check_interval: 900
  active_idle: 1800

jobtype:
{% for name, config in jobtype_defaults.items() %}
  {{ name | to_yaml }}:
    htc:
      RequestMemory: {{ config.htc.RequestMemory }}
      RequestCpus: {{ config.htc.RequestCpus }}
      walltime: "default"
      schedd_host: {{ config.htc.schedd_host | to_yaml }}
      cm_host: {{ config.htc.cm_host | to_yaml }}
      extra_param: true
{% endfor %}

hooks:
  modules: ""

app:
  plugins: ""

plugins:
  router_plugins: ""

unified_plugins:
  packages: ""

service:
  service_node: fastink-rootbrowse
  service_port: 2000
  ink_dir: /home/{username}
  monitor_url: {{ public_base_url | to_yaml }}
  job_monitor_url: {{ public_base_url | to_yaml }}
  rootbrowse_script: /dev/shm/start-rootbrowse.sh
  rootbrowse_check_script: /dev/shm/check-rootbrowse.sh
  openclaw_user_root: /scratchfs/{experiment_group_lower}/{username}
  openclaw_models_relpath: .openclaw
  openclaw_exp_bind_map_file: /afs/ihep.ac.cn/soft/common/sysgroup/exp_file.json
