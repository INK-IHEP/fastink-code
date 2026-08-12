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
#     YES → 1. Add a template variable placeholder here
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
  # Backends register by name (auth.type) via the auth-backend registry;
  # site backends (e.g. ihep_plugin's "hai") come from unified_plugins.
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
    # auth endpoints that are token-exempt but must stay behind the IP
    # whitelist: they return credentials, enumerate users, or create users.
    - /api/v2/auth/get_token
    - /api/v2/auth/get_permission
    - /api/v2/auth/get_users_by_permission
    - /api/v2/auth/get_user
    - /api/v2/auth/create_user
    - /api/v2/fs/shared_file
    - /api/v2/service/access_shared_rootfile
  skip_routers:
    - /api/v1/
    # anonymous auth entrypoints (no token available before login):
    - /api/v2/auth/create_token
    - /api/v2/auth/create_and_get_token
    - /api/v2/auth/validate_token
    - /api/v2/auth/auth_request
    # token-exempt but IP-controlled (see ip_controlled_routers above):
    - /api/v2/auth/get_token
    - /api/v2/auth/get_permission
    - /api/v2/auth/get_users_by_permission
    - /api/v2/auth/get_user
    - /api/v2/auth/create_user
    - /api/v2/status/
    - /health
    - /version
    - /status

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
  schedd_host: {{ schedd_host | to_yaml }}
  cm_host: {{ cm_host | to_yaml }}
  gateway_node: localhost
  # Root of the JobApp framework tree:
  #   <cluster_scripts>/apps/shell.sh                  ← shared launcher
  #   <cluster_scripts>/apps/<jobtype>/run.sh          ← per-app entrypoint
  #   <cluster_scripts>/apps/base.py + registry.py     ← JobApp abstraction
  # Plugin-shipped apps (fastink-plugins-ihep etc.) live under their own
  # Python package and are resolved via JobApp.run_script_path().
  cluster_scripts: /ink/src/fastink/computing
  interactive_job_time_limit: "24:00:00"
  nginx_node: {{ public_base_url | to_yaml }}
  # Per-user FastINK root. Supported placeholders: {username}, {user_group},
  # {experiment_group}, and {experiment_group_lower}. Use "~" for the
  # account's home directory.
  ink_dir: /home/{username}
  # Optional DNS suffix appended to bare node names in job listings
  # (e.g. ".example.org"). Empty = no suffix.
  node_domain_suffix: {{ node_domain_suffix | to_yaml }}
  # Slurm partitions counted in system-info summaries. Empty = all.
  system_info_partitions: []
  # NOTE: start_keywords / noenv_jobtype / iptables_jobtype are no longer
  # part of the config -- they are derived at runtime from the JobApp
  # classes under fastink.computing.apps (via fastink.computing.apps.registry).
  # NOTE: vnc_otp_script was removed; VNC OTP minting is done by the job's
  # own otp_start_listener (see apps/shell.sh) via FS-based RPC.

crond:
  submit_workers: []
  async_submit_retries: 3
  retry_delay_seconds: 10

job_time:
  walltime: 24
  check_interval: 900
  active_idle: 1800

# Site-specific per-jobtype overrides.  The generic defaults live on the
# JobApp classes under fastink.computing.apps.<name>.request_defaults;
# anything set here wins for that app at submit time.  Also carries
# app-specific extra config (herddisplay.herd_root/xml/bin, ...).
jobtype: {}

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
  # SSH private key used to reach the rootbrowse service node.
  ssh_private_key: /root/.ssh/id_rsa
  # Historical public URL prefix that may be embedded in stored URLs;
  # rewritten to computing.nginx_node when set. Empty = no rewriting.
  legacy_url_prefix: ""
  openclaw_user_root: /home/{username}
  openclaw_models_relpath: .openclaw
  # Scratch root for opencode/openchamber data directories. opencode keeps
  # its SQLite DB under ~/.local/share/opencode; AFS does not handle SQLite
  # locking well, so the on-AFS data dir is moved (or created fresh) under
  # {root}/.ink/opencode and symlinked back.
  code_datadir_root: /home/{username}
  # Site experiment -> extra bind mounts map for OpenClaw jobs. Empty = none.
  openclaw_exp_bind_map_file: ""
  # Site-provided OpenClaw container image (apptainer .sif). Required for
  # OpenClaw jobs.
  openclaw_container_image: ""
  # Extra provider templates offered in the OpenClaw UI (same shape as the
  # built-in templates; listed first, site keys win on conflict).
  openclaw_provider_templates: {}
  # Origins always allowed for the OpenClaw control UI.
  openclaw_allowed_origins: []
  # Base URL identifying "site-local" models (used by experiment-data
  # policies). Empty = no local models.
  openclaw_local_model_base_url: ""
  openclaw_local_model_prefix: ""

statistic:
  # Slurm server node name used to filter job statistics queries
  # (Elasticsearch "servernode" field). Empty = site statistics disabled.
  slurm_servernode: ""

status_page:
  # Enable the /status uptime page and its background prober (cron container).
  enabled: true
  # Server URL from the prober's (cron container's) point of view.
  target_url: http://fastink-server:8000
  # Points kept per probe (60 s interval x 1440 = 24 h).
  history_points: 1440
  # Home path used by the fs probe ({test_home} placeholder).
  test_home: ""
  # Cluster id used by the cr probe ({cluster} placeholder).
  probe_cluster: slurm
  probes:
    - name: health
      path: /health
      auth: none
    - name: version
      path: /version
      auth: none
    - name: auth_token
      path: /api/v2/auth/get_token?username={test_user}
      auth: none
    - name: fs_list
      path: /api/v2/fs/list_path?path={test_home}
      auth: test_user
    - name: cr_query
      path: /api/v2/cr/query_jobs?cluster_id={cluster}
      auth: test_user
    - name: fs_health
      path: /api/v2/status/get_fs_health
      auth: none

filesystem_health:
  # Container paths checked by /api/v2/status/get_fs_health. Each path is
  # verified to exist, be a directory, be readable, and be non-empty.
  # Empty by default (open-source deployments mount nothing site-specific).
  # Site overlays list their bind-mounted data paths here, e.g. the softlink
  # targets in fastink-dev/docker-compose.fs.yml (/home/bes, /besfs2, ...).
  paths: []
