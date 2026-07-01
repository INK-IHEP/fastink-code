CONDOR_HOST = {{ cm_host }}

SEC_DEFAULT_AUTHENTICATION_METHODS = {{ htcondor_auth_method }}
SEC_CLAIMTOBE_INCLUDE_DOMAIN = True

DAEMON_LIST = MASTER

FILESYSTEM_DOMAIN = {{ htcondor_fs_domain }}
UID_DOMAIN = {{ htcondor_uid_domain }}
