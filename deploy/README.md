# FastINK Deploy

`ink-code/deploy/` is the shared deployment layer for FastINK.

It serves two kinds of users:

- open-source users who want to install FastINK directly
- site overlay repositories such as `../fastink-dev/`

This directory is responsible for three things:

- defining the official image build inputs
- providing the shared render core and host-side checks
- providing both interactive and non-interactive deployment entrypoints

## Directory Layout

- `images/`
  Official image definitions. Current images include `init`, `server`, `cron`, `rootbrowse`, and the optional local `htcondor` image.
- `lib/`
  Shared deployment core, including defaults, path planning, host checks, and rendering.
- `templates/`
  Layered templates.
  - `base/`: common templates
  - `profiles/`: `minimal` and `full` profile overlays
  - `extras/`: optional capabilities such as `nginx`, `xrootd`, and local `htcondor`
- `install.py`
  Interactive CLI for open-source users.
- `render_profile.py`
  Non-interactive render entrypoint for CI and site overlays.
- `check_host.py`
  Host precheck entrypoint.

## Open-Source Deployment Flow

Run from the `ink-code/` root directory:

```bash
python deploy/install.py
```

or:

```bash
./deploy/bin/fastink-deploy
```

To reuse an existing `.deploy/` directory without rerunning the questionnaire:

```bash
python deploy/install.py --reuse
```

To see the CLI help:

```bash
python deploy/install.py --help
```

The flow is:

1. Run host prechecks.
   - check `docker`
   - check `docker compose`
   - check `/cvmfs`
   - warm the `/cvmfs` paths that FastINK actually needs
2. Choose a profile.
   - `minimal`: the FastINK core deployment
   - `full`: `minimal` plus more optional infrastructure enabled by default
3. Choose an image source.
   - default is `pull`, which uses the official images
   - `build` is still available if the user wants to build locally from `deploy/images/`
4. Fill the interactive runtime parameters.
   - when local HTCondor is enabled, the questionnaire also asks for an internal HTCondor domain used for shared-filesystem semantics
5. Generate the persistent `.deploy/` directory.
6. Render `config.yml`, `docker-compose.yml`, `.env`, keys, and runtime directories.
7. Run `docker build` or `docker pull` for the official images, including the one-shot `fastink-init` image.
8. Run the one-shot `fastink-init` container to generate deployment assets such as SSH keys, self-signed TLS certificates, and `sss.keytab`.
9. Run `docker compose up -d` and wait for the health check.

At render time, shared deploy also reads the checked-out FastINK source tree and injects:

- `SOURCE_COMMIT_SHA`
- `SOURCE_COMMIT_DATE`
- `SOURCE_COMMIT_TAG`

into the runtime container environment. This keeps the `/version` router aligned
with the exact `fastink-code` checkout used to render the deployment.

`--reuse` skips steps 2 through 8. It reuses the existing `.deploy/answers.json` and `.deploy/docker-compose.yml`, then runs `docker compose up -d` again with the saved project name.

## Preparation Notes Before Starting The CLI

Before running `python deploy/install.py`, users may want to prepare some optional inputs in advance.

Typical optional inputs include:

- an extra mount list file
- plugin source or plugin packages
- preload scripts
- an existing TLS certificate and private key
- a Kerberos keytab for xrootd if Kerberos-backed xrootd is required

The installer prints these preparation notes at startup so that users know what can be prepared ahead of time.

If `.deploy/answers.json` already exists, the interactive questionnaire also accepts `r` on prompts to reuse the saved value for that field.

## Host Commands Required

The interactive installer now keeps most one-shot asset generation inside the `fastink-init` container. The host only needs commands for host-side checks and Docker control:

Required on the host:

- `python3`
- `docker`
- `docker compose`
- `mountpoint`

Also required as host infrastructure rather than commands:

- `/cvmfs` must already be mounted
- the required `/cvmfs` paths must be readable on the host

The host no longer needs these commands just to complete initialization:

- `ssh-keygen`
- `openssl`
- `xrdsssadmin`

Those are now executed inside the one-shot `fastink-init` container.

## Optional Extra Mount List

The interactive installer now supports an optional extra mount list file.

By default, the installer creates `.deploy/extra-mounts.txt` if it does not already exist. Its initial content is:

```text
/home/:/home/
```

If enabled during the questionnaire, the user provides a file path. Each non-empty, non-comment line in that file is treated as a Docker volume rule.

Supported line format:

```text
/host/path:/container/path
/host/path:/container/path:ro
```

Lines starting with `#` are ignored.

A typical suggested location is:

```text
.deploy/extra-mounts.txt
```

Current behavior:

- the same extra mounts are applied to:
  - `fastink-server`
  - `fastink-redis-cron`
  - `fastink-rootbrowse`
  - `fastink-xrootd`
  - `fastink-htcondor` when local HTCondor is enabled
- this is intended for generic runtime filesystem mounts that should be visible in the main service containers

This mechanism is intentionally simpler than asking users to edit raw compose YAML during the interactive flow.

At the moment, the interactive installer does not provide a dedicated `--set key=value` or "edit one saved parameter" mode. If a user wants to change one parameter only, the current choices are:

- rerun `python deploy/install.py` and answer the questionnaire again
- edit the generated files under `.deploy/` manually
- use `python deploy/install.py --reuse` only when the existing `.deploy/` configuration should be reused as-is

## Profiles And Service Layers

The current profile meaning is:

- `minimal`
  - `fastink-db`
  - `fastink-redis`
  - `fastink-server`
  - `fastink-redis-cron`
  - `fastink-rootbrowse`
- `full`
  - inherits the complete `minimal` stack
  - then enables more optional infrastructure by default

Current optional extras:

- `enable_nginx`
  - adds `fastink-nginx`
  - provides an HTTPS entrypoint in front of `fastink-server`
- `enable_xrootd`
  - adds `fastink-xrootd`
- `enable_local_htcondor`
  - adds `fastink-htcondor`
  - starts a single-container HTCondor all-in-one pool for local/open-source testing
  - automatically points `schedd_host` to `schedd@fastink-htcondor` and `cm_host` to `fastink-htcondor`

## Local HTCondor

When `enable_local_htcondor` is enabled, deploy starts one `fastink-htcondor`
container that combines:

- collector
- negotiator
- schedd
- execute/startd

Integration rules:

- it uses the same shared `/etc-init` account view as `server`, `rootbrowse`, and `xrootd`
- it receives the same extra mount list entries as the main runtime containers
- if no extra mount list is specified, deploy falls back to `.deploy/extra-mounts.txt`, whose default content is `/home/:/home/`
- the installer asks once for `HTCondor internal domain`
- deploy writes that value into both:
  - `.deploy/condor/ink.conf` for the FastINK client containers
  - `.deploy/condor/htcondor.local.conf` for the local HTCondor AIO container
- this enables shared-filesystem behavior through matching `FILESYSTEM_DOMAIN` and `UID_DOMAIN`
- the installer prompts once for the default HTCondor job CPU and memory values
- those defaults are reused for `vscode`, `jupyter`, `vnc`, and `rootbrowse`
- the default values are `1` CPU and `6000` MB
- it is intended for local/open-source testing, not for a multi-node Condor cluster deployment

### Shared Filesystem Semantics

For local HTCondor AIO, FastINK assumes that the submit side and execute side
share the same mounted job directory tree.

Current generic deploy behavior is:

- `fastink-server` and `fastink-htcondor` receive the same extra mount list
- if local HTCondor or xrootd is enabled and no mount list is provided, deploy
  auto-enables `.deploy/extra-mounts.txt`
- the default initial content is:

```text
/home/:/home/
```

- the internal domain value provided during install is used as:
  - `FILESYSTEM_DOMAIN`
  - `UID_DOMAIN`

This is important for HTCondor job execution. Without matching shared-fs domain
settings, HTCondor treats the execute side as outside the submit filesystem
domain and falls back to execute sandboxes such as `/var/lib/condor/execute/...`.

### CVMFS-backed Job Types In Local HTCondor

The local HTCondor AIO container now mounts:

```text
/cvmfs:/cvmfs:ro
```

This is enough for job types whose runtime is already hosted in CVMFS.

Current local validation status:

- `jupyter`
  - validated in local HTCondor AIO
  - launches from CVMFS-backed Jupyter environments
- `rootbrowse`
  - validated in local HTCondor AIO
  - launches ROOT/browser runtime from CVMFS-backed software stacks
- `vscode`
  - still requires `/usr/bin/code-server` inside the execute image
  - not solved by the `/cvmfs` mount alone

So, at the moment:

- CVMFS-backed job types can be validated with local HTCondor AIO
- image-backed job types still depend on the software baked into the execute image

## What `.deploy/` Contains

`.deploy/` is the persistent deployment asset for open-source users. It should usually be kept together with the deployment so that it can be copied to another machine later.

Typical contents include:

- `config.yml`
- `docker-compose.yml`
- `.env`
- `answers.json`
- `runtime/`
- `keys/`
- `plugins/`
- `preload/`
- `xrootd/`
- `nginx/`
- `extra-mounts.txt` (optional)

The files that users should normally maintain directly are mainly:

- `answers.json`
- `plugins/`
- `preload/`
- runtime materials under `xrootd/` and `nginx/`
- `extra-mounts.txt` when extra host paths need to be mounted into the runtime containers

Generated outputs such as `config.yml` and `docker-compose.yml` are better regenerated than edited long-term by hand.

## Runtime Materials The User Must Handle

### 1. SSH Client Key

Deploy automatically generates:

- `.deploy/keys/ssh-client/id_rsa`
- `.deploy/keys/ssh-client/id_rsa.pub`

This key pair is used by `fastink-server` when accessing:

- `rootbrowse`
- condor nodes
- Slurm nodes
- login nodes

`rootbrowse` host keys are generated by the container itself.

Users must deploy `.deploy/keys/ssh-client/id_rsa.pub` to the appropriate remote account `authorized_keys`.

### 2. nginx TLS Certificate

If `nginx` is enabled, deploy maintains these files under `.deploy/nginx/`:

- `default.conf`
- `cert.pem`
- `key.pem`

Rules:

- if the user provides an existing certificate and key, deploy copies them into `.deploy/nginx/`
- otherwise deploy generates a self-signed certificate so nginx can still provide HTTPS encryption

### 3. xrootd Keytabs

If Kerberos is enabled, deploy mounts the host `krb5.conf` into the relevant containers. The interactive installer asks for the host path and defaults to `/etc/krb5.conf`.

If `xrootd` is enabled, deploy prepares:

- `.deploy/xrootd/sss.keytab`
- `.deploy/xrootd/krb5.keytab`

Rules:

- `sss.keytab`
  - is generated by the one-shot `fastink-init` container
  - does not require `xrdsssadmin` on the host
- `krb5.keytab`
  - is never auto-generated
  - for Kerberos-enabled xrootd, the installer asks for a host source path and mounts that file into `/etc/xrootd/krb5.keytab`
  - the installer also asks for the xrootd service principal used by the xrootd krb5 config

### 4. Slurm Host Environment

If FastINK needs Slurm access, the host must already provide a working Slurm client environment, including at least:

- `sbatch`
- `sacct`
- `scontrol`
- `scancel`
- a usable `munge` socket
- a valid `slurm.conf`

The official images provide the container-side client packages. Host-side Slurm configuration and authentication are still the user's responsibility.

If you enable `Expose host Slurm client config and munge socket` in the interactive installer, deploy will mount the host:

- `slurm.conf`
- `/var/run/munge`

into both `fastink-server` and `fastink-redis-cron`, so Slurm-backed runtime and cron jobs can use the same client state.

## Official Images And yum Repositories

Open-source users should normally use the official images.

The official image build inputs are defined under `deploy/images/`. Current image builds use IHEP-managed yum repositories where needed, including the IHEP mirror for `slurm`. The official image set now also includes `fastink-init`, which is used only for one-shot deployment initialization.

If a user wants to rebuild images locally or install extra RPMs, they should review and adapt:

- `deploy/images/repos/`

## Non-Interactive Render Flow

`render_profile.py` is the non-interactive entrypoint used by CI and site overlays. It is not the main long-term interface for open-source users.

Typical usage:

```bash
python deploy/render_profile.py \
  --profile full \
  --answers-file /path/to/render.answers.json \
  --output-dir /path/to/output \
  --config-overlay /path/to/site-config.yml
```

Its responsibilities are:

1. read an answers file
2. apply `--set` overrides
3. plan runtime directories
4. render base templates, profile overlays, and extras
5. output final `config.yml`, `docker-compose.yml`, and `.env`

`fastink-dev` consumes `deploy` through this path.

## Where To Change Things

By change type:

- public service topology and common compose structure: `templates/base/`
- `minimal` / `full` semantics: `templates/profiles/`
- optional capabilities such as `nginx` or `xrootd`: `templates/extras/`
- default values and profile defaults: `lib/defaults.py`
- runtime directory planning: `lib/paths.py`
- host prechecks and `/cvmfs` warmup: `lib/host_runtime.py`
- render and merge behavior: `lib/render.py`
- official image contents: `images/`
- interactive install UX: `install.py`
- CI / site render entrypoint: `render_profile.py`

The rule is simple:

- generic deployment semantics stay in `ink-code/deploy`
- site-specific differences should stay in overlay repositories such as `fastink-dev`
