# FastINK Image Publishing

This document defines how `ink-code` publishes official images and how
`fastink-dev` consumes them.

## Responsibilities

- `ink-code`
  - build clean publishable images
  - push them to `dockerhub.ihep.ac.cn/ink`
  - provide shared base/full templates and render logic
  - trigger downstream deployment in `fastink-dev`
- `fastink-dev`
  - keep only the IHEP overlay
  - render `base + full + ihep`
  - mount site configuration and secrets
  - mount private plugin source trees
  - mount preload scripts
  - start the production compose stack

## Published images

The publishing pipeline builds:

- `dockerhub.ihep.ac.cn/ink/fastink-init`
- `dockerhub.ihep.ac.cn/ink/fastink-server`
- `dockerhub.ihep.ac.cn/ink/fastink-redis-cron`
- `dockerhub.ihep.ac.cn/ink/fastink-rootbrowse`
- `dockerhub.ihep.ac.cn/ink/fastink-htcondor`

Image definitions live under:

- `deploy/images/init/`
- `deploy/images/server/`
- `deploy/images/cron/`
- `deploy/images/rootbrowse/`
- `deploy/images/htcondor/`

## Tagging rules

For `main` branch commits:

- build local images only on the shared runner host
- use fixed local tags:
  - `fastink-init:dev-local`
  - `fastink-server:dev-local`
  - `fastink-redis-cron:dev-local`
  - `fastink-rootbrowse:dev-local`
  - `fastink-htcondor:dev-local`
- do not push dev images to the registry

For release tags:

- immutable tag: `<CI_COMMIT_TAG>`
- moving tag: `latest`

## Downstream deployment variables

`ink-code/.gitlab-ci.yml` triggers `fastink-dev` and passes image names with:

- `FASTINK_SERVER_IMAGE`
- `FASTINK_CRON_IMAGE`
- `FASTINK_ROOTBROWSE_IMAGE`
- `FASTINK_HTCONDOR_IMAGE`

`fastink-dev` injects those values into its render step and deploys the
generated stack together with:

- `overlay/docker-compose.ihep.yml`
- `docker-compose.fs.yml`

For dev pipelines it also passes:

- `FASTINK_SKIP_PULL=true`

so the downstream job uses local images already built on the shared runner
machine.

`FASTINK_IMAGE_TAG` remains only as a fallback for manual deployment and local
testing. In CI-triggered deployments, the explicit image variables take
precedence.

## Site layering

Official images stay clean. Site-specific behavior must be layered at runtime:

- plugin sources:
  - mount under `/plugins/...`
  - set `PLUGIN_EDITABLE_DIRS`
- preload scripts:
  - mount under `/opt/preload/...`
  - set `PRELOAD_SCRIPT_DIRS` or `PRELOAD_SCRIPTS`
- sensitive runtime files:
  - overlay config fragments that render into the final `config.yml`
  - SSH authorized keys
  - keytabs
  - certificates
  - cluster-specific mounts

## Render contract

Public users interact with `deploy/install.py`, which persists a `.deploy/`
directory as durable deployment state.

IHEP automation does not maintain `.deploy/`. Instead, `fastink-dev` uses:

- `deploy/render_profile.py`
- `overlay/render.answers.json`
- `overlay/config.ihep.yml`
- `overlay/config.production.yml`
- `overlay/docker-compose.ihep.yml`

The final deployed compose stack is:

1. rendered full profile from `ink-code/deploy`
2. IHEP compose overlay
3. filesystem mount overlay

## Required CI variables

For `ink-code` publishing:

- `DOCKERHUB_USER`
- `DOCKERHUB_PASSWORD`

For release sync:

- `GITLAB_API_TOKEN`
- `GITHUB_TOKEN`
- `GITHUB_HTTP_PROXY` if needed by the runner

For `fastink-dev` deployment:

- registry credentials usable by `docker login`
- any site-specific overrides such as:
  - `FASTINK_DB_DATA_DEVICE`
  - `FASTINK_DB_PORT_BINDING`
  - `FASTINK_PLUGIN_EDITABLE_DIRS`

## Validation checklist

When changing image definitions or CI:

1. Build all three images locally from `ink-code`.
2. Confirm `fastink-dev` render output resolves with:
   `docker compose -f .generated/docker-compose.yml -f overlay/docker-compose.ihep.yml -f docker-compose.fs.yml config`
3. Confirm the downstream trigger passes explicit image names.
4. Confirm site plugin and preload mounts still match the container entrypoints.
