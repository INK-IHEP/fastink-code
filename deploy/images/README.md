# Deploy Images

This directory is for publishable FastINK container images.

Target split:

- `init/`: one-shot deployment initialization image
- `server/`: main FastINK API image
- `cron/`: Redis-backed scheduled task image
- `rootbrowse/`: ROOT browser image
- `htcondor/`: optional local HTCondor all-in-one image
- `repos/`: shared RPM repository definitions for image builds that need them

Design rules:

- keep image definitions in `ink-code`
- keep secrets and site configuration out of the image
- allow official site deployments to layer plugins and runtime files through `docker-compose`
- keep public deployment templates minimal even when the published images support the full stack
- site deployments should consume these image definitions through `ink-code/deploy` and `fastink-dev` overlays

Plugin support for the server image is runtime-driven:

- `PLUGIN_PIP_PACKAGES` installs wheel files or package specs
- `PLUGIN_EDITABLE_DIRS` installs mounted source trees in editable mode

The cron image follows the same layering model. It only ships generic jobs by
default and expects site-specific jobs to be mounted at runtime.

## Release contract

`ink-code/.gitlab-ci.yml` is the publishing pipeline for these images.

Tagging policy:

- branch `main`
  - build local runner images only
  - `fastink-init:dev-local`
  - `fastink-server:dev-local`
  - `fastink-redis-cron:dev-local`
  - `fastink-rootbrowse:dev-local`
  - `fastink-htcondor:dev-local`
  - downstream `fastink-dev` consumes them from the same machine without pull
- git tag `<release>`
  - publish `fastink-init:<release>`
  - publish `fastink-server:<release>`
  - publish `fastink-redis-cron:<release>`
  - publish `fastink-rootbrowse:<release>`
  - publish `fastink-htcondor:<release>`
  - also update `latest`

Consumer contract with `fastink-dev`:

- `ink-code` triggers `fastink-dev` with full image references
- `fastink-dev` renders a full base stack and deploys those exact image references
- `fastink-dev` remains responsible for site runtime files, private plugin
  sources, preload scripts, and environment-specific compose settings
