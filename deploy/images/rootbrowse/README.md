# Rootbrowse Image

This image is the clean, publishable rootbrowse runtime image.

Design rules:

- no SSH host keys are baked into the image
- no `authorized_keys` file is baked into the image
- SSH material is generated or mounted at container start

Runtime environment variables:

- `ROOTBROWSE_PORT`
- `AUTHORIZED_KEYS_SOURCE`
