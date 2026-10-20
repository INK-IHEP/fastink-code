# tools/

Operational and reference scripts for FastINK. `init_database.py` and
`migrate_drop_email.py` run automatically on server container startup
(see `deploy/images/server/start-ink.sh`); the remaining scripts are run
manually by operators/developers.

| Script | Purpose | Status |
|--------|---------|--------|
| `init_database.py` | Create DB tables and seed baseline permissions/users/authentications. Supports optional seed-users JSON file (`FASTINK_SEED_USERS_FILE` or `/opt/fastink/seed-users.json`); defaults to root-only. | Active — runs on container startup. |
| `migrate_drop_email.py` | Idempotent migration: drop the `email` column from `users`. Safe to re-run; no-ops when the column is already gone. | Active — runs on container startup after `init_database.py`; manual runs also supported for out-of-band schema convergence. |
| `test_auth.py` | Manual exercise of the permission decorator/functions (`has_permission`, `check_user_permission`). Reference for how the auth permission API is used. | Reference only — run manually against a configured environment. |
| `test_krb5_api.sh` | curl snippets for the auth token endpoints (`create_and_get_token`, `get_token`, `validate_token`). Replace `<username>`/`<token>` placeholders before use. | Reference only. |

## Usage

Scripts import `fastink.*`, so run them from inside the server container (or any
environment where `fastink` is installed and `INK_CONFIG_FILE` is set):

```bash
docker exec <server-container> python3 /ink/tools/init_database.py
```
