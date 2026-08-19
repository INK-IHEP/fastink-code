# tools/

Operational and reference scripts for FastINK. These are **not** part of the
runtime package — they are run manually by operators/developers.

| Script | Purpose | Status |
|--------|---------|--------|
| `init_database.py` | Create DB tables and seed baseline permissions/users/authentications. Run once against a fresh database. | Active. **TODO**: still contains IHEP-specific users/permissions (guocq, hanx, physics group); to be moved to an overlay-driven seed once the open-source cleanup lands. |
| `test_auth.py` | Manual exercise of the permission decorator/functions (`has_permission`, `check_user_permission`). Reference for how the auth permission API is used. | Reference only — run manually against a configured environment. |
| `test_krb5_api.sh` | curl snippets for the auth token endpoints (`create_and_get_token`, `get_token`, `validate_token`). Replace `<username>`/`<token>` placeholders before use. | Reference only. |

## Usage

Scripts import `fastink.*`, so run them from inside the server container (or any
environment where `fastink` is installed and `INK_CONFIG_FILE` is set):

```bash
docker exec <server-container> python3 /ink/tools/init_database.py
```
