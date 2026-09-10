"""Seed-file parsing and migration index-filter tests (no DB required)."""
import json

import pytest

from tools.init_database import _load_seed
from tools.migrate_drop_email import _email_index_names


def _seed_env(monkeypatch, tmp_path, content=None, *, as_env=True, as_dir=False):
    """Point the seed config at a tmp file/dir with optional content."""
    seed = tmp_path / "seed-users.json"
    if as_dir:
        seed.mkdir()
    elif content is not None:
        seed.write_text(content)
    if as_env:
        monkeypatch.setenv("FASTINK_SEED_USERS_FILE", str(seed))
    else:
        monkeypatch.delenv("FASTINK_SEED_USERS_FILE", raising=False)
        monkeypatch.setattr("tools.init_database.DEFAULT_SEED_FILE", seed)
    return seed


class TestLoadSeed:
    def test_unconfigured_returns_root_only(self, monkeypatch, tmp_path):
        monkeypatch.delenv("FASTINK_SEED_USERS_FILE", raising=False)
        monkeypatch.setattr(
            "tools.init_database.DEFAULT_SEED_FILE", tmp_path / "absent.json"
        )
        assert _load_seed() == {
            "users": [{"username": "root"}],
            "group_permissions": [],
        }

    def test_valid_list_form(self, monkeypatch, tmp_path):
        _seed_env(monkeypatch, tmp_path, json.dumps([{"username": "alice", "uid": 1001}]))
        assert _load_seed() == {
            "users": [{"username": "alice", "uid": 1001}],
            "group_permissions": [],
        }

    def test_valid_dict_form(self, monkeypatch, tmp_path):
        _seed_env(
            monkeypatch,
            tmp_path,
            json.dumps({
                "users": [{"username": "alice", "permissions": ["admin"]}],
                "group_permissions": [{"group_name": "physics", "permission": "CentOS7"}],
            }),
        )
        assert _load_seed() == {
            "users": [{"username": "alice", "permissions": ["admin"]}],
            "group_permissions": [{"group_name": "physics", "permission": "CentOS7"}],
        }

    def test_dict_form_without_users_key_raises(self, monkeypatch, tmp_path):
        _seed_env(monkeypatch, tmp_path, json.dumps({"username": "alice"}))
        with pytest.raises(RuntimeError, match="requires a 'users' list"):
            _load_seed()

    def test_configured_but_missing_file_raises(self, monkeypatch, tmp_path):
        monkeypatch.setenv(
            "FASTINK_SEED_USERS_FILE", str(tmp_path / "missing.json")
        )
        with pytest.raises(RuntimeError, match="not a readable file"):
            _load_seed()

    def test_default_path_directory_raises(self, monkeypatch, tmp_path):
        _seed_env(monkeypatch, tmp_path, as_env=False, as_dir=True)
        with pytest.raises(RuntimeError, match="not a readable file"):
            _load_seed()

    def test_malformed_json_raises(self, monkeypatch, tmp_path):
        _seed_env(monkeypatch, tmp_path, "{not json")
        with pytest.raises(json.JSONDecodeError):
            _load_seed()

    def test_scalar_json_raises(self, monkeypatch, tmp_path):
        _seed_env(monkeypatch, tmp_path, json.dumps("alice"))
        with pytest.raises(RuntimeError, match="JSON list or object"):
            _load_seed()

    def test_group_permissions_not_a_list_raises(self, monkeypatch, tmp_path):
        _seed_env(
            monkeypatch, tmp_path,
            json.dumps({"users": [], "group_permissions": {"group_name": "x"}}),
        )
        with pytest.raises(RuntimeError, match="'group_permissions' must be a list"):
            _load_seed()

    def test_entry_without_username_raises(self, monkeypatch, tmp_path):
        _seed_env(monkeypatch, tmp_path, json.dumps([{"uid": 1001}]))
        with pytest.raises(RuntimeError, match="Invalid seed user entry"):
            _load_seed()

    def test_whitespace_username_raises(self, monkeypatch, tmp_path):
        _seed_env(monkeypatch, tmp_path, json.dumps([{"username": "   "}]))
        with pytest.raises(RuntimeError, match="Invalid seed user entry"):
            _load_seed()

    def test_negative_uid_raises(self, monkeypatch, tmp_path):
        _seed_env(monkeypatch, tmp_path, json.dumps([{"username": "alice", "uid": -1}]))
        with pytest.raises(RuntimeError, match="non-negative integer"):
            _load_seed()

    def test_float_uid_raises(self, monkeypatch, tmp_path):
        _seed_env(monkeypatch, tmp_path, json.dumps([{"username": "alice", "uid": 1.5}]))
        with pytest.raises(RuntimeError, match="non-negative integer"):
            _load_seed()

    def test_string_uid_raises(self, monkeypatch, tmp_path):
        _seed_env(monkeypatch, tmp_path, json.dumps([{"username": "alice", "uid": "1001"}]))
        with pytest.raises(RuntimeError, match="non-negative integer"):
            _load_seed()

    def test_boolean_uid_raises(self, monkeypatch, tmp_path):
        _seed_env(monkeypatch, tmp_path, json.dumps([{"username": "alice", "uid": True}]))
        with pytest.raises(RuntimeError, match="non-negative integer"):
            _load_seed()

    def test_permissions_not_a_list_raises(self, monkeypatch, tmp_path):
        _seed_env(
            monkeypatch, tmp_path,
            json.dumps([{"username": "alice", "permissions": "admin"}]),
        )
        with pytest.raises(RuntimeError, match="Invalid permissions"):
            _load_seed()

    def test_permissions_with_blank_string_raises(self, monkeypatch, tmp_path):
        _seed_env(
            monkeypatch, tmp_path,
            json.dumps([{"username": "alice", "permissions": ["admin", "  "]}]),
        )
        with pytest.raises(RuntimeError, match="Invalid permissions"):
            _load_seed()

    def test_group_permission_entry_missing_fields_raises(self, monkeypatch, tmp_path):
        _seed_env(
            monkeypatch, tmp_path,
            json.dumps({"users": [], "group_permissions": [{"permission": "CentOS7"}]}),
        )
        with pytest.raises(RuntimeError, match="Invalid group_permission entry"):
            _load_seed()

    def test_group_permission_blank_group_name_raises(self, monkeypatch, tmp_path):
        _seed_env(
            monkeypatch, tmp_path,
            json.dumps({"users": [], "group_permissions": [{"group_name": "  ", "permission": "CentOS7"}]}),
        )
        with pytest.raises(RuntimeError, match="Invalid group_permission entry"):
            _load_seed()


class TestEmailIndexNames:
    def test_selects_exact_email_column_index(self):
        indexes = [
            {"name": "email", "column_names": ["email"]},
            {"name": "nonemail_lookup", "column_names": ["username"]},
            {"name": "email_and_uid", "column_names": ["email", "uid"]},
        ]
        assert _email_index_names(indexes) == ["email"]

    def test_empty_when_no_email_index(self):
        assert _email_index_names([{"name": "u", "column_names": ["username"]}]) == []
