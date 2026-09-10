"""init_database must never write the database password to any log."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


class FakeLogger:
    def __init__(self):
        self.messages = []

    def __getattr__(self, name):
        def _log(msg, *args, **kwargs):
            rendered = str(msg) % args if args else str(msg)
            self.messages.append((name, rendered))

        return _log


@pytest.fixture
def init_mod(monkeypatch):
    tools_dir = Path(__file__).resolve().parent.parent / "tools"
    spec = importlib.util.spec_from_file_location(
        "init_database_under_test", tools_dir / "init_database.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    engine_urls = []
    fake_logger = FakeLogger()
    monkeypatch.setattr(
        mod, "get_config",
        lambda section: {
            "host": "dbhost", "port": 3306, "user": "appuser",
            "password": "SUPERSECRET", "dbname": "inkdb",
        },
    )
    monkeypatch.setattr(
        mod, "create_engine",
        lambda url: (engine_urls.append(url), SimpleNamespace())[1],
    )
    monkeypatch.setattr(
        mod, "BASE",
        SimpleNamespace(metadata=SimpleNamespace(create_all=lambda engine: None)),
    )
    monkeypatch.setattr(
        mod, "common",
        SimpleNamespace(
            get_permission=lambda perm: {"id": 1},
            get_user=lambda **kwargs: None,
            get_authentication=lambda name: object(),
            add_group_permission=lambda **kwargs: None,
        ),
    )
    monkeypatch.setattr(
        mod, "_load_seed",
        lambda: {"users": [], "group_permissions": []},
    )
    monkeypatch.setattr(mod, "logger", fake_logger)
    mod.engine_urls = engine_urls
    return mod


def test_password_never_logged_but_engine_still_gets_it(init_mod):
    init_mod.init_db()

    assert len(init_mod.engine_urls) == 1
    assert "SUPERSECRET" in init_mod.engine_urls[0], "engine must still connect with password"

    logged = " ".join(msg for _, msg in init_mod.logger.messages)
    assert "SUPERSECRET" not in logged
    assert "dbhost" in logged  # sanity: connection info still logged
