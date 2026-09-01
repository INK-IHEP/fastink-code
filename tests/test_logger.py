"""Tests for fastink.common.logger.

Contract:
- single file sink: ConcurrentTimedRotatingFileHandler, daily rotation,
  gzip compression, unlimited retention (backupCount=0)
- no console handler
- level from config with INFO fallback on invalid value
- log directory auto-created
- logger name "ink", propagates to root (caplog compatibility)
"""
import importlib
import logging
from datetime import datetime

import pytest

from concurrent_log_handler import ConcurrentTimedRotatingFileHandler

LOGGER_MOD = "fastink.common.logger"


@pytest.fixture
def reload_logger(tmp_path, monkeypatch):
    """Reload the logger module bound to a temp config; restore afterwards."""

    def _load(level="INFO", log_path=None):
        cfg = tmp_path / "config.yml"
        cfg.write_text(
            f"common:\n  log_level: {level}\n  log_path: {log_path or tmp_path / 'ink.log'}\n"
        )
        monkeypatch.setenv("INK_CONFIG_FILE", str(cfg))
        return importlib.reload(importlib.import_module(LOGGER_MOD))

    yield _load
    monkeypatch.undo()
    importlib.reload(importlib.import_module(LOGGER_MOD))


def test_single_daily_gzip_file_handler(reload_logger):
    reload_logger("INFO")
    ink = logging.getLogger("ink")
    assert ink.name == "ink"
    assert ink.level == logging.INFO
    assert len(ink.handlers) == 1
    handler = ink.handlers[0]
    assert isinstance(handler, ConcurrentTimedRotatingFileHandler)
    assert handler.when == "MIDNIGHT"
    assert handler.clh.use_gzip is True
    assert handler.backupCount == 0
    assert handler.encoding == "utf-8"


def test_propagates_to_root_for_caplog(reload_logger):
    reload_logger()
    assert logging.getLogger("ink").propagate is True


def test_level_read_from_config(reload_logger):
    reload_logger("DEBUG")
    assert logging.getLogger("ink").level == logging.DEBUG


def test_invalid_level_falls_back_to_info(reload_logger):
    reload_logger("BOGUS")
    assert logging.getLogger("ink").level == logging.INFO


def test_log_directory_created_and_writable(reload_logger, tmp_path):
    log_path = tmp_path / "deep" / "nested" / "ink.log"
    reload_logger(log_path=str(log_path))
    assert log_path.parent.is_dir()
    logging.getLogger("ink").info("hello-logger-test")
    assert log_path.exists()
    assert "hello-logger-test" in log_path.read_text()


def test_rollover_computed_at_local_midnight(reload_logger):
    """Behavior contract: rollover happens at midnight, not N hours after init.

    Regression test for the when="D" mistake — "D" means every-24h-from-init,
    only "MIDNIGHT" aligns the rollover to 00:00 local time.
    """
    reload_logger()
    handler = logging.getLogger("ink").handlers[0]
    for hour in (0, 7, 15, 23):
        now = int(datetime(2026, 9, 1, hour, 30, 0).timestamp())
        rollover = handler.computeRollover(now)
        local = datetime.fromtimestamp(rollover)
        assert (local.hour, local.minute, local.second) == (0, 0, 0), (
            f"hour {hour}: rollover at {local}, expected midnight"
        )
        assert local.date() >= datetime(2026, 9, 2).date()


def test_reload_does_not_duplicate_handlers(reload_logger):
    reload_logger()
    reload_logger()
    assert len(logging.getLogger("ink").handlers) == 1
