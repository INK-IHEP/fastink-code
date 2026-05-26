import logging
from logging.config import dictConfig
import os

from fastink.common.config import get_config

_LOGGER = None
_CONSOLE_ONLY = os.environ.get("INK_CONSOLE_ONLY", "0").lower() in ("1", "true", "yes")


def _setup_logger() -> logging.Logger:
    log_path = get_config("common", "log_path", fallback="/ink/ink.log")
    log_level = get_config("common", "log_level", fallback="INFO").upper()
    valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    if log_level not in valid_levels:
        log_level = "INFO"

    log_format = get_config(
        "common", "log_format",
        fallback="%(asctime)s - %(name)s - %(levelname)s - "
                 "%(module)s.%(funcName)s (line %(lineno)d): %(message)s",
    )
    date_format = get_config("common", "log_datefmt", fallback="%Y-%m-%d %H:%M:%S")

    handlers = {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "detailed",
            "stream": "ext://sys.stdout",
        },
    }

    if not _CONSOLE_ONLY:
        from concurrent_log_handler import ConcurrentRotatingFileHandler  # noqa: F401

        handlers["file"] = {
            "class": "concurrent_log_handler.ConcurrentRotatingFileHandler",
            "formatter": "detailed",
            "filename": log_path,
            "maxBytes": 100_000_000,
            "backupCount": 10,
            "encoding": "utf-8",
        }

    dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "detailed": {
                "format": log_format,
                "datefmt": date_format,
            },
        },
        "handlers": handlers,
        "loggers": {
            "ink": {
                "handlers": list(handlers.keys()),
                "level": log_level,
                "propagate": True,
            },
        },
    })
    return logging.getLogger("ink")


def __getattr__(name):
    if name == "logger":
        global _LOGGER
        if _LOGGER is None:
            _LOGGER = _setup_logger()
        return _LOGGER
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
