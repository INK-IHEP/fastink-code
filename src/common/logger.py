import logging
from logging.config import dictConfig
from concurrent_log_handler import ConcurrentRotatingFileHandler

from src.common.config import get_config


def _setup_logger() -> logging.Logger:
    log_path = get_config("common", "log_path", fallback="/ink/ink.log")
    log_level = get_config("common", "log_level", fallback="INFO").upper()
    valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    if log_level not in valid_levels:
        log_level = "INFO"

    LOGGING_CONFIG = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "detailed": {
                "format": (
                    "%(asctime)s - %(name)s - %(levelname)s - "
                    "%(module)s.%(funcName)s (line %(lineno)d): %(message)s"
                ),
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "detailed",
                "stream": "ext://sys.stdout",
            },
            "file": {
                "class": "concurrent_log_handler.ConcurrentRotatingFileHandler",
                "formatter": "detailed",
                "filename": log_path,
                "maxBytes": 100_000_000,  # 100MB
                "backupCount": 10,
                "encoding": "utf-8",
            },
        },
        "loggers": {
            "ink": {
                "handlers": ["console", "file"],
                "level": log_level,
                "propagate": False,
            },
        },
    }

    dictConfig(LOGGING_CONFIG)
    return logging.getLogger("ink")


logger = _setup_logger()
