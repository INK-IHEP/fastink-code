import logging
import os

from concurrent_log_handler import ConcurrentTimedRotatingFileHandler

from fastink.common.config import get_config


def _setup_logger() -> logging.Logger:
    log_path = get_config("common", "log_path", fallback="/ink/log/ink.log")
    log_level = get_config("common", "log_level", fallback="INFO").upper()
    if log_level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        log_level = "INFO"

    log_dir = os.path.dirname(log_path)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    handler = ConcurrentTimedRotatingFileHandler(
        log_path,
        when="MIDNIGHT",
        interval=1,
        backupCount=0,  # ponytail: keep every day's gz; ops prune manually
        use_gzip=True,
        encoding="utf-8",
    )
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - "
            "%(module)s.%(funcName)s (line %(lineno)d): %(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
    )

    logger = logging.getLogger("ink")
    logger.setLevel(log_level)
    logger.handlers.clear()
    logger.addHandler(handler)
    return logger


logger = _setup_logger()
