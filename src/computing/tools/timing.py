import time
import logging
from contextlib import contextmanager

@contextmanager
def log_step(logger: logging.Logger, step: str, extra: str = ""):
    start = time.monotonic()
    try:
        yield
    finally:
        cost = time.monotonic() - start
        logger.info(
            "step=%s cost=%.3fs %s",
            step,
            cost,
            extra
        )
