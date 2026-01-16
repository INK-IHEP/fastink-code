import time
import logging
from contextlib import contextmanager
from typing import Optional


@contextmanager
def log_step(
    step: str,
    *,
    logger: Optional[logging.Logger] = None,
    extra: str = ""
):
    """
    Log execution time of a code block.

    Usage:
        with log_step("sbatch_submit", logger):
            ...

    Args:
        step: step name, e.g. 'sbatch_submit'
        logger: logging.Logger, default getLogger("ink.hpc")
        extra: extra info appended to log
    """
    if logger is None:
        logger = logging.getLogger("ink.hpc")

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

