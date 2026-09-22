import logging
import time
from contextlib import contextmanager
from typing import Optional
from contextvars import ContextVar

from fastink.common.logger import logger as _app_logger

submit_id_var: ContextVar[str | None] = ContextVar(
    "submit_id",
    default=None,
)

@contextmanager
def log_step(
    step: str,
    *,
    logger: Optional[logging.Logger] = None,
    phase: str = "default",
    extra: str = ""
):
    """
    Log execution time of a code block.

    Usage:
        with log_step("execute_sbatch", logger=sbatch_logger, phase="sbatch_submit"):
            ...

    Args:
        step: step name, e.g. 'execute_sbatch'
        logger: logging.Logger, default "ink" app logger
        phase: logical phase, e.g. 'build_job_env', 'sbatch_submit'
        extra: extra info appended to log
    """
    if logger is None:
        logger = _app_logger

    submit_id = submit_id_var.get()
    start = time.monotonic()
    try:
        yield
    finally:
        cost = time.monotonic() - start
        logger.info(
            "submit_id=%s phase=%s step=%s cost=%.3fs %s",
            submit_id,
            phase,
            step,
            cost,
            extra
        )

