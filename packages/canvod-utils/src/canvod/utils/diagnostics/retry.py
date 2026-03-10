"""Retry decorator wrapping tenacity with canvodpy conventions."""

from __future__ import annotations

from typing import TYPE_CHECKING

from tenacity import (
    retry as _tenacity_retry,
)
from tenacity import (
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

if TYPE_CHECKING:
    from collections.abc import Callable


def retry(
    *,
    attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
) -> Callable:
    """Retry a function on failure with exponential backoff.

    Wraps ``tenacity`` with a simple interface.

    Parameters
    ----------
    attempts : int
        Maximum number of attempts.
    delay : float
        Initial delay between retries in seconds.
    backoff : float
        Multiplier applied to delay after each retry.
    exceptions : tuple
        Exception types that trigger a retry.

    ::

        @retry(attempts=3, delay=0.5, exceptions=(ConnectionError, TimeoutError))
        def download_sp3(url):
            ...
    """
    return _tenacity_retry(
        stop=stop_after_attempt(attempts),
        wait=wait_exponential(multiplier=delay, exp_base=backoff, max=300),
        retry=retry_if_exception_type(exceptions),
        reraise=True,
    )
