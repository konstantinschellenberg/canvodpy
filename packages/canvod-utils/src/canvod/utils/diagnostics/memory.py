"""Memory tracking decorator and context manager.

Uses stdlib ``tracemalloc`` — no external dependencies.
"""

from __future__ import annotations

import functools
import tracemalloc
from typing import TYPE_CHECKING, Any

from canvod.utils.diagnostics._store import log_timing, record

if TYPE_CHECKING:
    from collections.abc import Callable


class track_memory:
    """Decorator and context manager for tracking peak memory usage.

    Uses ``tracemalloc`` to measure peak memory allocated during the
    decorated function or context block. Records to the global store.

    Examples
    --------
    As decorator::

        @track_memory("vod.compute")
        def compute_vod(ds): ...

    As context manager::

        with track_memory("store.write") as m:
            ds.to_zarr(store)
        print(f"Peak: {m.peak_mb:.1f} MB")
    """

    def __init__(self, operation: str, *, log: bool = False, **extras: Any):
        self.operation = operation
        self.log = log
        self.extras = extras
        self.peak_mb: float = 0.0
        self.current_mb: float = 0.0

    def __call__(self, func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            was_tracing = tracemalloc.is_tracing()
            if not was_tracing:
                tracemalloc.start()
            tracemalloc.reset_peak()
            try:
                return func(*args, **kwargs)
            finally:
                current, peak = tracemalloc.get_traced_memory()
                if not was_tracing:
                    tracemalloc.stop()
                peak_mb = peak / (1024 * 1024)
                record(
                    self.operation,
                    0.0,
                    peak_memory_mb=round(peak_mb, 2),
                    current_memory_mb=round(current / (1024 * 1024), 2),
                    metric_type="memory",
                    **self.extras,
                )
                if self.log:
                    log_timing(
                        self.operation,
                        0.0,
                        {
                            "peak_memory_mb": round(peak_mb, 2),
                            **self.extras,
                        },
                    )

        return wrapper

    def __enter__(self) -> track_memory:
        self._was_tracing = tracemalloc.is_tracing()
        if not self._was_tracing:
            tracemalloc.start()
        tracemalloc.reset_peak()
        return self

    def __exit__(self, *exc: Any) -> None:
        current, peak = tracemalloc.get_traced_memory()
        if not self._was_tracing:
            tracemalloc.stop()
        self.peak_mb = peak / (1024 * 1024)
        self.current_mb = current / (1024 * 1024)
        record(
            self.operation,
            0.0,
            peak_memory_mb=round(self.peak_mb, 2),
            current_memory_mb=round(self.current_mb, 2),
            metric_type="memory",
            **self.extras,
        )
        if self.log:
            log_timing(
                self.operation,
                0.0,
                {
                    "peak_memory_mb": round(self.peak_mb, 2),
                    **self.extras,
                },
            )
