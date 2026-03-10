"""Timing decorators and context managers.

Provides ``track_time``, ``BatchTracker``, ``timer``, and ``rate_limit``.
"""

from __future__ import annotations

import functools
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from canvod.utils.diagnostics._store import log_timing, record

if TYPE_CHECKING:
    from collections.abc import Callable, Generator


class track_time:
    """Decorator and context manager for timing operations.

    Parameters
    ----------
    operation : str
        Dot-separated operation name (e.g. "rinex.read", "store.write").
    log : bool
        If True, emit a structlog message on completion.
    **extras
        Additional key-value pairs stored with the timing record.

    Examples
    --------
    As decorator::

        @track_time("rinex.read")
        def read_file(path): ...

    As context manager::

        with track_time("store.write", group="2025001"):
            ds.to_zarr(store)

    Access elapsed time from context::

        with track_time("step") as t:
            do_work()
        print(t.elapsed)  # seconds
    """

    def __init__(self, operation: str, *, log: bool = False, **extras: Any):
        self.operation = operation
        self.log = log
        self.extras = extras
        self.elapsed: float = 0.0

    def __call__(self, func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            t0 = time.perf_counter()
            try:
                return func(*args, **kwargs)
            finally:
                elapsed = time.perf_counter() - t0
                extras = {**self.extras}
                record(self.operation, elapsed, **extras)
                if self.log:
                    log_timing(self.operation, elapsed, extras)

        return wrapper

    def __enter__(self) -> track_time:
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.elapsed = time.perf_counter() - self._t0
        record(self.operation, self.elapsed, **self.extras)
        if self.log:
            log_timing(self.operation, self.elapsed, self.extras)


@dataclass
class BatchTracker:
    """Track timing for a batch of sequential steps.

    Examples
    --------
    ::

        tracker = BatchTracker("process_date")
        for f in rinex_files:
            with tracker.step(f.name):
                process(f)
        print(tracker.summary())
        tracker.plot()  # quick bar chart
    """

    name: str
    log_each: bool = False
    _steps: list[dict[str, Any]] = field(default_factory=list, repr=False)

    @contextmanager
    def step(self, label: str, **extras: Any) -> Generator[None]:
        """Time a single step within the batch."""
        t0 = time.perf_counter()
        try:
            yield
        finally:
            elapsed = time.perf_counter() - t0
            step_record = {
                "step": label,
                "duration_s": round(elapsed, 6),
                "batch": self.name,
                **extras,
            }
            self._steps.append(step_record)
            record(f"{self.name}.{label}", elapsed, batch=self.name, **extras)
            if self.log_each:
                log_timing(f"{self.name}.{label}", elapsed, extras)

    def summary(self) -> Any:
        """Return per-step timing as a polars DataFrame."""
        import polars as pl

        if not self._steps:
            return pl.DataFrame(
                schema={"step": pl.Utf8, "duration_s": pl.Float64, "batch": pl.Utf8}
            )
        return pl.DataFrame(self._steps)

    @property
    def total(self) -> float:
        """Total elapsed time across all steps."""
        return sum(s["duration_s"] for s in self._steps)

    @property
    def mean(self) -> float:
        """Mean step duration."""
        return self.total / len(self._steps) if self._steps else 0.0

    def plot(self, *, title: str | None = None) -> Any:
        """Quick bar chart of step durations. Returns a matplotlib Figure."""
        import matplotlib.pyplot as plt

        df = self.summary()
        fig, ax = plt.subplots(figsize=(max(6, len(self._steps) * 0.4), 4))
        ax.barh(df["step"].to_list(), df["duration_s"].to_list(), color="#5D7D5B")
        ax.set_xlabel("Duration (s)")
        ax.set_title(title or f"{self.name} — step timing")
        ax.invert_yaxis()
        fig.tight_layout()
        return fig


@contextmanager
def timer(label: str = "elapsed") -> Generator[dict[str, float]]:
    """Minimal context manager that yields a dict with elapsed time.

    Does NOT record to the global store — use ``track_time`` for that.

    ::

        with timer("read") as t:
            ds = xr.open_dataset(path)
        print(f"Read took {t['elapsed']:.2f}s")
    """
    result: dict[str, float] = {"elapsed": 0.0}
    t0 = time.perf_counter()
    try:
        yield result
    finally:
        result["elapsed"] = time.perf_counter() - t0


def bottlenecks(*, top_n: int = 10, metric_type: str | None = None) -> Any:
    """Identify the slowest operations from the global metrics store.

    Groups by operation, aggregates total/mean/count, and ranks by total
    time. Use after a pipeline run to find what to optimize.

    Parameters
    ----------
    top_n : int
        Number of slowest operations to return.
    metric_type : str, optional
        Filter by metric_type (e.g. "task", "memory", "dataset").
        None returns all timing records.

    Returns
    -------
    polars.DataFrame
        Columns: operation, total_s, mean_s, count, pct (% of total time).

    Examples
    --------
    ::

        from canvod.utils.diagnostics import bottlenecks

        # After running a pipeline...
        df = bottlenecks(top_n=5)
        print(df)
        # ┌──────────────────┬─────────┬────────┬───────┬──────┐
        # │ operation        ┆ total_s ┆ mean_s ┆ count ┆ pct  │
        # ├──────────────────┼─────────┼────────┼───────┼──────┤
        # │ store.write      ┆ 45.2    ┆ 4.5    ┆ 10    ┆ 38.1 │
        # │ rinex.read       ┆ 32.1    ┆ 3.2    ┆ 10    ┆ 27.0 │
        # │ aux.interpolate  ┆ 18.7    ┆ 1.9    ┆ 10    ┆ 15.7 │
        # └──────────────────┴─────────┴────────┴───────┴──────┘
    """
    import polars as pl

    from canvod.utils.diagnostics._store import get_timings

    df = get_timings()
    if df.is_empty():
        return pl.DataFrame(
            schema={
                "operation": pl.Utf8,
                "total_s": pl.Float64,
                "mean_s": pl.Float64,
                "count": pl.UInt32,
                "pct": pl.Float64,
            }
        )

    if metric_type is not None and "metric_type" in df.columns:
        df = df.filter(pl.col("metric_type") == metric_type)
    elif metric_type is None and "metric_type" in df.columns:
        # Default: only timing records (exclude dataset/memory snapshots)
        df = df.filter(pl.col("metric_type").is_null())

    if df.is_empty():
        return pl.DataFrame(
            schema={
                "operation": pl.Utf8,
                "total_s": pl.Float64,
                "mean_s": pl.Float64,
                "count": pl.UInt32,
                "pct": pl.Float64,
            }
        )

    result = (
        df.group_by("operation")
        .agg(
            pl.col("duration_s").sum().alias("total_s"),
            pl.col("duration_s").mean().alias("mean_s"),
            pl.col("duration_s").count().alias("count"),
        )
        .sort("total_s", descending=True)
        .head(top_n)
    )

    grand_total = result["total_s"].sum()
    if grand_total > 0:
        result = result.with_columns(
            (pl.col("total_s") / grand_total * 100).round(1).alias("pct")
        )
    else:
        result = result.with_columns(pl.lit(0.0).alias("pct"))

    return result.with_columns(
        pl.col("total_s").round(3),
        pl.col("mean_s").round(3),
    )


def plot_bottlenecks(*, top_n: int = 10, title: str | None = None) -> Any:
    """Bar chart of the slowest operations. Returns a matplotlib Figure.

    ::

        from canvod.utils.diagnostics import plot_bottlenecks
        fig = plot_bottlenecks(top_n=8)
        fig.savefig("bottlenecks.png")
    """
    import matplotlib.pyplot as plt

    df = bottlenecks(top_n=top_n)
    if df.is_empty():
        fig, ax = plt.subplots()
        ax.text(0.5, 0.5, "No timing data", ha="center", va="center")
        return fig

    ops = df["operation"].to_list()
    totals = df["total_s"].to_list()
    pcts = df["pct"].to_list()

    fig, ax = plt.subplots(figsize=(max(6, len(ops) * 0.5), 4))
    bars = ax.barh(ops, totals, color="#5D7D5B")

    for bar, pct in zip(bars, pcts):
        ax.text(
            bar.get_width() + 0.1,
            bar.get_y() + bar.get_height() / 2,
            f"{pct:.0f}%",
            va="center",
            fontsize=9,
            color="#375D3B",
        )

    ax.set_xlabel("Total time (s)")
    ax.set_title(title or "Pipeline bottlenecks")
    ax.invert_yaxis()
    fig.tight_layout()
    return fig


def rate_limit(*, interval: float = 1.0) -> Callable:
    """Decorator that skips calls made more frequently than *interval* seconds.

    Useful for progress callbacks or logging inside tight loops.

    ::

        @rate_limit(interval=2.0)
        def log_progress(i, n):
            print(f"{i}/{n}")

        for i in range(10000):
            log_progress(i, 10000)  # prints at most every 2s
    """

    def decorator(func: Callable) -> Callable:
        last_call = [0.0]

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            now = time.perf_counter()
            if now - last_call[0] >= interval:
                last_call[0] = now
                return func(*args, **kwargs)
            return None

        return wrapper

    return decorator
