"""Diagnostics, monitoring, and performance tracking for canvod.

Submodules
----------
timing : track_time, BatchTracker, timer, rate_limit
memory : track_memory
dataset : track_dataset, DatasetReport
airflow : task_metrics, TaskMetrics
retry : retry (tenacity wrapper)
_store : get_timings, get_timings_raw, reset_timings (global metrics store)

Usage
-----
::

    from canvod.utils.diagnostics import track_time, track_memory, task_metrics

    @track_time("rinex.read")
    def read_rinex(path): ...

    with track_memory("vod.compute") as m:
        compute(ds)
    print(f"Peak: {m.peak_mb:.1f} MB")
"""

from canvod.utils.diagnostics._store import (
    configure_db,
    db_path,
    get_timings,
    get_timings_raw,
    query_db,
    reset_timings,
)
from canvod.utils.diagnostics.airflow import TaskMetrics, task_metrics
from canvod.utils.diagnostics.dataset import DatasetReport, track_dataset
from canvod.utils.diagnostics.memory import track_memory
from canvod.utils.diagnostics.retry import retry
from canvod.utils.diagnostics.timing import (
    BatchTracker,
    bottlenecks,
    plot_bottlenecks,
    rate_limit,
    timer,
    track_time,
)

__all__ = [
    "BatchTracker",
    "DatasetReport",
    "TaskMetrics",
    "bottlenecks",
    "configure_db",
    "db_path",
    "get_timings",
    "get_timings_raw",
    "plot_bottlenecks",
    "query_db",
    "rate_limit",
    "reset_timings",
    "retry",
    "task_metrics",
    "timer",
    "track_dataset",
    "track_memory",
    "track_time",
]
