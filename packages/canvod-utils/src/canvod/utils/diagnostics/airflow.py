"""Airflow-compatible task metrics.

Collects duration + peak memory, optionally pushes to XCom and StatsD.
Works standalone outside Airflow — just collects metrics locally.
"""

from __future__ import annotations

import os
import time
import tracemalloc
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from canvod.utils.diagnostics._store import log_timing, record

if TYPE_CHECKING:
    from collections.abc import Generator


@dataclass
class TaskMetrics:
    """Collected metrics from a ``task_metrics`` context block."""

    operation: str
    duration_s: float = 0.0
    peak_memory_mb: float = 0.0
    status: str = "running"
    extras: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        """Return metrics as a flat dict (suitable for Airflow XCom)."""
        return {
            "operation": self.operation,
            "duration_s": round(self.duration_s, 3),
            "peak_memory_mb": round(self.peak_memory_mb, 2),
            "status": self.status,
            **self.extras,
        }


@contextmanager
def task_metrics(
    operation: str,
    *,
    push: bool = False,
    log: bool = True,
    **extras: Any,
) -> Generator[TaskMetrics]:
    """Context manager that collects duration + peak memory for a task.

    Designed for Airflow DAGs: collects timing and memory, optionally pushes
    to Airflow XCom and emits StatsD metrics.

    Parameters
    ----------
    operation : str
        Task/operation name.
    push : bool
        If True and running inside Airflow, push metrics to XCom and StatsD.
    log : bool
        If True, emit a structlog message on completion.
    **extras
        Additional key-value pairs included in metrics.

    Examples
    --------
    In an Airflow task::

        @task
        def ingest_rinex(**context):
            with task_metrics("ingest_rinex", push=True) as m:
                process_all_files()
            return m.as_dict()  # also available as XCom

    Standalone::

        with task_metrics("batch_process") as m:
            run_pipeline()
        print(f"Took {m.duration_s:.1f}s, peak {m.peak_memory_mb:.0f} MB")
    """
    metrics = TaskMetrics(operation=operation, extras=extras)

    was_tracing = tracemalloc.is_tracing()
    if not was_tracing:
        tracemalloc.start()
    tracemalloc.reset_peak()
    t0 = time.perf_counter()

    try:
        yield metrics
        metrics.status = "success"
    except BaseException:
        metrics.status = "failed"
        raise
    finally:
        metrics.duration_s = time.perf_counter() - t0
        _, peak = tracemalloc.get_traced_memory()
        if not was_tracing:
            tracemalloc.stop()
        metrics.peak_memory_mb = peak / (1024 * 1024)

        record(
            operation,
            metrics.duration_s,
            peak_memory_mb=round(metrics.peak_memory_mb, 2),
            status=metrics.status,
            metric_type="task",
            **extras,
        )

        if log:
            log_timing(
                operation,
                metrics.duration_s,
                {
                    "peak_memory_mb": round(metrics.peak_memory_mb, 2),
                    "status": metrics.status,
                    **extras,
                },
            )

        if push:
            _push_airflow_metrics(metrics)


def _push_airflow_metrics(metrics: TaskMetrics) -> None:
    """Push metrics to Airflow XCom and StatsD if available."""
    if os.environ.get("AIRFLOW_CTX_DAG_ID"):
        try:
            from airflow.operators.python import (
                get_current_context,  # type: ignore[unresolved-import]
            )

            context = get_current_context()
            ti = context["ti"]
            ti.xcom_push(key=f"metrics_{metrics.operation}", value=metrics.as_dict())
        except Exception:
            pass

    try:
        from airflow.stats import Stats  # type: ignore[unresolved-import]

        Stats.timing(f"canvod.{metrics.operation}.duration_s", metrics.duration_s)
        Stats.gauge(
            f"canvod.{metrics.operation}.peak_memory_mb", metrics.peak_memory_mb
        )
        Stats.incr(f"canvod.{metrics.operation}.{metrics.status}")
    except Exception:
        pass
