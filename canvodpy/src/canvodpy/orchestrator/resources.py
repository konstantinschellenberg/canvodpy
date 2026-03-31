"""Resource management for pipeline processing.

Provides Dask LocalCluster lifecycle management, per-worker resource
initialization via a WorkerPlugin, and advisory memory monitoring.
"""

from __future__ import annotations

import logging
import os
import platform
from typing import TYPE_CHECKING, Any, cast

import psutil

from canvodpy.logging import get_logger

if TYPE_CHECKING:
    from dask.distributed import Client

logger = get_logger(__name__)


class MemoryMonitor:
    """Monitor system memory and log advisory snapshots.

    Used by ``PipelineOrchestrator`` for batch-level memory logging.
    Actual memory enforcement is handled by Dask's nanny process.

    Parameters
    ----------
    max_memory_gb : float | None
        Soft RAM limit in GB (informational only). None means no limit.

    """

    def __init__(self, max_memory_gb: float | None = None) -> None:
        self.max_memory_gb = max_memory_gb

    def available_gb(self) -> float:
        """Current available system memory in GB."""
        return psutil.virtual_memory().available / (1024**3)

    def used_percent(self) -> float:
        """Current system memory usage percentage."""
        return psutil.virtual_memory().percent

    def log_memory_stats(self, context: str = "") -> None:
        """Log current memory statistics.

        Parameters
        ----------
        context : str
            Description of when this snapshot was taken.

        """
        mem = psutil.virtual_memory()
        logger.info(
            "memory_stats",
            context=context,
            available_gb=round(mem.available / (1024**3), 2),
            used_percent=round(mem.percent, 1),
            total_gb=round(mem.total / (1024**3), 2),
        )


try:
    from dask.distributed import Client, LocalCluster
    from distributed.diagnostics.plugin import WorkerPlugin

    _HAS_DISTRIBUTED = True
except ImportError:  # pragma: no cover
    _HAS_DISTRIBUTED = False


class ResourceInitPlugin:
    """Dask WorkerPlugin that applies CPU affinity and nice priority.

    Automatically re-applied when a worker is restarted by the nanny
    (e.g. after an OOM kill).

    Parameters
    ----------
    cpu_affinity : list[int] | None
        CPU core IDs to pin workers to. None means no restriction.
    nice_value : int
        Process nice value (0=normal, 19=lowest priority).

    """

    name = "resource-init"

    def __init__(
        self,
        cpu_affinity: list[int] | None = None,
        nice_value: int = 0,
    ) -> None:
        self.cpu_affinity = cpu_affinity
        self.nice_value = nice_value

    def setup(self, worker: object) -> None:
        """Called when a worker starts (or restarts after nanny kill)."""
        log = logging.getLogger(__name__)

        if self.cpu_affinity is not None:
            if platform.system() == "Linux":
                sched_setaffinity = getattr(os, "sched_setaffinity", None)
                if callable(sched_setaffinity):
                    sched_setaffinity(0, self.cpu_affinity)
                    log.info(
                        "CPU affinity set to %s on worker %s",
                        self.cpu_affinity,
                        getattr(worker, "name", "unknown"),
                    )
                else:
                    log.warning("sched_setaffinity unavailable on this platform build")
            else:
                log.warning(
                    "cpu_affinity is only supported on Linux, skipping on %s",
                    platform.system(),
                )

        if self.nice_value > 0:
            try:
                os.setpriority(os.PRIO_PROCESS, 0, self.nice_value)
                log.info(
                    "Nice priority set to %d on worker %s",
                    self.nice_value,
                    getattr(worker, "name", "unknown"),
                )
            except (OSError, PermissionError) as e:
                log.warning(
                    "Failed to set nice priority to %d: %s",
                    self.nice_value,
                    e,
                )


if _HAS_DISTRIBUTED:
    # Register as a proper WorkerPlugin subclass at runtime so that the
    # class still loads when distributed is not installed.
    _ResourceInitPluginClass = type(
        "ResourceInitPlugin",
        (WorkerPlugin, ResourceInitPlugin),
        dict(ResourceInitPlugin.__dict__),
    )
else:
    _ResourceInitPluginClass = ResourceInitPlugin


class DaskClusterManager:
    """Manages a Dask ``LocalCluster`` and ``Client`` lifecycle.

    Parameters
    ----------
    n_workers : int | None
        Number of worker processes. ``None`` lets Dask auto-detect
        (defaults to ``os.cpu_count()``).
    memory_limit_per_worker : str | float
        Per-worker memory limit. ``"auto"`` lets Dask choose
        (system RAM / n_workers). A float is interpreted as bytes.
    cpu_affinity : list[int] | None
        CPU core IDs to pin workers to.
    nice_priority : int
        Process nice value for workers.
    threads_per_worker : int | None
        Threads per Dask worker process. ``None`` defaults to 1.

    """

    def __init__(
        self,
        n_workers: int | None = None,
        memory_limit_per_worker: str | float = "auto",
        cpu_affinity: list[int] | None = None,
        nice_priority: int = 0,
        threads_per_worker: int | None = None,
        scheduler_address: str | None = None,
    ) -> None:
        if not _HAS_DISTRIBUTED:
            msg = (
                "dask.distributed is required for DaskClusterManager. "
                "Install with: pip install 'dask[distributed]'"
            )
            raise ImportError(msg)

        self._scheduler_address = scheduler_address

        # Cap BLAS/OpenMP thread counts before starting workers so that
        # worker subprocesses inherit the env vars.  Without this, numpy/
        # scipy can spawn os.cpu_count() threads per worker, multiplying
        # actual CPU use by (n_workers × blas_threads).
        _effective_threads = threads_per_worker if threads_per_worker is not None else 1
        _thread_str = str(_effective_threads)
        for _var in (
            "OMP_NUM_THREADS",
            "MKL_NUM_THREADS",
            "OPENBLAS_NUM_THREADS",
            "NUMEXPR_NUM_THREADS",
            "VECLIB_MAXIMUM_THREADS",
        ):
            if _var not in os.environ:
                os.environ[_var] = _thread_str

        if scheduler_address is not None:
            # Connect to an existing scheduler — do NOT create a LocalCluster.
            # Resource caps (n_workers, memory, affinity) are properties of the
            # remote cluster, not configurable from here.
            self._cluster = None
            self._client = Client(address=scheduler_address)
            logger.info(
                "dask_cluster_remote",
                scheduler_address=scheduler_address,
            )
        else:
            cluster_kwargs: dict = {
                "threads_per_worker": threads_per_worker
                if threads_per_worker is not None
                else 1,
                "memory_limit": memory_limit_per_worker,
            }
            if n_workers is not None:
                cluster_kwargs["n_workers"] = n_workers

            self._cluster = LocalCluster(**cluster_kwargs)
            self._client = Client(self._cluster)

            # Ensure the local cluster is shut down when the process exits
            # (covers SIGTERM, uncaught exceptions, and Airflow task timeouts
            # that bypass __exit__).  atexit handlers run before interpreter
            # shutdown — workers are terminated cleanly instead of becoming
            # orphans that linger until Dask's idle timeout (~30 min).
            import atexit

            atexit.register(self.close)

            # Only register resource init plugin if affinity or nice is set
            if cpu_affinity is not None or nice_priority > 0:
                plugin_cls = cast(Any, _ResourceInitPluginClass)
                plugin = plugin_cls(
                    cpu_affinity=cpu_affinity,
                    nice_value=nice_priority,
                )
                self._client.register_plugin(plugin)

        self._closed = False
        self.log_cluster_info(threads_per_worker=threads_per_worker)

    @property
    def client(self) -> Client:
        """The Dask distributed client."""
        return self._client

    def log_cluster_info(self, threads_per_worker: int | None = None) -> None:
        """Log cluster configuration details."""
        info = self._client.scheduler_info()
        workers = info.get("workers", {})
        n_workers = len(workers)

        # Get memory limit from first worker (all workers have same limit)
        mem_limit = "unknown"
        if workers:
            first_worker = next(iter(workers.values()))
            mem_bytes = first_worker.get("memory_limit", 0)
            if mem_bytes:
                mem_limit = f"{mem_bytes / (1024**3):.1f} GB"

        dashboard = (
            self._cluster.dashboard_link
            if self._cluster is not None
            else "n/a (remote)"
        )
        logger.info(
            "dask_cluster_started",
            cluster_type="remote" if self._scheduler_address else "local",
            scheduler_address=self._scheduler_address or "auto",
            n_workers=n_workers,
            threads_per_worker=threads_per_worker
            if threads_per_worker is not None
            else 1,
            memory_limit_per_worker=mem_limit,
            dashboard_url=dashboard,
        )

    def close(self) -> None:
        """Shut down client and cluster."""
        if self._closed:
            return
        self._closed = True
        logger.info("dask_cluster_shutting_down")
        self._client.close()
        if self._cluster is not None:
            self._cluster.close()
        logger.info("dask_cluster_stopped")

    def __enter__(self) -> DaskClusterManager:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
