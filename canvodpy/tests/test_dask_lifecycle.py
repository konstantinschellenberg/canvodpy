"""Lifecycle tests for DaskClusterManager.

Verifies that clusters start, shut down cleanly, and leave no orphaned
processes behind — on success, exception, and atexit paths.

Marked ``integration`` (requires dask.distributed).  Run with::

    uv run pytest canvodpy/tests/test_dask_lifecycle.py -v
"""

from __future__ import annotations

import os

import pytest

pytest.importorskip("dask.distributed", reason="dask.distributed not installed")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _child_pids() -> set[int]:
    """Return the set of PIDs of all recursive children of this process."""
    psutil = pytest.importorskip("psutil")
    try:
        return {c.pid for c in psutil.Process(os.getpid()).children(recursive=True)}
    except psutil.NoSuchProcess:
        return set()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestDaskClusterManagerLifecycle:
    """Verify that DaskClusterManager creates and cleans up processes."""

    def test_cluster_spawns_processes(self):
        """Creating a cluster should produce child processes."""
        from canvodpy.orchestrator.resources import DaskClusterManager

        before = _child_pids()
        with DaskClusterManager(n_workers=2, threads_per_worker=1) as mgr:
            during = _child_pids()
            assert len(during) > len(before), (
                "LocalCluster should spawn scheduler + worker processes"
            )
            # Sanity: client is connected
            info = mgr.client.scheduler_info()
            assert len(info.get("workers", {})) == 2

    def test_context_manager_cleans_up_on_success(self):
        """No child processes should remain after a clean __exit__."""
        from canvodpy.orchestrator.resources import DaskClusterManager

        before = _child_pids()
        with DaskClusterManager(n_workers=2, threads_per_worker=1):
            pass  # normal exit
        after = _child_pids()

        leaked = after - before
        assert not leaked, (
            f"Leaked {len(leaked)} process(es) after clean exit: {leaked}"
        )

    def test_context_manager_cleans_up_on_exception(self):
        """No child processes should remain after an exception inside the block."""
        from canvodpy.orchestrator.resources import DaskClusterManager

        before = _child_pids()
        with pytest.raises(RuntimeError, match="simulated"):
            with DaskClusterManager(n_workers=2, threads_per_worker=1):
                raise RuntimeError("simulated crash")
        after = _child_pids()

        leaked = after - before
        assert not leaked, f"Leaked {len(leaked)} process(es) after exception: {leaked}"

    def test_explicit_close_cleans_up(self):
        """Calling .close() directly (no context manager) should terminate workers."""
        from canvodpy.orchestrator.resources import DaskClusterManager

        before = _child_pids()
        mgr = DaskClusterManager(n_workers=2, threads_per_worker=1)
        assert len(_child_pids()) > len(before), "cluster should have spawned processes"

        mgr.close()
        after = _child_pids()
        leaked = after - before
        assert not leaked, (
            f"Leaked {len(leaked)} process(es) after explicit close: {leaked}"
        )

    def test_double_close_is_safe(self):
        """Calling .close() twice must not raise."""
        from canvodpy.orchestrator.resources import DaskClusterManager

        mgr = DaskClusterManager(n_workers=2, threads_per_worker=1)
        mgr.close()
        mgr.close()  # second call must be a no-op

    def test_log_cluster_info_fields(self, caplog):
        """log_cluster_info must emit the expected structured fields."""
        import logging

        from canvodpy.orchestrator.resources import DaskClusterManager

        with caplog.at_level(logging.INFO, logger="canvodpy.orchestrator.resources"):
            with DaskClusterManager(n_workers=2, threads_per_worker=1):
                pass

        # Find the dask_cluster_started record
        started = [
            r for r in caplog.records if "dask_cluster_started" in r.getMessage()
        ]
        assert started, "Expected a dask_cluster_started log record"

    def test_thread_env_vars_set(self):
        """DaskClusterManager must cap BLAS thread env vars."""
        from canvodpy.orchestrator.resources import DaskClusterManager

        # Unset any pre-existing values so we can test the default
        saved = {}
        for var in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS"):
            saved[var] = os.environ.pop(var, None)

        try:
            with DaskClusterManager(n_workers=2, threads_per_worker=3):
                assert os.environ.get("OMP_NUM_THREADS") == "3"
                assert os.environ.get("MKL_NUM_THREADS") == "3"
                assert os.environ.get("OPENBLAS_NUM_THREADS") == "3"
        finally:
            for var, val in saved.items():
                if val is not None:
                    os.environ[var] = val
                else:
                    os.environ.pop(var, None)

    def test_thread_env_vars_not_overridden(self):
        """Existing operator-level env vars must not be overwritten."""
        from canvodpy.orchestrator.resources import DaskClusterManager

        os.environ["OMP_NUM_THREADS"] = "99"
        try:
            with DaskClusterManager(n_workers=2, threads_per_worker=1):
                assert os.environ["OMP_NUM_THREADS"] == "99", (
                    "Operator-set OMP_NUM_THREADS must not be overwritten"
                )
        finally:
            del os.environ["OMP_NUM_THREADS"]
