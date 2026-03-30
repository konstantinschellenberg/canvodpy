"""Unit tests for MemoryMonitor and ResourceInitPlugin (no Dask cluster needed)."""

from __future__ import annotations

import os
import unittest.mock

import pytest
from canvodpy.orchestrator.resources import MemoryMonitor, ResourceInitPlugin

# ---------------------------------------------------------------------------
# MemoryMonitor
# ---------------------------------------------------------------------------


def _mock_vmem(
    available: int = 4 * 1024**3, percent: float = 42.5, total: int = 8 * 1024**3
):
    m = unittest.mock.MagicMock()
    m.available = available
    m.percent = percent
    m.total = total
    return m


class TestMemoryMonitor:
    def test_init_stores_max_memory_gb(self):
        mm = MemoryMonitor(max_memory_gb=16.0)
        assert mm.max_memory_gb == 16.0

    def test_init_none(self):
        mm = MemoryMonitor()
        assert mm.max_memory_gb is None

    def test_available_gb(self):
        with unittest.mock.patch(
            "psutil.virtual_memory", return_value=_mock_vmem(available=4 * 1024**3)
        ):
            mm = MemoryMonitor()
            assert abs(mm.available_gb() - 4.0) < 0.01

    def test_used_percent(self):
        with unittest.mock.patch(
            "psutil.virtual_memory", return_value=_mock_vmem(percent=55.0)
        ):
            mm = MemoryMonitor()
            assert mm.used_percent() == pytest.approx(55.0)

    def test_log_memory_stats_does_not_raise(self):
        with unittest.mock.patch("psutil.virtual_memory", return_value=_mock_vmem()):
            mm = MemoryMonitor()
            mm.log_memory_stats(context="test_context")  # should not raise


# ---------------------------------------------------------------------------
# ResourceInitPlugin
# ---------------------------------------------------------------------------


class TestResourceInitPlugin:
    def test_defaults(self):
        plugin = ResourceInitPlugin()
        assert plugin.cpu_affinity is None
        assert plugin.nice_value == 0

    def test_custom_values(self):
        plugin = ResourceInitPlugin(cpu_affinity=[0, 1], nice_value=10)
        assert plugin.cpu_affinity == [0, 1]
        assert plugin.nice_value == 10

    def test_setup_no_affinity_no_nice_is_noop(self):
        """Default plugin.setup() touches nothing."""
        plugin = ResourceInitPlugin()
        worker = unittest.mock.MagicMock(name="worker-0")
        # Should not raise; no OS calls made
        plugin.setup(worker)

    def test_setup_nice_calls_setpriority(self):
        plugin = ResourceInitPlugin(nice_value=5)
        worker = unittest.mock.MagicMock()
        with unittest.mock.patch("os.setpriority") as mock_sp:
            plugin.setup(worker)
        mock_sp.assert_called_once_with(os.PRIO_PROCESS, 0, 5)

    def test_setup_nice_permission_error_does_not_raise(self):
        plugin = ResourceInitPlugin(nice_value=10)
        worker = unittest.mock.MagicMock()
        with unittest.mock.patch(
            "os.setpriority", side_effect=PermissionError("denied")
        ):
            plugin.setup(worker)  # must not propagate

    def test_setup_nice_os_error_does_not_raise(self):
        plugin = ResourceInitPlugin(nice_value=10)
        worker = unittest.mock.MagicMock()
        with unittest.mock.patch("os.setpriority", side_effect=OSError("nope")):
            plugin.setup(worker)  # must not propagate

    def test_setup_affinity_linux_calls_sched_setaffinity(self):
        plugin = ResourceInitPlugin(cpu_affinity=[0, 1])
        worker = unittest.mock.MagicMock()
        with (
            unittest.mock.patch("platform.system", return_value="Linux"),
            unittest.mock.patch("os.sched_setaffinity", create=True) as mock_aff,
        ):
            plugin.setup(worker)
        mock_aff.assert_called_once_with(0, [0, 1])

    def test_setup_affinity_non_linux_skips(self):
        plugin = ResourceInitPlugin(cpu_affinity=[0, 1])
        worker = unittest.mock.MagicMock()
        with (
            unittest.mock.patch("platform.system", return_value="Darwin"),
            unittest.mock.patch("os.sched_setaffinity", create=True) as mock_aff,
        ):
            plugin.setup(worker)
        mock_aff.assert_not_called()

    def test_setup_affinity_linux_no_sched_setaffinity_attr(self):
        """Graceful degradation when sched_setaffinity is absent from os module."""
        plugin = ResourceInitPlugin(cpu_affinity=[0])
        worker = unittest.mock.MagicMock()
        with (
            unittest.mock.patch("platform.system", return_value="Linux"),
            unittest.mock.patch("os.sched_setaffinity", None, create=True),
        ):
            plugin.setup(worker)  # must not raise
