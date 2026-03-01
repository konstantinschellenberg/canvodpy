"""Tests for canvodpy.utils.telemetry module."""

import pytest
from canvodpy.utils.telemetry import (
    OTEL_AVAILABLE,
    is_tracing_enabled,
    trace_aux_preprocessing,
    trace_icechunk_write,
    trace_operation,
    trace_rinex_processing,
    trace_vod_calculation,
)


class TestTraceOperation:
    """Tests for trace_operation context manager."""

    def test_noop_when_otel_unavailable(self):
        """Context manager should work as a no-op without OTel."""
        with trace_operation("test_op") as span:
            x = 1 + 1
        # Should not raise regardless of OTel availability
        assert x == 2

    def test_noop_with_attributes(self):
        with trace_operation("test_op", attributes={"key": "value"}) as span:
            pass

    def test_noop_no_record_duration(self):
        with trace_operation("test_op", record_duration=False) as span:
            pass

    def test_exception_propagates(self):
        """Exceptions inside the context manager should propagate."""
        with pytest.raises(ValueError, match="test error"):
            with trace_operation("failing_op"):
                raise ValueError("test error")


class TestTraceIcechunkWrite:
    """Tests for trace_icechunk_write."""

    def test_basic_usage(self):
        with trace_icechunk_write("2025213") as span:
            pass

    def test_with_size(self):
        with trace_icechunk_write("2025213", dataset_size_mb=45.2) as span:
            pass

    def test_with_num_variables(self):
        with trace_icechunk_write(
            "2025213", dataset_size_mb=10.0, num_variables=5
        ) as span:
            pass

    def test_exception_propagates(self):
        with pytest.raises(RuntimeError):
            with trace_icechunk_write("2025213"):
                raise RuntimeError("write failed")


class TestTraceRinexProcessing:
    """Tests for trace_rinex_processing."""

    def test_basic_usage(self):
        with trace_rinex_processing("test.25o") as span:
            pass

    def test_with_site_and_date(self):
        with trace_rinex_processing(
            "ract213a00.25o", site="Rosalia", date="2025213"
        ) as span:
            pass


class TestTraceAuxPreprocessing:
    """Tests for trace_aux_preprocessing."""

    def test_basic_usage(self):
        with trace_aux_preprocessing("2025213") as span:
            pass

    def test_with_operation(self):
        with trace_aux_preprocessing("2025213", operation="hermite") as span:
            pass


class TestTraceVodCalculation:
    """Tests for trace_vod_calculation."""

    def test_basic_usage(self):
        with trace_vod_calculation("tau_omega") as span:
            pass

    def test_with_site_and_date(self):
        with trace_vod_calculation("tau_omega", site="Rosalia", date="2025213") as span:
            pass


class TestIsTracingEnabled:
    """Tests for is_tracing_enabled."""

    def test_returns_bool(self):
        result = is_tracing_enabled()
        assert isinstance(result, bool)

    def test_consistent_with_otel_available(self):
        result = is_tracing_enabled()
        if not OTEL_AVAILABLE:
            assert result is False
