"""Tests for sidereal filtering."""

import numpy as np

from canvod.streamstats.spectral.sidereal import (
    SiderealFilterResult,
    sidereal_filter,
)


class TestSiderealFilter:
    """Sidereal filter tests."""

    def test_identical_repeated_signal(self):
        """Identical signal repeated after sidereal offset → residual ≈ 0."""
        offset = 100.0  # use a small offset for testing
        t1 = np.arange(0, 50, 1.0)
        t2 = t1 + offset
        t = np.concatenate([t1, t2])
        signal = np.sin(2 * np.pi * 0.1 * t1)
        values = np.concatenate([signal, signal])

        result = sidereal_filter(t, values, sidereal_offset=offset)
        assert isinstance(result, SiderealFilterResult)
        # Residual should be near zero where interpolation is valid
        if len(result.residual) > 0:
            np.testing.assert_allclose(result.residual, 0.0, atol=0.05)

    def test_signal_with_known_change(self):
        """Signal changes between days → residual captures the change."""
        offset = 100.0
        t1 = np.arange(0, 50, 1.0)
        t2 = t1 + offset
        t = np.concatenate([t1, t2])

        signal_day1 = np.sin(2 * np.pi * 0.1 * t1)
        signal_day2 = signal_day1 + 1.0  # shifted up by 1
        values = np.concatenate([signal_day1, signal_day2])

        result = sidereal_filter(t, values, sidereal_offset=offset)
        if len(result.residual) > 0:
            np.testing.assert_allclose(result.residual, 1.0, atol=0.05)

    def test_high_correlation_for_repeating(self):
        """Correlation should be ≈ 1 for a perfectly repeating signal."""
        offset = 100.0
        t = np.arange(0, 250, 1.0)
        # Create a periodic signal with period = offset
        values = np.sin(2 * np.pi * t / offset)

        result = sidereal_filter(t, values, sidereal_offset=offset)
        assert result.correlation > 0.95

    def test_empty_input(self):
        """Empty input returns NaN correlation."""
        result = sidereal_filter(np.array([]), np.array([]))
        assert np.isnan(result.correlation)
        assert len(result.times) == 0

    def test_nan_handling(self):
        """NaN values filtered out."""
        t = np.arange(0, 200, 1.0)
        values = np.sin(2 * np.pi * 0.01 * t)
        values[50:55] = np.nan

        result = sidereal_filter(t, values, sidereal_offset=100.0)
        assert not np.any(np.isnan(result.residual))
