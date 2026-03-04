"""Tests for SpectralSlopeAccumulator."""

import math

import numpy as np
import pytest

from canvod.streamstats.accumulators.spectral_slope import SpectralSlopeAccumulator


class TestSpectralSlopeAccumulator:
    def test_empty(self):
        s = SpectralSlopeAccumulator()
        assert s.count == 0
        assert math.isnan(s.slope)
        assert math.isnan(s.intercept)
        assert math.isnan(s.spectral_strength)

    def test_known_power_law(self):
        """PSD = T * f^p → log(PSD) = p*log(f) + log(T). Recover slope and T."""
        true_slope = -2.5
        true_T = 1e-3
        freqs = np.logspace(-2, 0, 500)  # 0.01 to 1 Hz
        psd = true_T * freqs**true_slope

        s = SpectralSlopeAccumulator()
        s.update_batch(freqs, psd)

        assert s.count == 500
        assert s.slope == pytest.approx(true_slope, abs=0.05)
        assert s.spectral_strength == pytest.approx(true_T, rel=0.1)

    def test_batch_update_incremental(self):
        """Two batch updates should accumulate."""
        true_slope = -2.0
        freqs1 = np.logspace(-2, -1, 100)
        psd1 = 0.01 * freqs1**true_slope
        freqs2 = np.logspace(-1, 0, 100)
        psd2 = 0.01 * freqs2**true_slope

        s = SpectralSlopeAccumulator()
        s.update_batch(freqs1, psd1)
        s.update_batch(freqs2, psd2)

        assert s.count == 200
        assert s.slope == pytest.approx(true_slope, abs=0.1)

    def test_nan_and_negative_handling(self):
        """NaN and non-positive values are filtered."""
        s = SpectralSlopeAccumulator()
        freqs = np.array([0.1, -0.5, np.nan, 0.5, 0.0, 1.0])
        psd = np.array([1.0, 2.0, 3.0, np.nan, 5.0, 0.5])
        s.update_batch(freqs, psd)
        # Only (0.1, 1.0) and (1.0, 0.5) are valid
        assert s.count == 2

    def test_roundtrip_serialization(self):
        freqs = np.logspace(-2, 0, 200)
        psd = 0.005 * freqs ** (-1.8)

        s = SpectralSlopeAccumulator()
        s.update_batch(freqs, psd)

        arr = s.to_array()
        restored = SpectralSlopeAccumulator.from_array(arr)

        assert restored.count == s.count
        assert restored.slope == pytest.approx(s.slope)
        assert restored.intercept == pytest.approx(s.intercept)

    def test_merge(self):
        """Merge takes the one with more observations (right-biased)."""
        freqs = np.logspace(-2, 0, 300)
        psd = 0.01 * freqs ** (-2.0)

        a = SpectralSlopeAccumulator()
        a.update_batch(freqs[:100], psd[:100])

        b = SpectralSlopeAccumulator()
        b.update_batch(freqs, psd)  # more observations

        a.merge(b)
        assert a.count == 300
        assert a.slope == pytest.approx(b.slope)

    def test_all_invalid_input(self):
        """All-NaN or all-negative input → no updates."""
        s = SpectralSlopeAccumulator()
        s.update_batch(np.array([-1.0, -2.0]), np.array([1.0, 2.0]))
        assert s.count == 0
        s.update_batch(np.array([1.0, 2.0]), np.array([np.nan, np.nan]))
        assert s.count == 0
