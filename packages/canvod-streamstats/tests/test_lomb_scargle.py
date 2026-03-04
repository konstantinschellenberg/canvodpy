"""Tests for Lomb-Scargle periodogram."""

import numpy as np

from canvod.streamstats.spectral.lomb_scargle import LombScargleResult, lomb_scargle


class TestLombScargle:
    """Lomb-Scargle tests."""

    def test_known_sine_uniform(self):
        """Pure sine in uniformly sampled data — peak at correct frequency."""
        f_true = 0.05  # Hz
        t = np.arange(0, 200, 1.0)
        signal = np.sin(2 * np.pi * f_true * t)

        result = lomb_scargle(t, signal, min_freq=0.01, max_freq=0.2, n_frequencies=500)
        assert isinstance(result, LombScargleResult)
        # Peak should be close to the true frequency
        np.testing.assert_allclose(result.peak_frequency, f_true, atol=0.005)
        assert result.peak_power > 0

    def test_known_sine_irregular(self):
        """Pure sine in irregularly sampled data — still recovers frequency."""
        f_true = 0.1
        rng = np.random.default_rng(42)
        t = np.sort(rng.uniform(0, 100, 200))
        signal = np.sin(2 * np.pi * f_true * t)

        result = lomb_scargle(
            t, signal, min_freq=0.01, max_freq=0.5, n_frequencies=1000
        )
        np.testing.assert_allclose(result.peak_frequency, f_true, atol=0.01)

    def test_nan_filtering(self):
        """NaN values are filtered before computation."""
        f_true = 0.05
        t = np.arange(0, 100, 1.0)
        signal = np.sin(2 * np.pi * f_true * t)
        signal[10:15] = np.nan
        t_nan = t.copy()
        t_nan[50] = np.nan

        result = lomb_scargle(t, signal, min_freq=0.01, max_freq=0.2)
        assert len(result.frequencies) > 0
        assert not np.isnan(result.peak_frequency)

    def test_strong_signal_low_fap(self):
        """Strong signal should have FAP below threshold."""
        t = np.arange(0, 500, 1.0)
        signal = 5.0 * np.sin(2 * np.pi * 0.05 * t)

        result = lomb_scargle(t, signal, min_freq=0.01, max_freq=0.2)
        assert result.false_alarm_probability < 0.05

    def test_empty_input(self):
        """Empty input returns NaN result."""
        result = lomb_scargle(np.array([]), np.array([]))
        assert np.isnan(result.peak_frequency)
        assert np.isnan(result.peak_power)
        assert len(result.frequencies) == 0

    def test_constant_input(self):
        """Constant signal should have low peak power."""
        t = np.arange(0, 100, 1.0)
        signal = np.ones_like(t) * 5.0

        result = lomb_scargle(t, signal, min_freq=0.01, max_freq=0.2)
        # Constant signal centred to zero → all power is essentially zero
        assert result.peak_power < 1e-10
