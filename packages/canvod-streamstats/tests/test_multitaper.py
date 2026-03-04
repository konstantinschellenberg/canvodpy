"""Tests for multitaper PSD estimation."""

import numpy as np

from canvod.streamstats.spectral.multitaper import MultitaperResult, multitaper_psd


class TestMultitaperPSD:
    """Multitaper PSD tests."""

    def test_white_noise_flat_psd(self):
        """White noise should yield approximately flat PSD (spectral_index ≈ 0)."""
        rng = np.random.default_rng(42)
        x = rng.standard_normal(2048)

        result = multitaper_psd(x, sample_rate=1.0)
        assert isinstance(result, MultitaperResult)
        assert result.noise_type == "white"
        assert abs(result.spectral_index) < 1.0  # approximately 0

    def test_random_walk_noise(self):
        """Integrated white noise (random walk) should have negative spectral index."""
        rng = np.random.default_rng(42)
        x = np.cumsum(rng.standard_normal(2048))

        result = multitaper_psd(x, sample_rate=1.0)
        assert result.spectral_index < -1.0
        assert result.noise_type in ("random_walk", "flicker")

    def test_pure_sine_harmonic_detection(self):
        """Pure sine should be detected as a harmonic line by the F-test."""
        t = np.arange(1024)
        x = np.sin(2 * np.pi * 0.1 * t)

        result = multitaper_psd(x, sample_rate=1.0)
        # Should detect at least one harmonic line
        assert len(result.f_test_lines) >= 1
        # One of the detected lines should be near 0.1 Hz
        assert np.any(np.abs(result.f_test_lines - 0.1) < 0.01)

    def test_frequency_axis_correct(self):
        """Frequency axis should range from 0 to Nyquist."""
        N = 256
        fs = 10.0
        x = np.random.default_rng(42).standard_normal(N)

        result = multitaper_psd(x, sample_rate=fs)
        assert result.frequencies[0] == 0.0
        np.testing.assert_allclose(result.frequencies[-1], fs / 2, rtol=0.01)

    def test_nan_handling(self):
        """NaN values replaced with zeros, result still computed."""
        x = np.random.default_rng(42).standard_normal(256)
        x[10:20] = np.nan

        result = multitaper_psd(x, sample_rate=1.0)
        assert len(result.psd) > 0
        assert not np.any(np.isnan(result.psd))

    def test_short_input(self):
        """Very short input returns empty result."""
        result = multitaper_psd(np.array([1.0, 2.0]))
        assert len(result.psd) == 0
        assert result.noise_type == "unknown"

    def test_psd_positive(self):
        """PSD values should be non-negative."""
        x = np.random.default_rng(42).standard_normal(512)
        result = multitaper_psd(x, sample_rate=1.0)
        assert np.all(result.psd >= 0)
