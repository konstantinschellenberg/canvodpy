"""Tests for Detrended Fluctuation Analysis and Hurst exponent."""

import numpy as np
import pytest

from canvod.streamstats.information.dfa import dfa, hurst_exponent


class TestDFA:
    def test_white_noise(self):
        rng = np.random.default_rng(42)
        result = dfa(rng.standard_normal(5000))
        # White noise → alpha ≈ 0.5
        assert result.alpha == pytest.approx(0.5, abs=0.15)
        assert result.behavior == "uncorrelated"

    def test_random_walk(self):
        rng = np.random.default_rng(42)
        walk = np.cumsum(rng.standard_normal(5000))
        result = dfa(walk)
        # Random walk → alpha ≈ 1.5
        assert result.alpha == pytest.approx(1.5, abs=0.2)
        assert result.behavior == "non_stationary"

    def test_behavior_classification(self):
        assert dfa(np.random.default_rng(42).standard_normal(5000)).behavior in (
            "uncorrelated",
            "anti_persistent",
            "persistent",
        )

    def test_short_input(self):
        result = dfa(np.array([1.0, 2.0, 3.0]))
        assert np.isnan(result.alpha)
        assert result.behavior == "insufficient_data"

    def test_r_squared_reasonable(self):
        rng = np.random.default_rng(42)
        result = dfa(rng.standard_normal(5000))
        assert result.r_squared > 0.9

    def test_scales_and_fluctuations(self):
        rng = np.random.default_rng(42)
        result = dfa(rng.standard_normal(2000))
        assert len(result.scales) > 0
        assert len(result.fluctuations) == len(result.scales)


class TestHurstExponent:
    def test_from_alpha(self):
        h = hurst_exponent(alpha=0.5)
        assert h == pytest.approx(0.5)

    def test_from_spectral_slope(self):
        # beta = 0 → H = 0.5 (white noise)
        h = hurst_exponent(spectral_slope=0.0)
        assert h == pytest.approx(0.5)

    def test_both_raises(self):
        with pytest.raises(ValueError, match="exactly one"):
            hurst_exponent(alpha=0.5, spectral_slope=0.0)

    def test_neither_raises(self):
        with pytest.raises(ValueError, match="exactly one"):
            hurst_exponent()
