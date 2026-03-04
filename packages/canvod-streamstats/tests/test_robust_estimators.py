"""Tests for robust statistics estimators."""

import numpy as np
import pytest

from canvod.streamstats.robust.estimators import mad, robust_std, trimmed_mean


class TestMAD:
    def test_known_values(self):
        """MAD of [1,2,3,4,5]: median=3, |x-3|=[2,1,0,1,2], MAD=1.0."""
        assert mad(np.array([1, 2, 3, 4, 5])) == pytest.approx(1.0)

    def test_single_value(self):
        assert mad(np.array([42.0])) == pytest.approx(0.0)

    def test_nan_handling(self):
        result = mad(np.array([1.0, np.nan, 3.0, np.nan, 5.0]))
        assert not np.isnan(result)
        assert result == pytest.approx(mad(np.array([1.0, 3.0, 5.0])))

    def test_empty(self):
        assert np.isnan(mad(np.array([])))

    def test_all_nan(self):
        assert np.isnan(mad(np.array([np.nan, np.nan])))


class TestRobustStd:
    def test_gaussian_consistency(self):
        """For large Gaussian sample, robust_std ≈ true std."""
        rng = np.random.default_rng(42)
        data = rng.normal(loc=0, scale=2.0, size=10000)
        r_std = robust_std(data)
        assert r_std == pytest.approx(2.0, rel=0.1)

    def test_custom_scale_factor(self):
        data = np.array([1, 2, 3, 4, 5])
        assert robust_std(data, scale_factor=1.0) == pytest.approx(mad(data))


class TestTrimmedMean:
    def test_outlier_resistance(self):
        """Trimmed mean resists outliers better than np.mean."""
        data = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 1000.0])
        tm = trimmed_mean(data, alpha=0.2)
        plain_mean = float(np.mean(data))
        # Trimmed mean should be much closer to median than plain mean
        assert abs(tm - 3.5) < abs(plain_mean - 3.5)

    def test_alpha_zero(self):
        """alpha=0 → plain mean."""
        data = np.array([1.0, 2.0, 3.0, 4.0])
        assert trimmed_mean(data, alpha=0.0) == pytest.approx(np.mean(data))

    def test_invalid_alpha_raises(self):
        with pytest.raises(ValueError, match="alpha must be in"):
            trimmed_mean(np.array([1.0, 2.0]), alpha=0.5)
        with pytest.raises(ValueError, match="alpha must be in"):
            trimmed_mean(np.array([1.0, 2.0]), alpha=-0.1)

    def test_nan_handling(self):
        data = np.array([1.0, np.nan, 3.0, np.nan, 5.0])
        result = trimmed_mean(data, alpha=0.0)
        assert result == pytest.approx(3.0)

    def test_empty(self):
        assert np.isnan(trimmed_mean(np.array([])))
