"""Tests for GKSketch."""

import math

import numpy as np
import pytest

from canvod.streamstats.accumulators.gk_sketch import GKSketch


class TestGKSketch:
    def test_empty(self):
        gk = GKSketch()
        assert gk.count == 0
        assert math.isnan(gk.query(0.5))

    def test_uniform_quantiles(self):
        gk = GKSketch(epsilon=0.01)
        rng = np.random.default_rng(42)
        data = rng.uniform(0, 100, size=10_000)
        for x in data:
            gk.update(float(x))

        assert gk.count == 10_000
        median = gk.query(0.5)
        assert 45.0 < median < 55.0  # rough check

        q10 = gk.query(0.1)
        assert 5.0 < q10 < 15.0

        q90 = gk.query(0.9)
        assert 85.0 < q90 < 95.0

    def test_normal_quantiles(self):
        gk = GKSketch(epsilon=0.01)
        rng = np.random.default_rng(99)
        data = rng.normal(0, 1, size=10_000)
        for x in data:
            gk.update(float(x))

        median = gk.query(0.5)
        assert -0.1 < median < 0.1

    def test_nan_skipped(self):
        gk = GKSketch()
        gk.update(1.0)
        gk.update(float("nan"))
        gk.update(3.0)
        assert gk.count == 2

    def test_merge(self):
        rng = np.random.default_rng(7)
        data = rng.uniform(0, 100, size=10_000)

        gk_full = GKSketch(epsilon=0.01)
        for x in data:
            gk_full.update(float(x))

        gk_a = GKSketch(epsilon=0.01)
        for x in data[:5000]:
            gk_a.update(float(x))

        gk_b = GKSketch(epsilon=0.01)
        for x in data[5000:]:
            gk_b.update(float(x))

        gk_a.merge(gk_b)
        assert gk_a.count == gk_full.count

        # Merged median should be close to full median
        assert abs(gk_a.query(0.5) - gk_full.query(0.5)) < 5.0

    def test_snapshot(self):
        gk = GKSketch(epsilon=0.01)
        rng = np.random.default_rng(42)
        for x in rng.uniform(0, 100, size=5000):
            gk.update(float(x))

        probs = (0.1, 0.5, 0.9)
        result = gk.snapshot(probs)
        assert result.shape == (3,)
        # Quantiles should be monotonically non-decreasing
        assert result[0] <= result[1] <= result[2]

    def test_roundtrip(self):
        gk = GKSketch(epsilon=0.02)
        for x in [1.0, 2.0, 3.0, 4.0, 5.0]:
            gk.update(x)

        arr = gk.to_array()
        gk2 = GKSketch.from_array(arr)

        assert gk2.count == gk.count
        assert gk2.epsilon == pytest.approx(gk.epsilon)
        assert gk2.query(0.5) == pytest.approx(gk.query(0.5))

    def test_epsilon_guarantee(self):
        """Ensure rank error stays within ε * n."""
        eps = 0.01
        gk = GKSketch(epsilon=eps)
        n = 10_000
        rng = np.random.default_rng(123)
        data = rng.uniform(0, 1, size=n)
        sorted_data = np.sort(data)

        for x in data:
            gk.update(float(x))

        for phi in [0.1, 0.25, 0.5, 0.75, 0.9]:
            q = gk.query(phi)
            true_rank = np.searchsorted(sorted_data, q)
            desired_rank = phi * n
            assert abs(true_rank - desired_rank) <= eps * n + 1

    def test_invalid_epsilon(self):
        with pytest.raises(ValueError, match="epsilon"):
            GKSketch(epsilon=0.0)
        with pytest.raises(ValueError, match="epsilon"):
            GKSketch(epsilon=1.0)
