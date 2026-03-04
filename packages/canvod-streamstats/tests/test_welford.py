"""Tests for WelfordAccumulator."""

import math

import numpy as np
import pytest

from canvod.streamstats.accumulators.welford import WelfordAccumulator


class TestWelfordAccumulator:
    def test_empty(self):
        w = WelfordAccumulator()
        assert w.count == 0
        assert math.isnan(w.mean)
        assert math.isnan(w.variance)
        assert math.isnan(w.min)
        assert math.isnan(w.max)

    def test_single_value(self):
        w = WelfordAccumulator()
        w.update(5.0)
        assert w.count == 1
        assert w.mean == 5.0
        assert math.isnan(w.variance)  # n < 2
        assert w.min == 5.0
        assert w.max == 5.0

    def test_known_sequence(self):
        """Test against known values: [2, 4, 4, 4, 5, 5, 7, 9]."""
        data = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
        w = WelfordAccumulator()
        for x in data:
            w.update(x)

        assert w.count == 8
        assert w.mean == pytest.approx(5.0)
        # Sample variance (Bessel-corrected): M2/(n-1) = 32/7 ≈ 4.571
        assert w.variance == pytest.approx(32.0 / 7.0)
        assert w.std == pytest.approx(math.sqrt(32.0 / 7.0))
        assert w.min == 2.0
        assert w.max == 9.0

    def test_nan_handling(self):
        w = WelfordAccumulator()
        w.update(1.0)
        w.update(float("nan"))
        w.update(3.0)
        assert w.count == 2
        assert w.n_nan == 1
        assert w.mean == pytest.approx(2.0)

    def test_batch_equals_sequential(self):
        rng = np.random.default_rng(42)
        data = rng.normal(10.0, 3.0, size=200)

        w_seq = WelfordAccumulator()
        for x in data:
            w_seq.update(float(x))

        w_batch = WelfordAccumulator()
        w_batch.update_batch(data)

        assert w_seq.count == w_batch.count
        assert w_seq.mean == pytest.approx(w_batch.mean)
        assert w_seq.variance == pytest.approx(w_batch.variance)
        assert w_seq.skewness == pytest.approx(w_batch.skewness, abs=1e-10)
        assert w_seq.kurtosis == pytest.approx(w_batch.kurtosis, abs=1e-10)

    def test_merge_halves(self):
        rng = np.random.default_rng(99)
        data = rng.normal(0.0, 1.0, size=1000)

        w_full = WelfordAccumulator()
        w_full.update_batch(data)

        w_a = WelfordAccumulator()
        w_a.update_batch(data[:500])
        w_b = WelfordAccumulator()
        w_b.update_batch(data[500:])
        w_a.merge(w_b)

        assert w_a.count == w_full.count
        assert w_a.mean == pytest.approx(w_full.mean, rel=1e-12)
        assert w_a.variance == pytest.approx(w_full.variance, rel=1e-10)
        assert w_a.min == w_full.min
        assert w_a.max == w_full.max

    def test_merge_commutative(self):
        rng = np.random.default_rng(7)
        a = rng.normal(5, 2, size=100)
        b = rng.normal(10, 3, size=150)

        w1 = WelfordAccumulator()
        w1.update_batch(a)
        w1_copy = WelfordAccumulator.from_array(w1.to_array())

        w2 = WelfordAccumulator()
        w2.update_batch(b)
        w2_copy = WelfordAccumulator.from_array(w2.to_array())

        w1.merge(w2)
        w2_copy.merge(w1_copy)

        assert w1.count == w2_copy.count
        assert w1.mean == pytest.approx(w2_copy.mean, rel=1e-12)
        assert w1.variance == pytest.approx(w2_copy.variance, rel=1e-10)

    def test_roundtrip(self):
        w = WelfordAccumulator()
        w.update_batch(np.array([1.0, 2.0, 3.0, float("nan"), 5.0]))

        arr = w.to_array()
        assert arr.shape == (8,)

        w2 = WelfordAccumulator.from_array(arr)
        assert w2.count == w.count
        assert w2.mean == pytest.approx(w.mean)
        assert w2.variance == pytest.approx(w.variance)
        assert w2.n_nan == w.n_nan

    def test_merge_with_empty(self):
        w = WelfordAccumulator()
        w.update_batch(np.array([1.0, 2.0, 3.0]))
        empty = WelfordAccumulator()

        mean_before = w.mean
        w.merge(empty)
        assert w.mean == mean_before
        assert w.count == 3

    def test_numerical_stability_large_offset(self):
        """Values with large mean should still produce correct variance."""
        data = np.array([1e9 + 1, 1e9 + 2, 1e9 + 3, 1e9 + 4, 1e9 + 5])
        w = WelfordAccumulator()
        w.update_batch(data)
        assert w.variance == pytest.approx(2.5, rel=1e-6)
