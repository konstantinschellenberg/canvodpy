"""Tests for EWMAAccumulator."""

import math

import numpy as np
import pytest

from canvod.streamstats.accumulators.ewma import EWMAAccumulator


class TestEWMAEmpty:
    def test_count_zero(self):
        e = EWMAAccumulator()
        assert e.count == 0

    def test_mean_nan(self):
        e = EWMAAccumulator()
        assert math.isnan(e.mean)

    def test_variance_nan(self):
        e = EWMAAccumulator()
        assert math.isnan(e.variance)

    def test_std_nan(self):
        e = EWMAAccumulator()
        assert math.isnan(e.std)

    def test_last_value_nan(self):
        e = EWMAAccumulator()
        assert math.isnan(e.last_value)


class TestEWMAInvalidArgs:
    def test_half_life_zero(self):
        with pytest.raises(ValueError, match="half_life"):
            EWMAAccumulator(half_life=0.0)

    def test_half_life_negative(self):
        with pytest.raises(ValueError, match="half_life"):
            EWMAAccumulator(half_life=-1.0)

    def test_alpha_zero(self):
        with pytest.raises(ValueError, match="alpha"):
            EWMAAccumulator(alpha=0.0)

    def test_alpha_negative(self):
        with pytest.raises(ValueError, match="alpha"):
            EWMAAccumulator(alpha=-0.5)

    def test_alpha_above_one(self):
        with pytest.raises(ValueError, match="alpha"):
            EWMAAccumulator(alpha=1.5)


class TestEWMASingleValue:
    def test_mean_equals_value(self):
        e = EWMAAccumulator()
        e.update(7.0)
        assert e.count == 1
        assert e.mean == 7.0
        assert e.last_value == 7.0

    def test_variance_nan_single(self):
        e = EWMAAccumulator()
        e.update(7.0)
        assert math.isnan(e.variance)


class TestEWMAKnownSequence:
    def test_constant_convergence(self):
        """EWMA of a constant should converge to that constant."""
        e = EWMAAccumulator(half_life=5.0)
        for _ in range(100):
            e.update(42.0)
        assert e.mean == pytest.approx(42.0)
        assert e.variance == pytest.approx(0.0, abs=1e-10)

    def test_step_change_tracking(self):
        """EWMA should track a step change."""
        e = EWMAAccumulator(half_life=3.0)
        for _ in range(50):
            e.update(10.0)
        for _ in range(50):
            e.update(20.0)
        # After 50 samples at 20, EWMA should be close to 20
        assert e.mean == pytest.approx(20.0, abs=0.01)

    def test_alpha_one_identity(self):
        """With alpha=1, EWMA mean equals the last value."""
        e = EWMAAccumulator(alpha=1.0)
        for x in [1.0, 5.0, 3.0, 8.0]:
            e.update(x)
        assert e.mean == 8.0
        assert e.last_value == 8.0

    def test_explicit_alpha_vs_halflife(self):
        """Verify alpha = 1 - 2^(-1/half_life) relationship."""
        hl = 10.0
        e = EWMAAccumulator(half_life=hl)
        expected_alpha = 1.0 - 2.0 ** (-1.0 / hl)
        assert e.alpha == pytest.approx(expected_alpha)


class TestEWMANanHandling:
    def test_nan_skipped(self):
        e = EWMAAccumulator()
        e.update(1.0)
        e.update(float("nan"))
        e.update(3.0)
        assert e.count == 2
        assert e.n_nan == 1

    def test_batch_with_nans(self):
        e = EWMAAccumulator()
        e.update_batch(np.array([1.0, float("nan"), 3.0, float("nan"), 5.0]))
        assert e.count == 3
        assert e.n_nan == 2


class TestEWMABatchVsSequential:
    def test_batch_matches_sequential(self):
        rng = np.random.default_rng(42)
        data = rng.normal(10.0, 3.0, size=200)
        e_seq = EWMAAccumulator(half_life=15.0)
        for x in data:
            e_seq.update(float(x))
        e_batch = EWMAAccumulator(half_life=15.0)
        e_batch.update_batch(data)
        assert e_seq.count == e_batch.count
        assert e_seq.mean == pytest.approx(e_batch.mean)
        assert e_seq.variance == pytest.approx(e_batch.variance)


class TestEWMASerialization:
    def test_to_array_length(self):
        e = EWMAAccumulator()
        e.update_batch(np.array([1.0, 2.0, 3.0]))
        arr = e.to_array()
        assert arr.shape == (6,)

    def test_roundtrip(self):
        e = EWMAAccumulator(half_life=7.0)
        e.update_batch(np.array([1.0, 2.0, 3.0, float("nan"), 5.0]))
        arr = e.to_array()
        e2 = EWMAAccumulator.from_array(arr)
        assert e2.count == e.count
        assert e2.mean == pytest.approx(e.mean)
        assert e2.variance == pytest.approx(e.variance)
        assert e2.alpha == pytest.approx(e.alpha)
        assert e2.n_nan == e.n_nan
        assert e2.last_value == pytest.approx(e.last_value)


class TestEWMAMerge:
    def test_right_biased(self):
        e1 = EWMAAccumulator(half_life=10.0)
        e1.update_batch(np.array([1.0, 2.0, 3.0]))
        e2 = EWMAAccumulator(half_life=10.0)
        e2.update_batch(np.array([100.0, 200.0, 300.0]))
        e1.merge(e2)
        # Should adopt e2's EWMA state (right-biased)
        assert e1.mean == pytest.approx(e2.mean)
        assert e1.count == 6  # sum of counts

    def test_merge_empty_left(self):
        e1 = EWMAAccumulator()
        e2 = EWMAAccumulator()
        e2.update_batch(np.array([10.0, 20.0]))
        e1.merge(e2)
        assert e1.count == 2
        assert e1.mean == pytest.approx(e2.mean)

    def test_merge_empty_right(self):
        e1 = EWMAAccumulator()
        e1.update_batch(np.array([10.0, 20.0]))
        mean_before = e1.mean
        e2 = EWMAAccumulator()
        e1.merge(e2)
        assert e1.mean == pytest.approx(mean_before)
        assert e1.count == 2
