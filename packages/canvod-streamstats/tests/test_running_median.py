"""Tests for RunningMedianFilter."""

import math

import numpy as np
import pytest

from canvod.streamstats.accumulators.running_median import RunningMedianFilter


class TestRunningMedianEmpty:
    def test_count_zero(self):
        f = RunningMedianFilter()
        assert f.count == 0

    def test_median_nan(self):
        f = RunningMedianFilter()
        assert math.isnan(f.median)

    def test_buffer_empty(self):
        f = RunningMedianFilter()
        assert len(f.buffer_values) == 0


class TestRunningMedianInvalidArgs:
    def test_window_zero(self):
        with pytest.raises(ValueError, match="window must be >= 1"):
            RunningMedianFilter(window=0)

    def test_window_negative(self):
        with pytest.raises(ValueError, match="window must be >= 1"):
            RunningMedianFilter(window=-3)

    def test_window_even(self):
        with pytest.raises(ValueError, match="window must be odd"):
            RunningMedianFilter(window=4)


class TestRunningMedianSingleValue:
    def test_median_equals_value(self):
        f = RunningMedianFilter()
        f.update(7.0)
        assert f.count == 1
        assert f.median == 7.0


class TestRunningMedianKnown:
    def test_window_of_five(self):
        f = RunningMedianFilter(window=5)
        for x in [1.0, 2.0, 3.0, 4.0, 5.0]:
            f.update(x)
        assert f.median == 3.0

    def test_window_overflow(self):
        """After overflow, oldest values are evicted."""
        f = RunningMedianFilter(window=3)
        for x in [1.0, 2.0, 3.0]:
            f.update(x)
        assert f.median == 2.0
        f.update(100.0)  # buffer: [100, 2, 3]
        assert f.median == 3.0

    def test_partial_window(self):
        f = RunningMedianFilter(window=5)
        f.update(10.0)
        f.update(20.0)
        assert f.median == 15.0  # nanmedian of [10, 20]

    def test_buffer_values_chronological(self):
        f = RunningMedianFilter(window=3)
        for x in [1.0, 2.0, 3.0, 4.0]:
            f.update(x)
        # Buffer wraps; chronological order should be [2, 3, 4]
        np.testing.assert_array_equal(f.buffer_values, [2.0, 3.0, 4.0])


class TestRunningMedianNan:
    def test_nan_skipped(self):
        f = RunningMedianFilter()
        f.update(1.0)
        f.update(float("nan"))
        f.update(3.0)
        assert f.count == 2
        assert f.n_nan == 1

    def test_batch_with_nans(self):
        f = RunningMedianFilter()
        f.update_batch(np.array([1.0, float("nan"), 3.0]))
        assert f.count == 2
        assert f.n_nan == 1


class TestRunningMedianSerialization:
    def test_roundtrip(self):
        f = RunningMedianFilter(window=7)
        f.update_batch(np.array([1.0, 2.0, 3.0, 4.0, 5.0]))
        arr = f.to_array()
        assert arr.shape == (4 + 7,)
        f2 = RunningMedianFilter.from_array(arr)
        assert f2.count == f.count
        assert f2.window == f.window
        assert f2.median == pytest.approx(f.median)
        assert f2.n_nan == f.n_nan

    def test_roundtrip_after_overflow(self):
        f = RunningMedianFilter(window=3)
        f.update_batch(np.array([1.0, 2.0, 3.0, 4.0, 5.0]))
        arr = f.to_array()
        f2 = RunningMedianFilter.from_array(arr)
        assert f2.median == pytest.approx(f.median)
        assert f2.count == f.count


class TestRunningMedianMerge:
    def test_right_biased(self):
        f1 = RunningMedianFilter(window=5)
        f1.update_batch(np.array([1.0, 2.0, 3.0]))
        f2 = RunningMedianFilter(window=5)
        f2.update_batch(np.array([100.0, 200.0, 300.0]))
        f1.merge(f2)
        assert f1.median == pytest.approx(f2.median)
        assert f1.count == 6

    def test_merge_empty_right(self):
        f1 = RunningMedianFilter(window=5)
        f1.update_batch(np.array([1.0, 2.0, 3.0]))
        median_before = f1.median
        f2 = RunningMedianFilter(window=5)
        f1.merge(f2)
        assert f1.median == pytest.approx(median_before)

    def test_merge_different_windows_raises(self):
        f1 = RunningMedianFilter(window=3)
        f2 = RunningMedianFilter(window=5)
        f2.update(1.0)
        with pytest.raises(ValueError, match="different windows"):
            f1.merge(f2)
