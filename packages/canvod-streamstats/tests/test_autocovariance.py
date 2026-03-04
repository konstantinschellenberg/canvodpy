"""Tests for StreamingAutocovariance accumulator."""

import numpy as np
import pytest

from canvod.streamstats.accumulators.autocovariance import StreamingAutocovariance


class TestStreamingAutocovarianceEmpty:
    def test_empty_count(self):
        ac = StreamingAutocovariance(max_lag=10)
        assert ac.count == 0

    def test_empty_mean_is_nan(self):
        ac = StreamingAutocovariance(max_lag=10)
        assert np.isnan(ac.mean)

    def test_empty_autocovariance_is_nan(self):
        ac = StreamingAutocovariance(max_lag=10)
        assert np.isnan(ac.autocovariance(0))

    def test_empty_autocorrelation_is_nan(self):
        ac = StreamingAutocovariance(max_lag=10)
        assert np.isnan(ac.autocorrelation(0))

    def test_invalid_max_lag(self):
        with pytest.raises(ValueError, match="max_lag must be >= 1"):
            StreamingAutocovariance(max_lag=0)


class TestStreamingAutocovarianceSineWave:
    """Known sine wave: autocorrelation should peak at lag=period."""

    def test_sine_autocorrelation_structure(self):
        rng = np.random.default_rng(42)
        period = 50
        n = 2000
        t = np.arange(n, dtype=np.float64)
        signal = np.sin(2 * np.pi * t / period) + rng.normal(0, 0.1, n)

        ac = StreamingAutocovariance(max_lag=100)
        ac.update_batch(signal)

        # Autocorrelation at lag 0 should be ~1.0
        assert ac.autocorrelation(0) == pytest.approx(1.0, abs=1e-10)

        # Autocorrelation at lag=period should be positive and high
        acorr_period = ac.autocorrelation(period)
        assert acorr_period > 0.8

        # Autocorrelation at lag=period/2 should be negative (half-period of sine)
        acorr_half = ac.autocorrelation(period // 2)
        assert acorr_half < -0.5


class TestStreamingAutocovarianceBatchVsSequential:
    def test_batch_matches_sequential(self):
        rng = np.random.default_rng(123)
        data = rng.standard_normal(200)

        seq = StreamingAutocovariance(max_lag=20)
        for x in data:
            seq.update(x)

        batch = StreamingAutocovariance(max_lag=20)
        batch.update_batch(data)

        # Autocovariance values should match closely
        np.testing.assert_allclose(
            seq.autocovariance_array, batch.autocovariance_array, rtol=0.05, atol=1e-6
        )


class TestStreamingAutocovarianceIncrementalBatches:
    def test_incremental_batches(self):
        rng = np.random.default_rng(456)
        data = rng.standard_normal(300)

        # Single batch
        single = StreamingAutocovariance(max_lag=20)
        single.update_batch(data)

        # Three incremental batches
        inc = StreamingAutocovariance(max_lag=20)
        inc.update_batch(data[:100])
        inc.update_batch(data[100:200])
        inc.update_batch(data[200:])

        assert inc.count == single.count
        np.testing.assert_allclose(inc.mean, single.mean, rtol=1e-10)


class TestStreamingAutocovarianceNaN:
    def test_nan_values_skipped(self):
        data = np.array([1.0, np.nan, 2.0, np.nan, 3.0])
        ac = StreamingAutocovariance(max_lag=5)
        ac.update_batch(data)
        assert ac.count == 3

    def test_all_nan(self):
        data = np.array([np.nan, np.nan, np.nan])
        ac = StreamingAutocovariance(max_lag=5)
        ac.update_batch(data)
        assert ac.count == 0


class TestStreamingAutocovarianceSerialization:
    def test_roundtrip(self):
        rng = np.random.default_rng(789)
        data = rng.standard_normal(100)

        ac = StreamingAutocovariance(max_lag=20)
        ac.update_batch(data)

        arr = ac.to_array()
        assert arr.shape == (2 * 20 + 5,)

        restored = StreamingAutocovariance.from_array(arr)
        assert restored.count == ac.count
        assert restored.max_lag == ac.max_lag
        np.testing.assert_allclose(restored.mean, ac.mean)
        np.testing.assert_allclose(
            restored.autocovariance_array, ac.autocovariance_array
        )


class TestStreamingAutocovarianceMerge:
    def test_merge_empty(self):
        ac = StreamingAutocovariance(max_lag=10)
        ac.update_batch(np.array([1.0, 2.0, 3.0]))
        other = StreamingAutocovariance(max_lag=10)
        ac.merge(other)
        assert ac.count == 3

    def test_merge_into_empty(self):
        ac = StreamingAutocovariance(max_lag=10)
        other = StreamingAutocovariance(max_lag=10)
        other.update_batch(np.array([1.0, 2.0, 3.0]))
        ac.merge(other)
        assert ac.count == 3

    def test_merge_two(self):
        rng = np.random.default_rng(111)
        data1 = rng.standard_normal(100)
        data2 = rng.standard_normal(100)

        ac1 = StreamingAutocovariance(max_lag=10)
        ac1.update_batch(data1)
        ac2 = StreamingAutocovariance(max_lag=10)
        ac2.update_batch(data2)

        ac1.merge(ac2)
        assert ac1.count == 200

    def test_merge_different_max_lag_raises(self):
        ac1 = StreamingAutocovariance(max_lag=10)
        ac2 = StreamingAutocovariance(max_lag=20)
        ac1.update_batch(np.array([1.0, 2.0]))
        ac2.update_batch(np.array([3.0, 4.0]))
        with pytest.raises(ValueError, match="different max_lag"):
            ac1.merge(ac2)


class TestStreamingAutocovarianceLagZero:
    def test_lag_zero_is_variance(self):
        rng = np.random.default_rng(222)
        data = rng.standard_normal(500)

        ac = StreamingAutocovariance(max_lag=10)
        ac.update_batch(data)

        # Autocovariance at lag 0 should equal population variance
        pop_var = np.var(data)
        assert ac.autocovariance(0) == pytest.approx(pop_var, rel=0.02)

    def test_autocorrelation_zero_is_one(self):
        rng = np.random.default_rng(333)
        data = rng.standard_normal(500)

        ac = StreamingAutocovariance(max_lag=10)
        ac.update_batch(data)

        assert ac.autocorrelation(0) == pytest.approx(1.0, abs=1e-10)
