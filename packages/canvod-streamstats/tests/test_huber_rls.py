"""Tests for HuberRLS accumulator."""

import numpy as np
import pytest

from canvod.streamstats.accumulators.huber_rls import HuberRLS
from canvod.streamstats.accumulators.rls import RecursiveLeastSquares


class TestHuberRLSEmpty:
    def test_empty_state(self):
        h = HuberRLS(n_features=2)
        assert h.count == 0
        np.testing.assert_array_equal(h.beta, np.zeros(2))
        assert h.n_features == 2


class TestHuberRLSInvalidParams:
    def test_invalid_n_features(self):
        with pytest.raises(ValueError, match="n_features"):
            HuberRLS(n_features=0)

    def test_invalid_forgetting_factor(self):
        with pytest.raises(ValueError, match="forgetting_factor"):
            HuberRLS(forgetting_factor=0.0)

    def test_invalid_huber_c(self):
        with pytest.raises(ValueError, match="huber_c"):
            HuberRLS(huber_c=-1.0)


class TestHuberRLSCleanConvergence:
    def test_converges_on_clean_data(self):
        """y = 2*x1 + 3*x2 + 1 with no outliers."""
        rng = np.random.default_rng(42)
        true_beta = np.array([2.0, 3.0, 1.0])
        n = 500

        X = np.column_stack(
            [
                rng.normal(size=n),
                rng.normal(size=n),
                np.ones(n),
            ]
        )
        y = X @ true_beta + rng.normal(scale=0.1, size=n)

        h = HuberRLS(n_features=3, forgetting_factor=0.999)
        h.update_batch(X, y)

        np.testing.assert_allclose(h.beta, true_beta, atol=0.15)
        assert h.count == n


class TestHuberRLSOutlierRobustness:
    def test_huber_beats_standard_rls_with_outliers(self):
        """HuberRLS should be closer to true beta than standard RLS on contaminated data."""
        rng = np.random.default_rng(99)
        true_beta = np.array([2.0, 3.0, 1.0])
        n = 500

        X = np.column_stack(
            [
                rng.normal(size=n),
                rng.normal(size=n),
                np.ones(n),
            ]
        )
        y = X @ true_beta + rng.normal(scale=0.1, size=n)

        # Contaminate 10% of observations with large outliers
        n_outliers = n // 10
        outlier_idx = rng.choice(n, size=n_outliers, replace=False)
        y[outlier_idx] += rng.choice([-50, 50], size=n_outliers)

        huber = HuberRLS(n_features=3, forgetting_factor=0.999, huber_c=1.345)
        huber.update_batch(X, y)

        standard = RecursiveLeastSquares(n_features=3, forgetting_factor=0.999)
        standard.update_batch(X, y)

        huber_error = np.linalg.norm(huber.beta - true_beta)
        standard_error = np.linalg.norm(standard.beta - true_beta)
        assert huber_error < standard_error


class TestHuberRLSNaN:
    def test_nan_skipped(self):
        h = HuberRLS(n_features=2)
        h.update(np.array([1.0, np.nan]), 1.0)
        assert h.count == 0
        h.update(np.array([1.0, 2.0]), np.nan)
        assert h.count == 0
        h.update(np.array([1.0, 2.0]), 3.0)
        assert h.count == 1


class TestHuberRLSSerialization:
    def test_roundtrip(self):
        rng = np.random.default_rng(7)
        h = HuberRLS(n_features=3, forgetting_factor=0.998, huber_c=2.0)
        X = rng.normal(size=(50, 3))
        y = rng.normal(size=50)
        h.update_batch(X, y)

        arr = h.to_array()
        assert arr.shape == (4 + 3 + 9,)

        restored = HuberRLS.from_array(arr)
        assert restored.count == h.count
        assert restored.huber_c == pytest.approx(h.huber_c)
        assert restored.forgetting_factor == pytest.approx(h.forgetting_factor)
        np.testing.assert_array_almost_equal(restored.beta, h.beta)
        np.testing.assert_array_almost_equal(restored.P, h.P)


class TestHuberRLSMerge:
    def test_merge_right_biased(self):
        h1 = HuberRLS(n_features=2)
        h2 = HuberRLS(n_features=2)

        rng = np.random.default_rng(42)
        X = rng.normal(size=(20, 2))
        y = rng.normal(size=20)
        h2.update_batch(X, y)

        h1.merge(h2)
        assert h1.count == h2.count
        np.testing.assert_array_equal(h1.beta, h2.beta)

    def test_merge_empty_other(self):
        h1 = HuberRLS(n_features=2)
        rng = np.random.default_rng(42)
        h1.update(rng.normal(size=2), 1.0)
        original_count = h1.count

        h2 = HuberRLS(n_features=2)
        h1.merge(h2)
        assert h1.count == original_count


class TestHuberRLSBatchVsSequential:
    def test_batch_matches_sequential(self):
        rng = np.random.default_rng(55)
        X = rng.normal(size=(30, 3))
        y = rng.normal(size=30)

        seq = HuberRLS(n_features=3, forgetting_factor=0.999, huber_c=1.345)
        for i in range(30):
            seq.update(X[i], y[i])

        batch = HuberRLS(n_features=3, forgetting_factor=0.999, huber_c=1.345)
        batch.update_batch(X, y)

        assert seq.count == batch.count
        np.testing.assert_allclose(seq.beta, batch.beta, atol=1e-12)
        np.testing.assert_allclose(seq.P, batch.P, atol=1e-10)
