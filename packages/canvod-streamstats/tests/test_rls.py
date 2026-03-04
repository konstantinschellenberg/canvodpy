"""Tests for RecursiveLeastSquares accumulator."""

import numpy as np
import pytest

from canvod.streamstats.accumulators.rls import RecursiveLeastSquares


class TestRecursiveLeastSquaresEmpty:
    def test_empty_count(self):
        rls = RecursiveLeastSquares(n_features=3)
        assert rls.count == 0

    def test_empty_beta_is_zero(self):
        rls = RecursiveLeastSquares(n_features=3)
        np.testing.assert_array_equal(rls.beta, np.zeros(3))

    def test_invalid_n_features(self):
        with pytest.raises(ValueError, match="n_features must be >= 1"):
            RecursiveLeastSquares(n_features=0)

    def test_invalid_forgetting_factor(self):
        with pytest.raises(ValueError, match="forgetting_factor must be in"):
            RecursiveLeastSquares(forgetting_factor=0.0)


class TestRecursiveLeastSquaresLinearFit:
    """Test convergence on y = 2*x1 + 3*x2 + 1."""

    def test_linear_convergence(self):
        rng = np.random.default_rng(42)
        true_beta = np.array([2.0, 3.0, 1.0])
        n = 1000

        rls = RecursiveLeastSquares(n_features=3, forgetting_factor=0.999)
        for _ in range(n):
            x = np.array([rng.standard_normal(), rng.standard_normal(), 1.0])
            y = x @ true_beta + rng.normal(0, 0.01)
            rls.update(x, y)

        np.testing.assert_allclose(rls.beta, true_beta, atol=0.1)
        assert rls.count == n


class TestRecursiveLeastSquaresBatchVsSequential:
    def test_batch_matches_sequential(self):
        rng = np.random.default_rng(123)
        true_beta = np.array([1.5, -0.5, 2.0])
        n = 200

        X = np.column_stack([rng.standard_normal((n, 2)), np.ones(n)])
        y = X @ true_beta + rng.normal(0, 0.01, n)

        seq = RecursiveLeastSquares(n_features=3, forgetting_factor=1.0)
        for i in range(n):
            seq.update(X[i], y[i])

        batch = RecursiveLeastSquares(n_features=3, forgetting_factor=1.0)
        batch.update_batch(X, y)

        np.testing.assert_allclose(seq.beta, batch.beta, atol=0.15)
        assert batch.count == n


class TestRecursiveLeastSquaresForgetting:
    def test_forgetting_adapts_to_change(self):
        rng = np.random.default_rng(456)
        p = 2

        rls = RecursiveLeastSquares(n_features=p, forgetting_factor=0.95)

        # Phase 1: y = 1*x + 0
        for _ in range(500):
            x = np.array([rng.standard_normal(), 1.0])
            y = 1.0 * x[0] + rng.normal(0, 0.01)
            rls.update(x, y)

        # Phase 2: y = -1*x + 0
        for _ in range(500):
            x = np.array([rng.standard_normal(), 1.0])
            y = -1.0 * x[0] + rng.normal(0, 0.01)
            rls.update(x, y)

        # With forgetting, beta[0] should have adapted toward -1
        assert rls.beta[0] < 0.0


class TestRecursiveLeastSquaresNaN:
    def test_nan_rows_skipped(self):
        rls = RecursiveLeastSquares(n_features=2)
        X = np.array([[1.0, 2.0], [np.nan, 3.0], [4.0, 5.0]])
        y = np.array([1.0, 2.0, 3.0])
        rls.update_batch(X, y)
        assert rls.count == 2

    def test_nan_y_skipped(self):
        rls = RecursiveLeastSquares(n_features=2)
        X = np.array([[1.0, 2.0], [3.0, 4.0]])
        y = np.array([1.0, np.nan])
        rls.update_batch(X, y)
        assert rls.count == 1

    def test_scalar_nan_skipped(self):
        rls = RecursiveLeastSquares(n_features=2)
        rls.update(np.array([1.0, np.nan]), 5.0)
        assert rls.count == 0


class TestRecursiveLeastSquaresSerialization:
    def test_roundtrip(self):
        rng = np.random.default_rng(789)
        rls = RecursiveLeastSquares(n_features=3, forgetting_factor=0.99)

        X = rng.standard_normal((50, 3))
        y = X @ np.array([1.0, 2.0, 3.0]) + rng.normal(0, 0.01, 50)
        rls.update_batch(X, y)

        arr = rls.to_array()
        assert arr.shape == (3 + 3 + 9,)

        restored = RecursiveLeastSquares.from_array(arr)
        assert restored.count == rls.count
        assert restored.n_features == rls.n_features
        assert restored.forgetting_factor == pytest.approx(rls.forgetting_factor)
        np.testing.assert_allclose(restored.beta, rls.beta)
        np.testing.assert_allclose(restored.P, rls.P)


class TestRecursiveLeastSquaresBatchExact:
    def test_batch_exact_for_small_p(self):
        """With lambda=1 and enough data, batch RLS should match OLS."""
        rng = np.random.default_rng(111)
        true_beta = np.array([2.0, -1.0, 0.5])
        n = 500

        X = rng.standard_normal((n, 3))
        y = X @ true_beta + rng.normal(0, 0.01, n)

        rls = RecursiveLeastSquares(n_features=3, forgetting_factor=1.0)
        rls.update_batch(X, y)

        # OLS solution
        ols_beta = np.linalg.lstsq(X, y, rcond=None)[0]
        np.testing.assert_allclose(rls.beta, ols_beta, atol=0.05)


class TestRecursiveLeastSquaresSingleObservation:
    def test_single_observation(self):
        rls = RecursiveLeastSquares(n_features=2)
        rls.update(np.array([1.0, 0.0]), 5.0)
        assert rls.count == 1
        # After one observation, beta should be non-zero
        assert not np.all(rls.beta == 0.0)


class TestRecursiveLeastSquaresMerge:
    def test_merge_empty(self):
        rls = RecursiveLeastSquares(n_features=2)
        rls.update(np.array([1.0, 0.0]), 5.0)
        other = RecursiveLeastSquares(n_features=2)
        rls.merge(other)
        assert rls.count == 1

    def test_merge_right_biased(self):
        rls1 = RecursiveLeastSquares(n_features=2)
        rls2 = RecursiveLeastSquares(n_features=2)

        rls1.update(np.array([1.0, 0.0]), 1.0)
        rls2.update(np.array([1.0, 0.0]), 2.0)
        rls2.update(np.array([0.0, 1.0]), 3.0)

        rls1.merge(rls2)
        # rls2 had higher count, so rls1 should now have rls2's state
        assert rls1.count == 2
