"""Tests for IncrementalPCA accumulator."""

import numpy as np
import pytest

from canvod.streamstats.accumulators.pca import IncrementalPCA


class TestIncrementalPCAEmpty:
    def test_empty_count(self):
        pca = IncrementalPCA(n_variables=5, n_components=2)
        assert pca.count == 0

    def test_empty_explained_variance_is_nan(self):
        pca = IncrementalPCA(n_variables=5, n_components=2)
        assert np.all(np.isnan(pca.explained_variance))

    def test_invalid_n_components(self):
        with pytest.raises(ValueError, match="n_components must be >= 1"):
            IncrementalPCA(n_variables=5, n_components=0)

    def test_invalid_n_variables(self):
        with pytest.raises(ValueError, match=r"n_variables.*must be >= n_components"):
            IncrementalPCA(n_variables=2, n_components=5)


class TestIncrementalPCAKnownCovariance:
    """Test with known covariance structure — first component should align with major axis."""

    def test_known_axes(self):
        rng = np.random.default_rng(42)
        n = 2000
        d = 5
        k = 2

        # Create data with known covariance structure
        # Variance along dim 0 = 10, dim 1 = 5, rest = 1
        scales = np.array([10.0, 5.0, 1.0, 1.0, 1.0])
        X = rng.standard_normal((n, d)) * scales

        pca = IncrementalPCA(n_variables=d, n_components=k)
        pca.update_batch(X)

        # First component should capture most variance (aligned with dim 0)
        assert pca.explained_variance_ratio[0] > 0.5

        # First component should point mostly along dimension 0
        first_component = pca.components[0]
        assert abs(first_component[0]) > 0.8


class TestIncrementalPCABatchVsFullSVD:
    def test_batch_vs_full_svd(self):
        rng = np.random.default_rng(123)
        n = 500
        d = 6
        k = 3

        X = rng.standard_normal((n, d))

        pca = IncrementalPCA(n_variables=d, n_components=k)
        pca.update_batch(X)

        # Compare with full SVD
        X_centered = X - X.mean(axis=0)
        U, s, Vt = np.linalg.svd(X_centered, full_matrices=False)

        # Singular values should match
        np.testing.assert_allclose(pca.singular_values, s[:k], rtol=0.01)


class TestIncrementalPCAIncrementalBatches:
    def test_incremental_batches_consistent(self):
        rng = np.random.default_rng(456)
        n = 600
        d = 8
        k = 3

        X = rng.standard_normal((n, d))

        # Single batch
        single = IncrementalPCA(n_variables=d, n_components=k)
        single.update_batch(X)

        # Three incremental batches
        inc = IncrementalPCA(n_variables=d, n_components=k)
        inc.update_batch(X[:200])
        inc.update_batch(X[200:400])
        inc.update_batch(X[400:])

        assert inc.count == single.count
        np.testing.assert_allclose(inc.mean, single.mean, atol=1e-10)

        # Singular values should be similar (not exact due to incremental approximation)
        np.testing.assert_allclose(
            inc.singular_values, single.singular_values, rtol=0.15
        )


class TestIncrementalPCANaN:
    def test_nan_rows_skipped(self):
        X = np.array(
            [
                [1.0, 2.0, 3.0],
                [np.nan, 5.0, 6.0],
                [7.0, 8.0, 9.0],
            ]
        )
        pca = IncrementalPCA(n_variables=3, n_components=2)
        pca.update_batch(X)
        assert pca.count == 2

    def test_all_nan(self):
        X = np.full((3, 4), np.nan)
        pca = IncrementalPCA(n_variables=4, n_components=2)
        pca.update_batch(X)
        assert pca.count == 0


class TestIncrementalPCASerialization:
    def test_roundtrip(self):
        rng = np.random.default_rng(789)
        d, k = 14, 5
        X = rng.standard_normal((100, d))

        pca = IncrementalPCA(n_variables=d, n_components=k)
        pca.update_batch(X)

        arr = pca.to_array()
        assert arr.shape == (3 + d + k + d * k,)  # 3 + 14 + 5 + 70 = 92

        restored = IncrementalPCA.from_array(arr)
        assert restored.count == pca.count
        np.testing.assert_allclose(restored.mean, pca.mean)
        np.testing.assert_allclose(restored.singular_values, pca.singular_values)
        np.testing.assert_allclose(restored.components, pca.components)


class TestIncrementalPCAMerge:
    def test_merge_empty(self):
        pca = IncrementalPCA(n_variables=5, n_components=2)
        pca.update_batch(np.random.default_rng(1).standard_normal((50, 5)))
        other = IncrementalPCA(n_variables=5, n_components=2)
        pca.merge(other)
        assert pca.count == 50

    def test_merge_into_empty(self):
        pca = IncrementalPCA(n_variables=5, n_components=2)
        other = IncrementalPCA(n_variables=5, n_components=2)
        other.update_batch(np.random.default_rng(1).standard_normal((50, 5)))
        pca.merge(other)
        assert pca.count == 50

    def test_merge_two(self):
        rng = np.random.default_rng(111)
        d, k = 5, 2

        pca1 = IncrementalPCA(n_variables=d, n_components=k)
        pca1.update_batch(rng.standard_normal((100, d)))

        pca2 = IncrementalPCA(n_variables=d, n_components=k)
        pca2.update_batch(rng.standard_normal((100, d)))

        pca1.merge(pca2)
        assert pca1.count == 200

    def test_merge_different_dims_raises(self):
        pca1 = IncrementalPCA(n_variables=5, n_components=2)
        pca2 = IncrementalPCA(n_variables=6, n_components=2)
        pca1.update_batch(np.random.default_rng(1).standard_normal((10, 5)))
        pca2.update_batch(np.random.default_rng(2).standard_normal((10, 6)))
        with pytest.raises(ValueError, match="different dims"):
            pca1.merge(pca2)


class TestIncrementalPCAExplainedVariance:
    def test_explained_variance_ratio_sums_to_near_one(self):
        """When k == d, explained variance ratio should sum to ~1.0."""
        rng = np.random.default_rng(222)
        d = 4
        k = 4

        # Use data with clear variance structure
        scales = np.array([5.0, 3.0, 1.0, 0.5])
        X = rng.standard_normal((1000, d)) * scales

        pca = IncrementalPCA(n_variables=d, n_components=k)
        pca.update_batch(X)

        ratio_sum = np.sum(pca.explained_variance_ratio)
        assert ratio_sum == pytest.approx(1.0, abs=0.01)
