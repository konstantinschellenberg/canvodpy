"""Tests for CAR/ICAR spatial smoothing."""

import numpy as np
import pytest
from scipy import sparse

from canvod.streamstats.bayesian.spatial import (
    CARResult,
    adjacency_from_grid,
    car_smooth,
    icar_smooth,
)


def _chain_adjacency(n: int) -> sparse.csr_matrix:
    """Build a chain (path) graph adjacency matrix of size n."""
    diags = np.ones(n - 1)
    W = sparse.diags([diags, diags], offsets=[-1, 1], shape=(n, n), format="csr")
    return W


class TestCARNoSpatial:
    def test_rho_zero_returns_raw(self):
        """rho=0 (no spatial): smoothed ≈ raw (tau dominates)."""
        rng = np.random.default_rng(42)
        n = 20
        values = rng.normal(0, 1, n)
        W = _chain_adjacency(n)

        result = car_smooth(values, W, tau=100.0, rho=0.0)
        # With rho=0 and high tau, Q = tau*I, solution = values
        np.testing.assert_allclose(result.smoothed, values, atol=0.1)


class TestCARSmoothing:
    def test_noisy_chain_lower_variance(self):
        """Noisy chain graph: smoothed should have lower variance than raw."""
        rng = np.random.default_rng(123)
        n = 50
        # Smooth underlying signal + noise
        signal = np.sin(np.linspace(0, 2 * np.pi, n))
        noise = rng.normal(0, 0.5, n)
        values = signal + noise
        W = _chain_adjacency(n)

        result = car_smooth(values, W, tau=1.0, rho=0.99)

        raw_var = np.var(values - signal)
        smooth_var = np.var(result.smoothed - signal)
        assert smooth_var < raw_var

    def test_constant_field_unchanged(self):
        """Constant field should remain approximately constant after smoothing.

        With a chain graph, boundary nodes have degree 1 vs interior degree 2,
        causing slight deviation from the constant. Use high tau to minimize it.
        """
        n = 30
        values = np.full(n, 7.0)
        W = _chain_adjacency(n)

        result = car_smooth(values, W, tau=10.0, rho=0.99)
        np.testing.assert_allclose(result.smoothed, 7.0, atol=0.02)

    def test_single_outlier_pulled(self):
        """Single outlier should be pulled toward neighbors."""
        n = 20
        values = np.zeros(n)
        values[10] = 10.0  # outlier
        W = _chain_adjacency(n)

        result = car_smooth(values, W, tau=0.5, rho=0.99)

        # Outlier should be dampened
        assert result.smoothed[10] < 10.0
        # Neighbors should be slightly raised
        assert result.smoothed[9] > 0.0
        assert result.smoothed[11] > 0.0


class TestICARSmoothing:
    def test_icar_works(self):
        """ICAR smoothing produces finite output."""
        rng = np.random.default_rng(456)
        n = 30
        values = rng.normal(0, 1, n)
        W = _chain_adjacency(n)

        result = icar_smooth(values, W, tau=1.0)
        assert np.all(np.isfinite(result.smoothed))
        assert result.effective_cells == n

    def test_icar_close_to_car_099(self):
        """ICAR ≈ CAR(ρ=0.99) for moderate tau."""
        rng = np.random.default_rng(789)
        n = 30
        values = rng.normal(0, 1, n)
        W = _chain_adjacency(n)

        r_car = car_smooth(values, W, tau=1.0, rho=0.99)
        r_icar = icar_smooth(values, W, tau=1.0)

        # Should be close but not identical (rho=1.0 vs 0.99)
        np.testing.assert_allclose(r_car.smoothed, r_icar.smoothed, atol=0.5)


class TestCARNaN:
    def test_nan_cells_get_finite(self):
        """NaN cells should get finite smoothed values."""
        n = 20
        values = np.ones(n)
        values[5] = np.nan
        values[15] = np.nan
        W = _chain_adjacency(n)

        result = car_smooth(values, W, tau=1.0, rho=0.99)
        assert np.all(np.isfinite(result.smoothed))
        assert result.effective_cells == 18

    def test_all_nan(self):
        """All-NaN input produces NaN output."""
        n = 10
        values = np.full(n, np.nan)
        W = _chain_adjacency(n)

        result = car_smooth(values, W, tau=1.0, rho=0.99)
        assert np.all(np.isnan(result.smoothed))
        assert result.effective_cells == 0


class TestCARResiduals:
    def test_residuals_equal_raw_minus_smoothed(self):
        """residuals = raw - smoothed (for finite cells)."""
        rng = np.random.default_rng(101)
        n = 20
        values = rng.normal(0, 1, n)
        W = _chain_adjacency(n)

        result = car_smooth(values, W, tau=1.0, rho=0.99)
        expected = values - result.smoothed
        np.testing.assert_allclose(result.residuals, expected, atol=1e-10)

    def test_effective_cells_counts_finite(self):
        n = 15
        values = np.ones(n)
        values[3] = np.nan
        values[7] = np.nan
        W = _chain_adjacency(n)

        result = car_smooth(values, W, tau=1.0, rho=0.99)
        assert result.effective_cells == 13


class TestAdjacencyFromGrid:
    def test_chain_graph(self):
        """Build chain graph: each cell neighbors the next."""
        cell_ids = np.array([0, 1, 2, 3, 4])

        def neighbor_fn(cid):
            neighbors = []
            if cid > 0:
                neighbors.append(cid - 1)
            if cid < 4:
                neighbors.append(cid + 1)
            return neighbors

        W = adjacency_from_grid(cell_ids, neighbor_fn)
        assert W.shape == (5, 5)
        # Check adjacency of cell 2: neighbors are 1 and 3
        assert W[2, 1] == 1.0
        assert W[2, 3] == 1.0
        assert W[2, 0] == 0.0

    def test_symmetry(self):
        """Adjacency matrix should be symmetric."""
        cell_ids = np.array([10, 20, 30])

        def neighbor_fn(cid):
            if cid == 10:
                return [20]
            elif cid == 20:
                return [10, 30]
            else:
                return [20]

        W = adjacency_from_grid(cell_ids, neighbor_fn)
        diff = W - W.T
        assert diff.nnz == 0 or np.allclose(diff.toarray(), 0)


class TestCARDenseAdjacency:
    def test_dense_adjacency_works(self):
        """car_smooth should accept dense numpy adjacency matrix."""
        n = 10
        values = np.ones(n)
        W_dense = np.zeros((n, n))
        for i in range(n - 1):
            W_dense[i, i + 1] = 1.0
            W_dense[i + 1, i] = 1.0

        result = car_smooth(values, W_dense, tau=1.0, rho=0.99)
        assert np.all(np.isfinite(result.smoothed))


class TestCARResultFrozen:
    def test_frozen(self):
        r = CARResult(
            smoothed=np.zeros(5),
            raw=np.zeros(5),
            residuals=np.zeros(5),
            spatial_variance=0.0,
            n_iterations=0,
            converged=True,
            effective_cells=5,
        )
        with pytest.raises(AttributeError):
            r.converged = False  # type: ignore[misc]
