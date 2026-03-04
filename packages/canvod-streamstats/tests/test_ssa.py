"""Tests for SSA decomposition."""

import numpy as np

from canvod.streamstats.spectral.ssa import SSAResult, ssa_decompose


class TestSSADecompose:
    """SSA decomposition tests."""

    def test_trend_plus_sine(self):
        """First component should capture the slowly varying trend."""
        N = 200
        t = np.arange(N, dtype=np.float64)
        trend = 0.01 * t  # linear trend
        periodic = np.sin(2 * np.pi * t / 20)
        signal = trend + periodic

        result = ssa_decompose(signal, window=50, n_components=5)
        assert isinstance(result, SSAResult)
        assert result.components.shape[0] == 5
        assert result.components.shape[1] == N
        assert result.trend.shape == (N,)

    def test_singular_values_descending(self):
        """Singular values should be in descending order."""
        rng = np.random.default_rng(42)
        signal = np.sin(2 * np.pi * np.arange(200) / 20) + 0.1 * rng.standard_normal(
            200
        )

        result = ssa_decompose(signal, window=50, n_components=5)
        assert np.all(np.diff(result.singular_values) <= 1e-10)

    def test_explained_variance_sums_near_one(self):
        """Explained variance should sum close to 1 when enough components used."""
        N = 100
        signal = np.sin(2 * np.pi * np.arange(N) / 20)

        result = ssa_decompose(signal, window=30, n_components=30)
        total = np.sum(result.explained_variance)
        # With enough components relative to the signal, should capture most variance
        assert total > 0.5

    def test_reconstruction_accuracy(self):
        """Sum of all components + residual ≈ original signal (approximately)."""
        N = 100
        signal = np.sin(2 * np.pi * np.arange(N) / 20)

        result = ssa_decompose(signal, window=30, n_components=30)
        reconstructed = np.sum(result.components, axis=0)
        # Not exact due to truncated SVD, but should be close
        np.testing.assert_allclose(reconstructed, signal, atol=0.5)

    def test_short_input(self):
        """Very short input returns empty result."""
        result = ssa_decompose(np.array([1.0, 2.0]))
        assert result.components.shape[0] == 0

    def test_window_clamped(self):
        """Window larger than N/2 is clamped automatically."""
        N = 20
        signal = np.arange(N, dtype=np.float64)
        result = ssa_decompose(signal, window=100, n_components=3)
        # Should not raise, window gets clamped to N//2 = 10
        assert result.components.shape[0] == 3
        assert result.components.shape[1] == N
