"""Tests for transfer entropy."""

import numpy as np

from canvod.streamstats.information.transfer_entropy import transfer_entropy


class TestTransferEntropy:
    def test_independent_signals(self):
        rng = np.random.default_rng(42)
        x = rng.standard_normal(5000)
        y = rng.standard_normal(5000)
        result = transfer_entropy(x, y, lag=1, n_bins=6)
        # Independent signals → TE ≈ 0 (few bins to avoid histogram bias)
        assert result.transfer_entropy < 0.15
        assert result.reverse_te < 0.15

    def test_causal_coupling(self):
        rng = np.random.default_rng(42)
        n = 5000
        x = rng.standard_normal(n)
        y = np.zeros(n)
        # y[t] = 0.8 * x[t-1] + noise
        y[1:] = 0.8 * x[:-1] + 0.2 * rng.standard_normal(n - 1)
        result = transfer_entropy(x, y, lag=1, n_bins=20)
        # X drives Y → T_{X→Y} > T_{Y→X}
        assert result.transfer_entropy > result.reverse_te
        assert result.net_transfer > 0

    def test_symmetric_coupling(self):
        rng = np.random.default_rng(42)
        n = 5000
        x = np.zeros(n)
        y = np.zeros(n)
        x[0] = rng.standard_normal()
        y[0] = rng.standard_normal()
        for t in range(1, n):
            x[t] = 0.5 * y[t - 1] + rng.standard_normal() * 0.5
            y[t] = 0.5 * x[t - 1] + rng.standard_normal() * 0.5
        result = transfer_entropy(x, y, lag=1, n_bins=20)
        # Symmetric → net_transfer ≈ 0
        assert abs(result.net_transfer) < 0.2

    def test_short_input(self):
        result = transfer_entropy(np.array([1.0, 2.0]), np.array([3.0, 4.0]))
        assert np.isnan(result.transfer_entropy)

    def test_nan_handling(self):
        rng = np.random.default_rng(42)
        x = rng.standard_normal(100)
        y = rng.standard_normal(100)
        x[10] = float("nan")
        y[20] = float("nan")
        result = transfer_entropy(x, y)
        assert result.n_samples < 100
