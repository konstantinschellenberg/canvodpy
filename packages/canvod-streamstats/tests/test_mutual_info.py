"""Tests for mutual information."""

import numpy as np
import pytest

from canvod.streamstats.accumulators.bivariate_histogram import BivariateHistogram
from canvod.streamstats.information.mutual_info import (
    mutual_information,
    mutual_information_from_histogram,
)


class TestMutualInformation:
    def test_independent_signals(self):
        rng = np.random.default_rng(42)
        x = rng.standard_normal(10000)
        y = rng.standard_normal(10000)
        result = mutual_information(x, y, n_bins=30)
        # Independent → MI ≈ 0
        assert result.mutual_information < 0.1

    def test_identical_signals(self):
        rng = np.random.default_rng(42)
        x = rng.standard_normal(10000)
        result = mutual_information(x, x, n_bins=30)
        # MI(X;X) = H(X)
        assert result.mutual_information == pytest.approx(result.entropy_x, abs=0.05)

    def test_linear_relationship(self):
        rng = np.random.default_rng(42)
        x = rng.standard_normal(10000)
        y = 2.0 * x + rng.standard_normal(10000) * 0.1
        result = mutual_information(x, y, n_bins=30)
        assert result.mutual_information > 1.0

    def test_from_histogram(self):
        rng = np.random.default_rng(42)
        bh = BivariateHistogram(-5.0, 5.0, -5.0, 5.0, n_bins_x=20, n_bins_y=20)
        x = rng.standard_normal(5000)
        y = rng.standard_normal(5000)
        bh.update_batch(x, y)
        result = mutual_information_from_histogram(bh)
        assert result.mutual_information >= 0

    def test_normalized_mi_range(self):
        rng = np.random.default_rng(42)
        x = rng.standard_normal(5000)
        y = x + rng.standard_normal(5000) * 0.5
        result = mutual_information(x, y, n_bins=30)
        assert 0.0 <= result.normalized_mi <= 1.0 + 1e-10
