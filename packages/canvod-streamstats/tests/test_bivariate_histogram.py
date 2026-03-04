"""Tests for BivariateHistogram."""

import numpy as np
import pytest

from canvod.streamstats.accumulators.bivariate_histogram import BivariateHistogram


class TestBivariateHistogram:
    def test_single_point(self):
        h = BivariateHistogram(0.0, 10.0, 0.0, 10.0, n_bins_x=10, n_bins_y=10)
        h.update(5.0, 5.0)
        assert h.total == 1
        assert h.counts.sum() == 1

    def test_uniform_random(self):
        rng = np.random.default_rng(42)
        h = BivariateHistogram(0.0, 1.0, 0.0, 1.0, n_bins_x=10, n_bins_y=10)
        x = rng.uniform(0.0, 1.0, 10000)
        y = rng.uniform(0.0, 1.0, 10000)
        h.update_batch(x, y)
        assert h.total == 10000
        # Each bin should have ~100 counts for 10x10
        assert h.counts.min() > 50

    def test_marginals_match_1d(self):
        rng = np.random.default_rng(42)
        h = BivariateHistogram(0.0, 10.0, 0.0, 10.0, n_bins_x=5, n_bins_y=5)
        x = rng.uniform(0.0, 10.0, 1000)
        y = rng.uniform(0.0, 10.0, 1000)
        h.update_batch(x, y)

        # Marginals should sum to total
        assert h.marginal_x.sum() == h.total
        assert h.marginal_y.sum() == h.total

    def test_merge(self):
        h1 = BivariateHistogram(0.0, 10.0, 0.0, 10.0, n_bins_x=5, n_bins_y=5)
        h2 = BivariateHistogram(0.0, 10.0, 0.0, 10.0, n_bins_x=5, n_bins_y=5)
        h1.update(1.0, 1.0)
        h2.update(2.0, 2.0)
        h1.merge(h2)
        assert h1.total == 2

    def test_merge_incompatible(self):
        h1 = BivariateHistogram(0.0, 10.0, 0.0, 10.0, n_bins_x=5, n_bins_y=5)
        h2 = BivariateHistogram(0.0, 20.0, 0.0, 10.0, n_bins_x=5, n_bins_y=5)
        with pytest.raises(ValueError, match="different bin specs"):
            h1.merge(h2)

    def test_serialization_roundtrip(self):
        rng = np.random.default_rng(42)
        h = BivariateHistogram(0.0, 10.0, 0.0, 10.0, n_bins_x=5, n_bins_y=5)
        x = rng.uniform(0.0, 10.0, 200)
        y = rng.uniform(0.0, 10.0, 200)
        h.update_batch(x, y)

        arr = h.to_array()
        h2 = BivariateHistogram.from_array(arr)

        assert h2.total == h.total
        np.testing.assert_array_equal(h2.counts, h.counts)

    def test_out_of_range(self):
        h = BivariateHistogram(0.0, 10.0, 0.0, 10.0, n_bins_x=5, n_bins_y=5)
        h.update(-1.0, 5.0)  # x out of range
        h.update(5.0, -1.0)  # y out of range
        h.update(15.0, 5.0)  # x out of range
        assert h.total == 0

    def test_nan_handling(self):
        h = BivariateHistogram(0.0, 10.0, 0.0, 10.0, n_bins_x=5, n_bins_y=5)
        h.update(float("nan"), 5.0)
        h.update(5.0, float("nan"))
        h.update_batch(np.array([1.0, float("nan")]), np.array([float("nan"), 2.0]))
        assert h.total == 0

    def test_joint_probabilities(self):
        h = BivariateHistogram(0.0, 10.0, 0.0, 10.0, n_bins_x=5, n_bins_y=5)
        h.update(1.0, 1.0)
        h.update(3.0, 3.0)
        probs = h.joint_probabilities
        assert probs.sum() == pytest.approx(1.0, abs=1e-10)
