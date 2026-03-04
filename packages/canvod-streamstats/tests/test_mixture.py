"""Tests for Gaussian mixture model fitting from histogram summaries."""

import numpy as np
import pytest

from canvod.streamstats.accumulators.histogram import StreamingHistogram
from canvod.streamstats.bayesian.mixture import (
    GaussianMixtureResult,
    fit_gaussian_mixture,
    fit_gaussian_mixture_from_histogram,
)


def _make_histogram_from_samples(samples, low, high, n_bins):
    """Helper: build a StreamingHistogram from sample data."""
    h = StreamingHistogram(low, high, n_bins)
    h.update_batch(samples)
    return h


class TestSingleGaussian:
    def test_recovers_mean(self):
        """Single Gaussian: recovers true mean from histogram."""
        rng = np.random.default_rng(42)
        samples = rng.normal(5.0, 1.0, 10000)
        h = _make_histogram_from_samples(samples, 0.0, 10.0, 200)
        result = fit_gaussian_mixture(h.bin_edges, h.counts, n_components=1)

        assert result.n_samples == h.total
        assert abs(result.means[0] - 5.0) < 0.2

    def test_recovers_std(self):
        """Single Gaussian: recovers true std from histogram."""
        rng = np.random.default_rng(42)
        samples = rng.normal(5.0, 1.0, 10000)
        h = _make_histogram_from_samples(samples, 0.0, 10.0, 200)
        result = fit_gaussian_mixture(h.bin_edges, h.counts, n_components=1)

        assert abs(result.stds[0] - 1.0) < 0.2


class TestBimodal:
    def test_recovers_both_components(self):
        """Bimodal (0.5·N(0,1) + 0.5·N(10,1)): recovers both means."""
        rng = np.random.default_rng(123)
        s1 = rng.normal(0.0, 1.0, 5000)
        s2 = rng.normal(10.0, 1.0, 5000)
        samples = np.concatenate([s1, s2])
        h = _make_histogram_from_samples(samples, -5.0, 15.0, 400)
        result = fit_gaussian_mixture(h.bin_edges, h.counts, n_components=2)

        # Sorted by mean
        assert abs(result.means[0] - 0.0) < 0.5
        assert abs(result.means[1] - 10.0) < 0.5


class TestUnequalWeights:
    def test_weight_recovery(self):
        """Unequal weights (0.3/0.7): recovers approximate weights."""
        rng = np.random.default_rng(456)
        s1 = rng.normal(0.0, 1.0, 3000)
        s2 = rng.normal(8.0, 1.0, 7000)
        samples = np.concatenate([s1, s2])
        h = _make_histogram_from_samples(samples, -5.0, 13.0, 360)
        result = fit_gaussian_mixture(h.bin_edges, h.counts, n_components=2)

        # Sorted by mean, first component should be ~0.3
        assert abs(result.weights[0] - 0.3) < 0.1
        assert abs(result.weights[1] - 0.7) < 0.1


class TestConvergence:
    def test_converges_for_simple_case(self):
        """Simple unimodal should converge within default iterations."""
        rng = np.random.default_rng(789)
        samples = rng.normal(0.0, 1.0, 5000)
        h = _make_histogram_from_samples(samples, -5.0, 5.0, 200)
        result = fit_gaussian_mixture(h.bin_edges, h.counts, n_components=1)
        assert result.converged is True

    def test_max_iter_1_not_converged(self):
        """max_iter=1 should not converge for bimodal data."""
        rng = np.random.default_rng(101)
        s1 = rng.normal(0.0, 1.0, 3000)
        s2 = rng.normal(10.0, 1.0, 3000)
        samples = np.concatenate([s1, s2])
        h = _make_histogram_from_samples(samples, -5.0, 15.0, 400)
        result = fit_gaussian_mixture(h.bin_edges, h.counts, n_components=2, max_iter=1)
        assert result.converged is False
        assert result.n_iterations == 1


class TestEmptyHistogram:
    def test_empty_counts(self):
        """Empty histogram → NaN results."""
        edges = np.linspace(0, 10, 11)
        counts = np.zeros(10)
        result = fit_gaussian_mixture(edges, counts, n_components=2)
        assert np.all(np.isnan(result.means))
        assert np.all(np.isnan(result.stds))
        assert np.all(np.isnan(result.weights))
        assert np.isnan(result.log_likelihood)
        assert result.n_samples == 0

    def test_zero_length(self):
        """Zero-length counts array."""
        edges = np.array([0.0])
        counts = np.array([], dtype=np.float64)
        result = fit_gaussian_mixture(edges, counts)
        assert np.isnan(result.bic)


class TestBIC:
    def test_bic_favors_correct_k(self):
        """BIC should favor K=1 over K=3 for unimodal data."""
        rng = np.random.default_rng(202)
        samples = rng.normal(5.0, 1.0, 10000)
        h = _make_histogram_from_samples(samples, 0.0, 10.0, 200)

        result_k1 = fit_gaussian_mixture(h.bin_edges, h.counts, n_components=1)
        result_k3 = fit_gaussian_mixture(h.bin_edges, h.counts, n_components=3)

        # Lower BIC is better; K=1 should be favored for unimodal data
        assert result_k1.bic < result_k3.bic


class TestFromHistogram:
    def test_matches_raw_call(self):
        """fit_gaussian_mixture_from_histogram matches direct call."""
        rng = np.random.default_rng(303)
        samples = rng.normal(5.0, 1.0, 5000)
        h = _make_histogram_from_samples(samples, 0.0, 10.0, 200)

        r1 = fit_gaussian_mixture(h.bin_edges, h.counts, n_components=2)
        r2 = fit_gaussian_mixture_from_histogram(h, n_components=2)

        np.testing.assert_array_equal(r1.means, r2.means)
        np.testing.assert_array_equal(r1.stds, r2.stds)
        np.testing.assert_array_equal(r1.weights, r2.weights)
        assert r1.log_likelihood == r2.log_likelihood


class TestWeightsAndStds:
    def test_weights_sum_to_one(self):
        rng = np.random.default_rng(404)
        samples = rng.normal(0.0, 1.0, 5000)
        h = _make_histogram_from_samples(samples, -5.0, 5.0, 200)
        result = fit_gaussian_mixture(h.bin_edges, h.counts, n_components=3)
        np.testing.assert_allclose(result.weights.sum(), 1.0, atol=1e-10)

    def test_stds_positive(self):
        rng = np.random.default_rng(505)
        samples = rng.normal(0.0, 1.0, 5000)
        h = _make_histogram_from_samples(samples, -5.0, 5.0, 200)
        result = fit_gaussian_mixture(h.bin_edges, h.counts, n_components=3)
        assert np.all(result.stds > 0)


class TestGaussianMixtureResultFrozen:
    def test_frozen(self):
        r = GaussianMixtureResult(
            weights=np.array([1.0]),
            means=np.array([0.0]),
            stds=np.array([1.0]),
            log_likelihood=0.0,
            n_iterations=1,
            converged=True,
            bic=0.0,
            n_samples=100,
        )
        with pytest.raises(AttributeError):
            r.converged = False  # type: ignore[misc]
