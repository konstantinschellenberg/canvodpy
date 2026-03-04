"""Tests for effective sample size and aggregation uncertainty."""

from __future__ import annotations

import numpy as np
import pytest

from canvod.streamstats.uncertainty.aggregation import (
    aggregation_uncertainty,
    effective_sample_size,
    effective_sample_size_from_autocovariance,
)


class TestEffectiveSampleSize:
    """Tests for n_eff computation."""

    def test_zero_autocorrelation(self) -> None:
        """Zero autocorrelation → n_eff = n (independent samples)."""
        rho = np.array([0.0, 0.0, 0.0])
        assert effective_sample_size(rho, 100) == pytest.approx(100.0)

    def test_positive_autocorrelation_reduces_n_eff(self) -> None:
        """Constant positive ρ → n_eff < n."""
        rho = np.array([0.5, 0.5, 0.5])
        n_eff = effective_sample_size(rho, 100)
        # denominator = 1 + 2*(0.5+0.5+0.5) = 4.0 → n_eff = 25
        assert n_eff == pytest.approx(25.0)
        assert n_eff < 100.0

    def test_truncates_at_first_negative(self) -> None:
        """Sum stops at first negative autocorrelation."""
        rho = np.array([0.5, 0.3, -0.1, 0.2])
        n_eff = effective_sample_size(rho, 100)
        # Only uses ρ(1)=0.5, ρ(2)=0.3 → denom = 1 + 2*(0.5+0.3) = 2.6
        expected = 100.0 / 2.6
        assert n_eff == pytest.approx(expected)

    def test_empty_autocorrelations(self) -> None:
        """Empty autocorrelation array → n_eff = n."""
        assert effective_sample_size(np.array([]), 50) == pytest.approx(50.0)

    def test_minimum_is_one(self) -> None:
        """n_eff is always ≥ 1."""
        rho = np.array([0.99, 0.98, 0.97, 0.96])
        n_eff = effective_sample_size(rho, 5)
        assert n_eff >= 1.0


class TestAggregationUncertainty:
    """Tests for inverse-variance weighted aggregation."""

    def test_equal_sigma_independent(self) -> None:
        """Equal σ, independent obs → σ_agg = σ/√n."""
        sigma_obs = np.full(100, 2.0)
        result = aggregation_uncertainty(sigma_obs)
        expected = 2.0 / np.sqrt(100.0)
        assert result == pytest.approx(expected)

    def test_n_eff_correction(self) -> None:
        """n_eff < n increases aggregation uncertainty."""
        sigma_obs = np.full(100, 2.0)
        indep = aggregation_uncertainty(sigma_obs, n_eff=100.0)
        correlated = aggregation_uncertainty(sigma_obs, n_eff=25.0)
        assert correlated > indep
        # Ratio should be √(100/25) = 2
        assert correlated / indep == pytest.approx(2.0)

    def test_empty_returns_nan(self) -> None:
        """No valid observations → NaN."""
        assert np.isnan(aggregation_uncertainty(np.array([])))

    def test_zero_sigma_filtered(self) -> None:
        """Observations with σ=0 are filtered out."""
        sigma_obs = np.array([1.0, 0.0, 1.0])
        result = aggregation_uncertainty(sigma_obs, n=2)
        # Two valid obs with σ=1 → σ_agg = 1/√(2) · √(2/2) = 1/√2
        expected = 1.0 / np.sqrt(2.0)
        assert result == pytest.approx(expected)


class TestEffectiveSampleSizeFromAutocovariance:
    """Tests for autocovariance → n_eff convenience function."""

    def test_normalisation(self) -> None:
        """Autocovariance [4, 2, 1] → ρ = [0.5, 0.25] with n=3."""
        acov = np.array([4.0, 2.0, 1.0])
        n_eff = effective_sample_size_from_autocovariance(acov)
        # ρ = [0.5, 0.25], denom = 1+2*(0.5+0.25) = 2.5, n_eff = 3/2.5
        expected = 3.0 / 2.5
        assert n_eff == pytest.approx(expected)

    def test_single_element(self) -> None:
        """Single autocovariance → n_eff = 1."""
        assert effective_sample_size_from_autocovariance(np.array([5.0])) == 1.0

    def test_zero_variance(self) -> None:
        """Zero variance → n_eff = 1."""
        assert effective_sample_size_from_autocovariance(np.array([0.0, 0.0])) == 1.0
