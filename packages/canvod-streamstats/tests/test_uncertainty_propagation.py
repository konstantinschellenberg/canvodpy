"""Tests for uncertainty propagation functions."""

from __future__ import annotations

import numpy as np
import pytest

from canvod.streamstats.uncertainty.propagation import (
    sigma_cn0,
    sigma_cn0_batch,
    sigma_delta_snr,
    sigma_delta_snr_batch,
    sigma_transmissivity,
    sigma_transmissivity_batch,
    sigma_vod,
    sigma_vod_batch,
)


class TestSigmaCN0:
    """Tests for C/N₀ uncertainty."""

    def test_known_value(self) -> None:
        """σ_cn0 at 40 dB-Hz with default params gives a small positive value."""
        result = sigma_cn0(40.0)
        assert result > 0.0
        # At 40 dB-Hz with T_c=20ms, M=50, uncertainty should be sub-dB
        assert result < 1.0

    def test_low_cn0_larger_uncertainty(self) -> None:
        """Lower C/N₀ should produce larger uncertainty."""
        sigma_high = sigma_cn0(45.0)
        sigma_low = sigma_cn0(25.0)
        assert sigma_low > sigma_high

    def test_more_averages_reduces_uncertainty(self) -> None:
        """Increasing M should reduce uncertainty."""
        sigma_m50 = sigma_cn0(40.0, m=50)
        sigma_m200 = sigma_cn0(40.0, m=200)
        assert sigma_m200 < sigma_m50


class TestSigmaDeltaSNR:
    """Tests for ΔSNR uncertainty."""

    def test_pythagorean_equal_sigmas(self) -> None:
        """Two equal σ should combine as σ√2."""
        result = sigma_delta_snr(1.0, 1.0)
        assert result == pytest.approx(np.sqrt(2.0))

    def test_zero_reference(self) -> None:
        """If reference has zero uncertainty, result equals canopy uncertainty."""
        result = sigma_delta_snr(0.5, 0.0)
        assert result == pytest.approx(0.5)


class TestSigmaTransmissivity:
    """Tests for transmissivity uncertainty."""

    def test_known_value(self) -> None:
        """σ_T = (ln10/10) · T · σ_ΔSNR for T=0.5, σ=1.0."""
        result = sigma_transmissivity(0.5, 1.0)
        expected = (np.log(10.0) / 10.0) * 0.5 * 1.0
        assert result == pytest.approx(expected)

    def test_no_attenuation(self) -> None:
        """At T=1 (no attenuation), uncertainty is (ln10/10)·σ_ΔSNR."""
        result = sigma_transmissivity(1.0, 1.0)
        expected = np.log(10.0) / 10.0
        assert result == pytest.approx(expected)

    def test_t_zero_returns_nan(self) -> None:
        """T ≤ 0 is unphysical → NaN."""
        assert np.isnan(sigma_transmissivity(0.0, 1.0))
        assert np.isnan(sigma_transmissivity(-0.1, 1.0))


class TestSigmaVOD:
    """Tests for full VOD uncertainty."""

    def test_known_value_45deg(self) -> None:
        """σ_VOD at θ=45°, T=0.5, σ_ΔSNR=1.0 with default σ_θ."""
        theta = np.pi / 4.0
        result = sigma_vod(0.5, theta, 1.0)
        assert result > 0.0
        # Cross-check: SNR term dominates since σ_θ is tiny
        cos_th = np.cos(theta)
        snr_term = (cos_th * np.log(10.0) / 10.0 * 1.0) ** 2
        assert result == pytest.approx(np.sqrt(snr_term), abs=1e-4)

    def test_negligible_theta_uncertainty(self) -> None:
        """With σ_θ≈0, only the SNR term matters."""
        theta = np.pi / 4.0
        result = sigma_vod(0.5, theta, 1.0, sigma_theta_rad=0.0)
        cos_th = np.cos(theta)
        expected = abs(cos_th * np.log(10.0) / 10.0 * 1.0)
        assert result == pytest.approx(expected)

    def test_t_zero_returns_nan(self) -> None:
        """T ≤ 0 → NaN."""
        assert np.isnan(sigma_vod(0.0, 0.5, 1.0))


class TestBatchConsistency:
    """Batch variants must agree with scalar functions."""

    def test_sigma_cn0_batch(self) -> None:
        cn0_values = np.array([25.0, 35.0, 45.0])
        batch = sigma_cn0_batch(cn0_values)
        scalar = np.array([sigma_cn0(v) for v in cn0_values])
        np.testing.assert_allclose(batch, scalar)

    def test_sigma_delta_snr_batch(self) -> None:
        sc = np.array([0.5, 1.0, 1.5])
        sr = np.array([0.3, 0.7, 1.0])
        batch = sigma_delta_snr_batch(sc, sr)
        scalar = np.array([sigma_delta_snr(a, b) for a, b in zip(sc, sr)])
        np.testing.assert_allclose(batch, scalar)

    def test_sigma_transmissivity_batch(self) -> None:
        t = np.array([0.3, 0.5, 0.8, -0.1])
        sd = np.array([1.0, 1.0, 1.0, 1.0])
        batch = sigma_transmissivity_batch(t, sd)
        scalar = np.array([sigma_transmissivity(a, b) for a, b in zip(t, sd)])
        np.testing.assert_allclose(batch, scalar)

    def test_sigma_vod_batch(self) -> None:
        t = np.array([0.3, 0.5, 0.8])
        theta = np.array([0.3, 0.5, 0.8])
        sd = np.array([1.0, 1.0, 1.0])
        batch = sigma_vod_batch(t, theta, sd)
        scalar = np.array([sigma_vod(a, b, c) for a, b, c in zip(t, theta, sd)])
        np.testing.assert_allclose(batch, scalar)
