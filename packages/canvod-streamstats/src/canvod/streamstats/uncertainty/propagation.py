"""Per-observation uncertainty propagation through the GNSS-VOD processing chain.

Propagates thermal noise uncertainty from C/N₀ measurements through the
differencing, transmissivity, and VOD retrieval steps using the delta method.

All functions are pure — they compute from scalar or array inputs with no
internal state.
"""

from __future__ import annotations

import numpy as np

from canvod.streamstats._types import (
    DEFAULT_COHERENT_INTEGRATION_MS,
    DEFAULT_ELEVATION_SIGMA_RAD,
    DEFAULT_NONCOHERENT_AVERAGES,
)

_LN10_OVER_10 = np.log(10.0) / 10.0


# ── Scalar functions ─────────────────────────────────────────────────────


def sigma_cn0(
    cn0_db_hz: float,
    t_c_ms: float = DEFAULT_COHERENT_INTEGRATION_MS,
    m: int = DEFAULT_NONCOHERENT_AVERAGES,
) -> float:
    """Thermal C/N₀ uncertainty (dB-Hz).

    σ² ≈ 1/(M·T_c) · (1 + 1/(T_c · C/N₀_linear))²

    Parameters
    ----------
    cn0_db_hz : float
        Carrier-to-noise density ratio in dB-Hz.
    t_c_ms : float
        Coherent integration time in milliseconds.
    m : int
        Number of non-coherent averages.

    Returns
    -------
    float
        Standard deviation of the C/N₀ estimate in dB-Hz.
    """
    t_c_s = t_c_ms / 1000.0
    cn0_linear = 10.0 ** (cn0_db_hz / 10.0)
    # Floor at 1e-12 to prevent division by zero for blocked signals (-inf dB)
    cn0_linear = max(cn0_linear, 1e-12)
    variance = (1.0 / (m * t_c_s)) * (1.0 + 1.0 / (t_c_s * cn0_linear)) ** 2
    # Convert variance in linear power domain to dB via delta method:
    # σ_dB ≈ (10/ln10) · σ_linear / cn0_linear
    sigma_linear = np.sqrt(variance)
    return float((10.0 / np.log(10.0)) * sigma_linear / cn0_linear)


def sigma_delta_snr(sigma_canopy: float, sigma_ref: float) -> float:
    """ΔSNR uncertainty from independent receiver errors.

    σ²_ΔSNR = σ²_canopy + σ²_ref

    Parameters
    ----------
    sigma_canopy : float
        C/N₀ uncertainty of the canopy receiver (dB-Hz).
    sigma_ref : float
        C/N₀ uncertainty of the reference receiver (dB-Hz).

    Returns
    -------
    float
        Standard deviation of ΔSNR in dB.
    """
    return float(np.sqrt(sigma_canopy**2 + sigma_ref**2))


def sigma_transmissivity(t: float, sigma_delta_snr: float) -> float:
    """Transmissivity uncertainty via delta method.

    T = 10^(ΔSNR/10), so σ_T = (ln10/10) · T · σ_ΔSNR

    Parameters
    ----------
    t : float
        Transmissivity (0, 1].
    sigma_delta_snr : float
        Standard deviation of ΔSNR in dB.

    Returns
    -------
    float
        Standard deviation of the transmissivity estimate.
        Returns NaN if *t* ≤ 0.
    """
    if t <= 0.0:
        return float("nan")
    return float(_LN10_OVER_10 * t * sigma_delta_snr)


def sigma_vod(
    t: float,
    theta_rad: float,
    sigma_delta_snr: float,
    sigma_theta_rad: float = DEFAULT_ELEVATION_SIGMA_RAD,
) -> float:
    """Full VOD uncertainty from error propagation.

    VOD = −cos(θ) · ln(T), so by the delta method:

    σ²_VOD = cos²θ · (ln10/10)² · σ²_ΔSNR + ln²(T) · sin²θ · σ²_θ

    Parameters
    ----------
    t : float
        Transmissivity (0, 1].
    theta_rad : float
        Zenith angle in radians.
    sigma_delta_snr : float
        Standard deviation of ΔSNR in dB.
    sigma_theta_rad : float
        Standard deviation of the elevation/zenith angle in radians.

    Returns
    -------
    float
        Standard deviation of the VOD estimate.
        Returns NaN if *t* ≤ 0.
    """
    if t <= 0.0:
        return float("nan")
    cos_th = np.cos(theta_rad)
    sin_th = np.sin(theta_rad)
    ln_t = np.log(t)
    term_snr = (cos_th * _LN10_OVER_10 * sigma_delta_snr) ** 2
    term_angle = (ln_t * sin_th * sigma_theta_rad) ** 2
    return float(np.sqrt(term_snr + term_angle))


# ── Batch (vectorised) variants ──────────────────────────────────────────


def sigma_cn0_batch(
    cn0_db_hz: np.ndarray,
    t_c_ms: float = DEFAULT_COHERENT_INTEGRATION_MS,
    m: int = DEFAULT_NONCOHERENT_AVERAGES,
) -> np.ndarray:
    """Vectorised :func:`sigma_cn0`."""
    cn0_db_hz = np.asarray(cn0_db_hz, dtype=np.float64)
    t_c_s = t_c_ms / 1000.0
    cn0_linear = 10.0 ** (cn0_db_hz / 10.0)
    # Floor at 1e-12 to prevent division by zero for blocked signals (-inf dB)
    cn0_linear = np.maximum(cn0_linear, 1e-12)
    variance = (1.0 / (m * t_c_s)) * (1.0 + 1.0 / (t_c_s * cn0_linear)) ** 2
    sigma_linear = np.sqrt(variance)
    return (10.0 / np.log(10.0)) * sigma_linear / cn0_linear


def sigma_delta_snr_batch(
    sigma_canopy: np.ndarray,
    sigma_ref: np.ndarray,
) -> np.ndarray:
    """Vectorised :func:`sigma_delta_snr`."""
    sigma_canopy = np.asarray(sigma_canopy, dtype=np.float64)
    sigma_ref = np.asarray(sigma_ref, dtype=np.float64)
    return np.sqrt(sigma_canopy**2 + sigma_ref**2)


def sigma_transmissivity_batch(
    t: np.ndarray,
    sigma_delta_snr: np.ndarray,
) -> np.ndarray:
    """Vectorised :func:`sigma_transmissivity`.

    Returns NaN where *t* ≤ 0.
    """
    t = np.asarray(t, dtype=np.float64)
    sigma_delta_snr = np.asarray(sigma_delta_snr, dtype=np.float64)
    result = _LN10_OVER_10 * t * sigma_delta_snr
    result = np.where(t > 0.0, result, np.nan)
    return result


def sigma_vod_batch(
    t: np.ndarray,
    theta_rad: np.ndarray,
    sigma_delta_snr: np.ndarray,
    sigma_theta_rad: float | np.ndarray = DEFAULT_ELEVATION_SIGMA_RAD,
) -> np.ndarray:
    """Vectorised :func:`sigma_vod`.

    Returns NaN where *t* ≤ 0.
    """
    t = np.asarray(t, dtype=np.float64)
    theta_rad = np.asarray(theta_rad, dtype=np.float64)
    sigma_delta_snr = np.asarray(sigma_delta_snr, dtype=np.float64)
    sigma_theta_rad = np.asarray(sigma_theta_rad, dtype=np.float64)

    cos_th = np.cos(theta_rad)
    sin_th = np.sin(theta_rad)
    ln_t = np.where(t > 0.0, np.log(t), np.nan)

    term_snr = (cos_th * _LN10_OVER_10 * sigma_delta_snr) ** 2
    term_angle = (ln_t * sin_th * sigma_theta_rad) ** 2
    result = np.sqrt(term_snr + term_angle)
    return np.where(t > 0.0, result, np.nan)
