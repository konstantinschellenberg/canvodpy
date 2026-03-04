"""Multitaper power spectral density estimation using DPSS tapers."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from scipy.signal.windows import dpss

from canvod.streamstats._types import DEFAULT_MULTITAPER_K, DEFAULT_MULTITAPER_NW


@dataclass(frozen=True)
class MultitaperResult:
    """Result of multitaper PSD estimation."""

    frequencies: np.ndarray  # (N_freq,) frequency axis in Hz
    psd: np.ndarray  # (N_freq,) averaged PSD estimate
    noise_type: str  # "white" / "flicker" / "random_walk" / "unknown"
    spectral_index: float  # power-law exponent from log-log fit
    f_test_lines: np.ndarray  # frequencies of significant harmonic lines


def multitaper_psd(
    values: np.ndarray,
    sample_rate: float = 1.0,
    NW: float = DEFAULT_MULTITAPER_NW,
    K: int = DEFAULT_MULTITAPER_K,
) -> MultitaperResult:
    """Compute multitaper PSD estimate.

    Parameters
    ----------
    values : array
        Evenly sampled time series.
    sample_rate : float
        Sampling rate in Hz (default 1.0).
    NW : float
        Time-bandwidth product (default 4.0).
    K : int
        Number of DPSS tapers (default 7, should be <= 2*NW - 1).

    Returns
    -------
    MultitaperResult
    """
    x = np.asarray(values, dtype=np.float64).ravel()

    # Filter NaN — replace with zero (standard practice for spectral estimation)
    nan_mask = np.isnan(x)
    if nan_mask.any():
        x = x.copy()
        x[nan_mask] = 0.0

    N = len(x)
    if N < 4:
        empty = np.array([], dtype=np.float64)
        return MultitaperResult(
            frequencies=empty,
            psd=empty,
            noise_type="unknown",
            spectral_index=float("nan"),
            f_test_lines=empty,
        )

    # Compute DPSS tapers
    K = min(K, N - 1)  # guard against K > N-1
    tapers = dpss(N, NW, K)  # shape (K, N)

    # Compute tapered FFTs — vectorised as matrix operation
    # Each row of tapers multiplied by x, then FFT
    tapered = tapers * x[np.newaxis, :]  # (K, N)
    fft_result = np.fft.rfft(tapered, axis=1)  # (K, N//2+1)

    # Power spectra per taper
    power_per_taper = np.abs(fft_result) ** 2  # (K, N//2+1)

    # Average across tapers
    psd = np.mean(power_per_taper, axis=0)  # (N//2+1,)

    # Normalise: PSD = power / (N * sample_rate)
    psd /= N * sample_rate

    # One-sided PSD: multiply by 2 for positive frequencies (excluding DC and Nyquist)
    psd_dc = psd[0]
    psd *= 2
    psd[0] = psd_dc
    if N % 2 == 0:
        psd[-1] /= 2  # Nyquist bin: undo the doubling

    # Frequency axis
    freqs = np.fft.rfftfreq(N, d=1.0 / sample_rate)  # (N//2+1,)

    # Noise characterisation via log-log linear fit (skip DC)
    spectral_index, noise_type = _classify_noise(freqs, psd)

    # F-test for harmonic line detection
    f_test_lines = _f_test_harmonics(freqs, power_per_taper, K)

    return MultitaperResult(
        frequencies=freqs,
        psd=psd,
        noise_type=noise_type,
        spectral_index=spectral_index,
        f_test_lines=f_test_lines,
    )


def _classify_noise(freqs: np.ndarray, psd: np.ndarray) -> tuple[float, str]:
    """Fit spectral index from log-log PSD and classify noise type."""
    # Skip DC component and any zeros
    mask = (freqs > 0) & (psd > 0)
    if np.sum(mask) < 2:
        return float("nan"), "unknown"

    log_f = np.log10(freqs[mask])
    log_psd = np.log10(psd[mask])

    # Linear fit: log(PSD) = slope * log(f) + intercept
    coeffs = np.polyfit(log_f, log_psd, 1)
    slope = float(coeffs[0])

    # Classify
    if abs(slope) < 0.5:
        noise_type = "white"
    elif -1.5 < slope <= -0.5:
        noise_type = "flicker"
    elif slope <= -1.5:
        noise_type = "random_walk"
    else:
        noise_type = "unknown"

    return slope, noise_type


def _f_test_harmonics(
    freqs: np.ndarray,
    power_per_taper: np.ndarray,
    K: int,
    alpha: float = 0.01,
) -> np.ndarray:
    """Detect harmonic lines via Thomson's (1982) F-test.

    Compares per-frequency coherence across tapers.  At a harmonic line all
    tapers see the same sinusoidal component, so their power estimates are
    tightly clustered (low variance-to-mean ratio).  At noise frequencies
    the ratio is higher.

    As a practical approximation we flag frequencies whose averaged PSD
    exceeds ``threshold_factor`` times the median PSD (skip DC).

    Returns array of frequencies where the test rejects the null.
    """
    if K < 2:
        return np.array([], dtype=np.float64)

    mean_power = np.mean(power_per_taper, axis=0)  # (N_freq,)

    # Use median of positive-frequency PSD as the noise floor
    pos = freqs > 0
    if not np.any(pos) or not np.any(mean_power[pos] > 0):
        return np.array([], dtype=np.float64)

    median_power = np.median(mean_power[pos])
    if median_power <= 0:
        return np.array([], dtype=np.float64)

    ratio = mean_power / median_power

    # Threshold: a line should stand well above the noise floor.
    # Use a conservative multiplier that scales mildly with K.
    threshold = 10.0 * K

    significant = (ratio > threshold) & pos
    return freqs[significant]
