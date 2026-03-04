"""S4 scintillation index accumulator and amplitude distribution helpers."""

from __future__ import annotations

import math

import numpy as np

from canvod.streamstats.accumulators.welford import WelfordAccumulator


class S4Accumulator:
    """Streaming S4 amplitude scintillation index.

    Internally converts SNR (dB) to linear intensity I = 10^(SNR/10) and
    tracks moments via WelfordAccumulator on intensity.

    S4 = sqrt(var(I) / mean(I)²)

    State: 8 float64 (same as Welford on intensity).
    """

    __slots__ = ("_welford",)

    def __init__(self) -> None:
        self._welford = WelfordAccumulator()

    def update(self, snr_db: float) -> None:
        """Incorporate a single SNR observation in dB."""
        if math.isnan(snr_db):
            self._welford.update(float("nan"))
            return
        intensity = 10.0 ** (snr_db / 10.0)
        self._welford.update(intensity)

    def update_batch(self, snr_db_array: np.ndarray) -> None:
        """Incorporate an array of SNR observations in dB."""
        arr = np.asarray(snr_db_array, dtype=np.float64).ravel()
        nan_mask = np.isnan(arr)
        intensity = np.empty_like(arr)
        intensity[nan_mask] = np.nan
        intensity[~nan_mask] = 10.0 ** (arr[~nan_mask] / 10.0)
        self._welford.update_batch(intensity)

    def merge(self, other: S4Accumulator) -> S4Accumulator:
        """Merge another S4Accumulator into this one. Returns self."""
        self._welford.merge(other._welford)
        return self

    # --- Properties ---

    @property
    def count(self) -> int:
        return self._welford.count

    @property
    def n_nan(self) -> int:
        return self._welford.n_nan

    @property
    def mean_intensity(self) -> float:
        return self._welford.mean

    @property
    def variance_intensity(self) -> float:
        return self._welford.variance

    @property
    def s4(self) -> float:
        """Amplitude scintillation index S4 = sqrt(var(I) / mean(I)²)."""
        n = self._welford.count
        if n < 2:
            return float("nan")
        mean = self._welford.mean
        if mean == 0.0:
            return float("nan")
        var = self._welford.variance
        return math.sqrt(var / (mean * mean))

    @property
    def nakagami_m(self) -> float:
        """Nakagami-m shape parameter: m = 1 / S4²."""
        s4 = self.s4
        if math.isnan(s4) or s4 <= 0.0:
            return float("nan")
        return 1.0 / (s4 * s4)

    @property
    def scintillation_regime(self) -> str:
        """Classify scintillation: 'weak' (S4<0.3), 'moderate' (0.3-0.6), 'strong' (>0.6)."""
        s4 = self.s4
        if math.isnan(s4):
            return "unknown"
        if s4 < 0.3:
            return "weak"
        if s4 <= 0.6:
            return "moderate"
        return "strong"

    # --- Serialization ---

    def to_array(self) -> np.ndarray:
        """Return state as a float64 array of shape (8,)."""
        return self._welford.to_array()

    @classmethod
    def from_array(cls, arr: np.ndarray) -> S4Accumulator:
        """Restore from a state array."""
        obj = cls.__new__(cls)
        obj._welford = WelfordAccumulator.from_array(arr)
        return obj


# ---------------------------------------------------------------------------
# Standalone helper functions
# ---------------------------------------------------------------------------


def sigma_phi(variance: float) -> float:
    """Phase scintillation index σ_φ = sqrt(variance) of detrended carrier phase.

    The caller is responsible for detrending and computing variance via
    WelfordAccumulator on the detrended phase series.
    """
    if math.isnan(variance) or variance < 0.0:
        return float("nan")
    return math.sqrt(variance)


def nakagami_m_from_s4(s4: float) -> float:
    """Nakagami-m shape parameter: m = 1 / S4²."""
    if s4 <= 0.0 or math.isnan(s4):
        return float("nan")
    return 1.0 / (s4 * s4)


def lognormal_params(welford: WelfordAccumulator) -> tuple[float, float]:
    """Log-normal μ_ln, σ_ln from a Welford accumulator on log-intensity."""
    return welford.mean, welford.std
