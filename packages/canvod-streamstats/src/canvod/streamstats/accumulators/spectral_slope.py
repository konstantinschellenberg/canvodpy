"""Spectral slope accumulator via log-periodogram regression."""

from __future__ import annotations

import numpy as np

from canvod.streamstats._types import (
    DEFAULT_RLS_FORGETTING_FACTOR,
    DEFAULT_SPECTRAL_SLOPE_N_FEATURES,
)
from canvod.streamstats.accumulators.rls import RecursiveLeastSquares


class SpectralSlopeAccumulator:
    """Streaming spectral slope estimation via log-log RLS regression.

    Wraps RecursiveLeastSquares(n_features=2) to fit:
        log10(PSD) = slope * log10(f) + intercept

    The slope (power-law exponent p) characterizes the phase PSD spectral
    behaviour; spectral strength T = 10^intercept.

    State: same as RLS with p=2: 3 + 2 + 4 = 9 float64.
    """

    __slots__ = ("_rls",)

    def __init__(
        self,
        forgetting_factor: float = DEFAULT_RLS_FORGETTING_FACTOR,
    ) -> None:
        self._rls = RecursiveLeastSquares(
            n_features=DEFAULT_SPECTRAL_SLOPE_N_FEATURES,
            forgetting_factor=forgetting_factor,
        )

    def update_batch(self, frequencies: np.ndarray, psd_values: np.ndarray) -> None:
        """Fit log10(PSD) = slope * log10(f) + intercept on positive values.

        Parameters
        ----------
        frequencies : array-like, shape (N,)
            Frequency values (Hz). Non-positive values are filtered.
        psd_values : array-like, shape (N,)
            Power spectral density values. Non-positive values are filtered.
        """
        freqs = np.asarray(frequencies, dtype=np.float64).ravel()
        psd = np.asarray(psd_values, dtype=np.float64).ravel()

        # Filter to positive frequencies and positive PSD
        mask = (freqs > 0) & (psd > 0) & ~np.isnan(freqs) & ~np.isnan(psd)
        freqs = freqs[mask]
        psd = psd[mask]

        if len(freqs) == 0:
            return

        log_f = np.log10(freqs)
        log_psd = np.log10(psd)
        X = np.column_stack([log_f, np.ones(len(log_f))])
        self._rls.update_batch(X, log_psd)

    def merge(self, other: SpectralSlopeAccumulator) -> SpectralSlopeAccumulator:
        """Merge another SpectralSlopeAccumulator. Right-biased. Returns self."""
        self._rls.merge(other._rls)
        return self

    # --- Properties ---

    @property
    def count(self) -> int:
        return self._rls.count

    @property
    def slope(self) -> float:
        """Power-law exponent p from log10(PSD) = p*log10(f) + c."""
        if self._rls.count == 0:
            return float("nan")
        return float(self._rls.beta[0])

    @property
    def intercept(self) -> float:
        """Intercept c = log10(spectral_strength)."""
        if self._rls.count == 0:
            return float("nan")
        return float(self._rls.beta[1])

    @property
    def spectral_strength(self) -> float:
        """T = 10^intercept."""
        if self._rls.count == 0:
            return float("nan")
        return float(10.0 ** self._rls.beta[1])

    # --- Serialization ---

    def to_array(self) -> np.ndarray:
        """Return state as flat float64 array (delegates to RLS)."""
        return self._rls.to_array()

    @classmethod
    def from_array(cls, arr: np.ndarray) -> SpectralSlopeAccumulator:
        """Restore from serialized array."""
        obj = cls.__new__(cls)
        obj._rls = RecursiveLeastSquares.from_array(arr)
        return obj
