"""Welford online accumulator for streaming moments (mean, var, skew, kurt)."""

from __future__ import annotations

import math

import numpy as np


class WelfordAccumulator:
    """Streaming computation of count, mean, variance, skewness, kurtosis, min, max.

    Uses Welford (1962) for mean/M2 and Pébay (2008) extensions for M3/M4.
    Supports single-value updates, batch updates, and parallel merges via
    Chan et al. (1979).

    State is stored as 8 float64 values: [count, mean, M2, M3, M4, min, max, n_nan].
    """

    __slots__ = ("_state",)

    # Indices into the state array
    _COUNT = 0
    _MEAN = 1
    _M2 = 2
    _M3 = 3
    _M4 = 4
    _MIN = 5
    _MAX = 6
    _N_NAN = 7

    def __init__(self) -> None:
        self._state = np.zeros(8, dtype=np.float64)
        self._state[self._MIN] = np.inf
        self._state[self._MAX] = -np.inf

    def update(self, x: float) -> None:
        """Incorporate a single observation.

        NaN values increment the NaN counter only.
        """
        if math.isnan(x):
            self._state[self._N_NAN] += 1.0
            return

        s = self._state
        n1 = s[self._COUNT]
        n = n1 + 1.0
        delta = x - s[self._MEAN]
        delta_n = delta / n
        delta_n2 = delta_n * delta_n
        term1 = delta * delta_n * n1

        # Update M4 before M3 (uses old M2)
        s[self._M4] += (
            term1 * delta_n2 * (n * n - 3.0 * n + 3.0)
            + 6.0 * delta_n2 * s[self._M2]
            - 4.0 * delta_n * s[self._M3]
        )
        s[self._M3] += term1 * delta_n * (n - 2.0) - 3.0 * delta_n * s[self._M2]
        s[self._M2] += term1
        s[self._MEAN] += delta_n
        s[self._COUNT] = n

        if x < s[self._MIN]:
            s[self._MIN] = x
        if x > s[self._MAX]:
            s[self._MAX] = x

    def update_batch(self, values: np.ndarray) -> None:
        """Incorporate an array of observations using vectorized batch moments.

        Computes batch statistics with NumPy, then merges via Chan et al. (1979).
        """
        flat = np.asarray(values, dtype=np.float64).ravel()

        # Count and separate NaN values
        nan_mask = np.isnan(flat)
        n_nan = int(np.sum(nan_mask))

        valid = flat[~nan_mask]
        nb = len(valid)
        if nb == 0:
            self._state[self._N_NAN] += n_nan
            return

        # Compute batch moments
        batch_mean = np.mean(valid)
        diff = valid - batch_mean
        batch_M2 = np.sum(diff * diff)
        batch_M3 = np.sum(diff * diff * diff)
        batch_M4 = np.sum(diff * diff * diff * diff)
        batch_min = np.min(valid)
        batch_max = np.max(valid)

        # Build temporary batch state and merge (n_nan tracked separately
        # because merge's na==0 path does sa[:] = sb[:] which would overwrite)
        prior_nan = self._state[self._N_NAN]
        batch = WelfordAccumulator.__new__(WelfordAccumulator)
        batch._state = np.array(
            [nb, batch_mean, batch_M2, batch_M3, batch_M4, batch_min, batch_max, 0.0],
            dtype=np.float64,
        )
        self.merge(batch)
        self._state[self._N_NAN] = prior_nan + n_nan

    def merge(self, other: WelfordAccumulator) -> WelfordAccumulator:
        """Merge another accumulator into this one (Chan et al. 1979).

        Returns self for chaining.
        """
        sa, sb = self._state, other._state
        na, nb = sa[self._COUNT], sb[self._COUNT]

        if nb == 0.0:
            sa[self._N_NAN] += sb[self._N_NAN]
            return self
        if na == 0.0:
            sa[:] = sb[:]
            return self

        n = na + nb
        delta = sb[self._MEAN] - sa[self._MEAN]
        delta2 = delta * delta
        delta3 = delta2 * delta
        delta4 = delta2 * delta2

        new_mean = (na * sa[self._MEAN] + nb * sb[self._MEAN]) / n

        new_M2 = sa[self._M2] + sb[self._M2] + delta2 * na * nb / n

        new_M3 = (
            sa[self._M3]
            + sb[self._M3]
            + delta3 * na * nb * (na - nb) / (n * n)
            + 3.0 * delta * (na * sb[self._M2] - nb * sa[self._M2]) / n
        )

        new_M4 = (
            sa[self._M4]
            + sb[self._M4]
            + delta4 * na * nb * (na * na - na * nb + nb * nb) / (n * n * n)
            + 6.0 * delta2 * (na * na * sb[self._M2] + nb * nb * sa[self._M2]) / (n * n)
            + 4.0 * delta * (na * sb[self._M3] - nb * sa[self._M3]) / n
        )

        sa[self._COUNT] = n
        sa[self._MEAN] = new_mean
        sa[self._M2] = new_M2
        sa[self._M3] = new_M3
        sa[self._M4] = new_M4
        sa[self._MIN] = min(sa[self._MIN], sb[self._MIN])
        sa[self._MAX] = max(sa[self._MAX], sb[self._MAX])
        sa[self._N_NAN] += sb[self._N_NAN]

        return self

    # --- Properties ---

    @property
    def count(self) -> int:
        return int(self._state[self._COUNT])

    @property
    def n_nan(self) -> int:
        return int(self._state[self._N_NAN])

    @property
    def mean(self) -> float:
        if self._state[self._COUNT] == 0:
            return float("nan")
        return float(self._state[self._MEAN])

    @property
    def variance(self) -> float:
        n = self._state[self._COUNT]
        if n < 2:
            return float("nan")
        return float(self._state[self._M2] / (n - 1.0))

    @property
    def std(self) -> float:
        return math.sqrt(self.variance) if self.count >= 2 else float("nan")

    @property
    def skewness(self) -> float:
        n = self._state[self._COUNT]
        m2 = self._state[self._M2]
        if n < 3 or m2 == 0.0:
            return float("nan")
        return float(math.sqrt(n) * self._state[self._M3] / (m2**1.5))

    @property
    def kurtosis(self) -> float:
        """Excess kurtosis (Fisher definition, normal = 0)."""
        n = self._state[self._COUNT]
        m2 = self._state[self._M2]
        if n < 4 or m2 == 0.0:
            return float("nan")
        return float(n * self._state[self._M4] / (m2 * m2) - 3.0)

    @property
    def min(self) -> float:
        if self._state[self._COUNT] == 0:
            return float("nan")
        return float(self._state[self._MIN])

    @property
    def max(self) -> float:
        if self._state[self._COUNT] == 0:
            return float("nan")
        return float(self._state[self._MAX])

    # --- Serialization ---

    def to_array(self) -> np.ndarray:
        """Return state as a float64 array of shape (8,)."""
        return self._state.copy()

    @classmethod
    def from_array(cls, arr: np.ndarray) -> WelfordAccumulator:
        """Restore from a state array."""
        obj = cls.__new__(cls)
        obj._state = np.asarray(arr, dtype=np.float64).copy()
        return obj
