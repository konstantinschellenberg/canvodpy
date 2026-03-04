"""Streaming autocovariance accumulator with circular buffer."""

from __future__ import annotations

import numpy as np


class StreamingAutocovariance:
    """Streaming autocovariance for lags 0..max_lag-1 using a circular buffer.

    Accumulates raw (uncentered) cross-products and centers at query time
    using the identity Cov(X_t, X_{t-τ}) = E[X_t · X_{t-τ}] − μ².
    This ensures batch and sequential updates produce identical results.

    State layout: [max_lag, count, pos, sum, reserved, *buffer(L), *raw_cross(L)]
    Total size: 2*L + 5
    """

    __slots__ = ("_buffer", "_count", "_max_lag", "_pos", "_raw_cross", "_sum")

    def __init__(self, max_lag: int = 1440) -> None:
        if max_lag < 1:
            msg = f"max_lag must be >= 1, got {max_lag}"
            raise ValueError(msg)
        self._max_lag = int(max_lag)
        self._buffer = np.zeros(self._max_lag, dtype=np.float64)
        self._raw_cross = np.zeros(self._max_lag, dtype=np.float64)
        self._pos = 0
        self._count = 0
        self._sum = 0.0

    def update(self, x: float) -> None:
        """Incorporate a single observation. NaN values are skipped."""
        if np.isnan(x):
            return

        L = self._max_lag
        filled = min(self._count, L)

        self._sum += x
        self._raw_cross[0] += x * x

        if filled > 0:
            max_tau = min(filled, L - 1)  # lag indices 1..L-1
            tau_range = np.arange(1, max_tau + 1)
            buf_indices = (self._pos - tau_range) % L
            self._raw_cross[tau_range] += x * self._buffer[buf_indices]

        self._buffer[self._pos] = x
        self._pos = (self._pos + 1) % L
        self._count += 1

    def update_batch(self, values: np.ndarray) -> None:
        """Incorporate an array of observations using vectorized lagged products.

        NaN values are filtered. For B valid values and L max_lag,
        computes raw cross-products for each lag via vectorized slicing.
        """
        flat = np.asarray(values, dtype=np.float64).ravel()
        valid = flat[~np.isnan(flat)]
        B = len(valid)
        if B == 0:
            return

        L = self._max_lag

        # Linearize existing circular buffer tail
        filled = min(self._count, L)
        if filled > 0:
            indices = (self._pos - filled + np.arange(filled)) % L
            linear_tail = self._buffer[indices]
        else:
            linear_tail = np.empty(0, dtype=np.float64)

        # Update sum
        self._sum += np.sum(valid)

        # Build working array: [linear_tail, valid]
        working = np.concatenate([linear_tail, valid])
        offset = len(linear_tail)

        # Lag 0: raw_cross[0] += Σ valid_i²
        self._raw_cross[0] += np.sum(valid * valid)

        # Lags 1..min(L, len(working))-1: vectorized raw products
        max_tau = min(L, len(working))
        for tau in range(1, max_tau):
            # For each batch element i, product = valid[i] * working[offset + i - tau]
            start_idx = max(0, tau - offset)
            if start_idx >= B:
                break
            batch_slice = valid[start_idx:]
            lag_indices = offset + np.arange(start_idx, B) - tau
            self._raw_cross[tau] += np.sum(batch_slice * working[lag_indices])

        # Update count
        self._count += B

        # Refill circular buffer with last min(L, total) values
        tail = working[-L:] if len(working) >= L else working
        tail_len = len(tail)
        self._buffer[:tail_len] = tail
        if tail_len < L:
            self._buffer[tail_len:] = 0.0
        self._pos = tail_len % L

    def merge(self, other: StreamingAutocovariance) -> StreamingAutocovariance:
        """Merge another autocovariance accumulator into this one.

        Sums raw cross-products and running sums. Cross-boundary pairs
        between the two chunks are not captured (documented limitation).
        Takes the most recent buffer from *other*. Returns self.
        """
        if other._count == 0:
            return self
        if self._count == 0:
            self._max_lag = other._max_lag
            self._buffer = other._buffer.copy()
            self._raw_cross = other._raw_cross.copy()
            self._pos = other._pos
            self._count = other._count
            self._sum = other._sum
            return self

        if self._max_lag != other._max_lag:
            msg = f"Cannot merge autocovariances with different max_lag: {self._max_lag} vs {other._max_lag}"
            raise ValueError(msg)

        self._raw_cross += other._raw_cross
        self._sum += other._sum
        self._count += other._count

        # Take the most recent buffer (from other)
        self._buffer = other._buffer.copy()
        self._pos = other._pos

        return self

    # --- Properties ---

    @property
    def count(self) -> int:
        return self._count

    @property
    def max_lag(self) -> int:
        return self._max_lag

    @property
    def mean(self) -> float:
        if self._count == 0:
            return float("nan")
        return self._sum / self._count

    def autocovariance(self, tau: int) -> float:
        """Return the autocovariance at lag tau.

        Uses: Cov(τ) = raw_cross[τ] / n_pairs(τ) − μ²
        where n_pairs(τ) = count − τ.
        """
        if tau < 0 or tau >= self._max_lag:
            return float("nan")
        n_pairs = self._count - tau
        if n_pairs < 1:
            return float("nan")
        mu = self._sum / self._count
        return self._raw_cross[tau] / n_pairs - mu * mu

    def autocorrelation(self, tau: int) -> float:
        """Return the autocorrelation at lag tau (normalized by lag-0)."""
        c0 = self.autocovariance(0)
        if np.isnan(c0) or c0 == 0.0:
            return float("nan")
        ct = self.autocovariance(tau)
        if np.isnan(ct):
            return float("nan")
        return ct / c0

    @property
    def autocovariance_array(self) -> np.ndarray:
        """Return autocovariance for all lags as array of shape (max_lag,)."""
        if self._count < 1:
            return np.full(self._max_lag, np.nan)
        mu = self._sum / self._count
        n_pairs = np.maximum(1, self._count - np.arange(self._max_lag))
        result = self._raw_cross / n_pairs - mu * mu
        # Mask lags with insufficient pairs
        result[self._count - np.arange(self._max_lag) < 1] = np.nan
        return result

    @property
    def autocorrelation_array(self) -> np.ndarray:
        """Return autocorrelation for all lags as array of shape (max_lag,)."""
        acov = self.autocovariance_array
        c0 = acov[0]
        if np.isnan(c0) or c0 == 0.0:
            return np.full(self._max_lag, np.nan)
        return acov / c0

    # --- Serialization ---

    def to_array(self) -> np.ndarray:
        """Serialize to flat array: [max_lag, count, pos, sum, 0, *buffer, *raw_cross].

        Shape: (2*max_lag + 5,)
        """
        L = self._max_lag
        arr = np.empty(2 * L + 5, dtype=np.float64)
        arr[0] = float(self._max_lag)
        arr[1] = float(self._count)
        arr[2] = float(self._pos)
        arr[3] = self._sum
        arr[4] = 0.0  # reserved
        arr[5 : 5 + L] = self._buffer
        arr[5 + L : 5 + 2 * L] = self._raw_cross
        return arr

    @classmethod
    def from_array(cls, arr: np.ndarray) -> StreamingAutocovariance:
        """Restore from serialized array."""
        data = np.asarray(arr, dtype=np.float64)
        max_lag = int(data[0])
        obj = cls.__new__(cls)
        obj._max_lag = max_lag
        obj._count = int(data[1])
        obj._pos = int(data[2])
        obj._sum = float(data[3])
        obj._buffer = data[5 : 5 + max_lag].copy()
        obj._raw_cross = data[5 + max_lag : 5 + 2 * max_lag].copy()
        return obj
