"""Fixed-bin streaming histogram."""

from __future__ import annotations

import math

import numpy as np

from canvod.streamstats._types import DEFAULT_HISTOGRAM_BINS


class StreamingHistogram:
    """Fixed-bin histogram with O(1) updates and underflow/overflow tracking.

    Parameters
    ----------
    low : float
        Lower edge of the first bin.
    high : float
        Upper edge of the last bin.
    n_bins : int
        Number of bins.
    """

    __slots__ = (
        "_bin_width",
        "_counts",
        "_high",
        "_low",
        "_n_bins",
        "_overflow",
        "_underflow",
    )

    def __init__(self, low: float, high: float, n_bins: int) -> None:
        if high <= low:
            msg = f"high ({high}) must be > low ({low})"
            raise ValueError(msg)
        if n_bins < 1:
            msg = f"n_bins must be >= 1, got {n_bins}"
            raise ValueError(msg)
        self._low = float(low)
        self._high = float(high)
        self._n_bins = int(n_bins)
        self._bin_width = (self._high - self._low) / self._n_bins
        self._counts = np.zeros(self._n_bins, dtype=np.int64)
        self._underflow: int = 0
        self._overflow: int = 0

    @classmethod
    def for_variable(cls, variable: str) -> StreamingHistogram:
        """Create a histogram with default bins for a known variable.

        Parameters
        ----------
        variable : str
            Variable name (must be a key in DEFAULT_HISTOGRAM_BINS).
        """
        if variable not in DEFAULT_HISTOGRAM_BINS:
            msg = (
                f"Unknown variable {variable!r}. Known: {list(DEFAULT_HISTOGRAM_BINS)}"
            )
            raise KeyError(msg)
        low, high, n_bins = DEFAULT_HISTOGRAM_BINS[variable]
        return cls(low, high, n_bins)

    def update(self, x: float) -> None:
        """Add a single observation."""
        if math.isnan(x):
            return
        if x < self._low:
            self._underflow += 1
        elif x >= self._high:
            self._overflow += 1
        else:
            idx = int((x - self._low) / self._bin_width)
            # Clamp to last bin for edge case x == high - epsilon
            if idx >= self._n_bins:
                idx = self._n_bins - 1
            self._counts[idx] += 1

    def update_batch(self, values: np.ndarray) -> None:
        """Add an array of observations (vectorized)."""
        arr = np.asarray(values, dtype=np.float64).ravel()
        valid = arr[np.isfinite(arr)]
        if len(valid) == 0:
            return

        self._underflow += int(np.sum(valid < self._low))
        self._overflow += int(np.sum(valid >= self._high))

        mask = (valid >= self._low) & (valid < self._high)
        in_range = valid[mask]
        if len(in_range) > 0:
            indices = ((in_range - self._low) / self._bin_width).astype(np.int64)
            np.clip(indices, 0, self._n_bins - 1, out=indices)
            np.add.at(self._counts, indices, 1)

    def merge(self, other: StreamingHistogram) -> StreamingHistogram:
        """Merge another histogram into this one. Returns self."""
        if (
            self._low != other._low
            or self._high != other._high
            or self._n_bins != other._n_bins
        ):
            msg = "Cannot merge histograms with different bin specs"
            raise ValueError(msg)
        self._counts += other._counts
        self._underflow += other._underflow
        self._overflow += other._overflow
        return self

    # --- Properties ---

    @property
    def low(self) -> float:
        return self._low

    @property
    def high(self) -> float:
        return self._high

    @property
    def n_bins(self) -> int:
        return self._n_bins

    @property
    def counts(self) -> np.ndarray:
        return self._counts.copy()

    @property
    def underflow(self) -> int:
        return self._underflow

    @property
    def overflow(self) -> int:
        return self._overflow

    @property
    def total(self) -> int:
        return int(self._counts.sum()) + self._underflow + self._overflow

    @property
    def bin_edges(self) -> np.ndarray:
        """Return bin edges array of shape (n_bins + 1,)."""
        return np.linspace(self._low, self._high, self._n_bins + 1)

    # --- Serialization ---

    def to_array(self) -> np.ndarray:
        """Serialize to array: [underflow, overflow, counts...]."""
        meta = np.array([self._underflow, self._overflow], dtype=np.int64)
        return np.concatenate([meta, self._counts])

    @classmethod
    def from_array(
        cls, arr: np.ndarray, low: float, high: float, n_bins: int
    ) -> StreamingHistogram:
        """Restore from serialized array."""
        obj = cls(low, high, n_bins)
        data = np.asarray(arr, dtype=np.int64)
        obj._underflow = int(data[0])
        obj._overflow = int(data[1])
        obj._counts = data[2 : 2 + n_bins].copy()
        return obj
