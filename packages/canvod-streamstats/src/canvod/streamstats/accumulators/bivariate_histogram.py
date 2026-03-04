"""Streaming 2D fixed-bin histogram for joint distributions."""

from __future__ import annotations

import math

import numpy as np

from canvod.streamstats._types import DEFAULT_BIVARIATE_N_BINS


class BivariateHistogram:
    """2D fixed-bin histogram for joint distributions.

    Parameters
    ----------
    low_x, high_x : float
        Bin range for the x axis.
    low_y, high_y : float
        Bin range for the y axis.
    n_bins_x, n_bins_y : int
        Number of bins per axis.
    """

    __slots__ = (
        "_bin_width_x",
        "_bin_width_y",
        "_count",
        "_counts",
        "_high_x",
        "_high_y",
        "_low_x",
        "_low_y",
        "_n_bins_x",
        "_n_bins_y",
    )

    def __init__(
        self,
        low_x: float,
        high_x: float,
        low_y: float,
        high_y: float,
        n_bins_x: int = DEFAULT_BIVARIATE_N_BINS,
        n_bins_y: int = DEFAULT_BIVARIATE_N_BINS,
    ) -> None:
        if high_x <= low_x:
            msg = f"high_x ({high_x}) must be > low_x ({low_x})"
            raise ValueError(msg)
        if high_y <= low_y:
            msg = f"high_y ({high_y}) must be > low_y ({low_y})"
            raise ValueError(msg)
        if n_bins_x < 1 or n_bins_y < 1:
            msg = f"n_bins must be >= 1, got ({n_bins_x}, {n_bins_y})"
            raise ValueError(msg)
        self._low_x = float(low_x)
        self._high_x = float(high_x)
        self._low_y = float(low_y)
        self._high_y = float(high_y)
        self._n_bins_x = int(n_bins_x)
        self._n_bins_y = int(n_bins_y)
        self._bin_width_x = (self._high_x - self._low_x) / self._n_bins_x
        self._bin_width_y = (self._high_y - self._low_y) / self._n_bins_y
        self._counts = np.zeros((self._n_bins_x, self._n_bins_y), dtype=np.int64)
        self._count = 0

    def update(self, x: float, y: float) -> None:
        """Add a single (x, y) observation."""
        if math.isnan(x) or math.isnan(y):
            return
        if x < self._low_x or x >= self._high_x:
            return
        if y < self._low_y or y >= self._high_y:
            return
        ix = int((x - self._low_x) / self._bin_width_x)
        iy = int((y - self._low_y) / self._bin_width_y)
        ix = min(ix, self._n_bins_x - 1)
        iy = min(iy, self._n_bins_y - 1)
        self._counts[ix, iy] += 1
        self._count += 1

    def update_batch(self, x_arr: np.ndarray, y_arr: np.ndarray) -> None:
        """Add arrays of (x, y) observations (vectorized)."""
        x = np.asarray(x_arr, dtype=np.float64).ravel()
        y = np.asarray(y_arr, dtype=np.float64).ravel()
        valid = np.isfinite(x) & np.isfinite(y)
        x = x[valid]
        y = y[valid]
        if len(x) == 0:
            return

        in_range = (
            (x >= self._low_x)
            & (x < self._high_x)
            & (y >= self._low_y)
            & (y < self._high_y)
        )
        xr = x[in_range]
        yr = y[in_range]
        if len(xr) == 0:
            return

        ix = ((xr - self._low_x) / self._bin_width_x).astype(np.int64)
        iy = ((yr - self._low_y) / self._bin_width_y).astype(np.int64)
        np.clip(ix, 0, self._n_bins_x - 1, out=ix)
        np.clip(iy, 0, self._n_bins_y - 1, out=iy)
        np.add.at(self._counts, (ix, iy), 1)
        self._count += len(xr)

    def merge(self, other: BivariateHistogram) -> BivariateHistogram:
        """Merge another histogram into this one. Returns self."""
        if (
            self._low_x != other._low_x
            or self._high_x != other._high_x
            or self._low_y != other._low_y
            or self._high_y != other._high_y
            or self._n_bins_x != other._n_bins_x
            or self._n_bins_y != other._n_bins_y
        ):
            msg = "Cannot merge histograms with different bin specs"
            raise ValueError(msg)
        self._counts += other._counts
        self._count += other._count
        return self

    # --- Properties ---

    @property
    def n_bins_x(self) -> int:
        return self._n_bins_x

    @property
    def n_bins_y(self) -> int:
        return self._n_bins_y

    @property
    def counts(self) -> np.ndarray:
        return self._counts.copy()

    @property
    def total(self) -> int:
        return self._count

    @property
    def marginal_x(self) -> np.ndarray:
        """1D marginal counts along x (sum over y axis)."""
        return self._counts.sum(axis=1)

    @property
    def marginal_y(self) -> np.ndarray:
        """1D marginal counts along y (sum over x axis)."""
        return self._counts.sum(axis=0)

    @property
    def joint_probabilities(self) -> np.ndarray:
        """Joint probability matrix P(x, y) = counts / total."""
        if self._count == 0:
            return np.zeros_like(self._counts, dtype=np.float64)
        return self._counts.astype(np.float64) / self._count

    # --- Serialization ---

    def to_array(self) -> np.ndarray:
        """Serialize: [n_bins_x, n_bins_y, low_x, high_x, low_y, high_y, count, *counts.ravel()]."""
        meta = np.array(
            [
                self._n_bins_x,
                self._n_bins_y,
                self._low_x,
                self._high_x,
                self._low_y,
                self._high_y,
                self._count,
            ],
            dtype=np.float64,
        )
        return np.concatenate([meta, self._counts.ravel().astype(np.float64)])

    @classmethod
    def from_array(cls, arr: np.ndarray) -> BivariateHistogram:
        """Restore from serialized array."""
        data = np.asarray(arr, dtype=np.float64)
        n_bins_x = int(data[0])
        n_bins_y = int(data[1])
        low_x = float(data[2])
        high_x = float(data[3])
        low_y = float(data[4])
        high_y = float(data[5])
        obj = cls(low_x, high_x, low_y, high_y, n_bins_x, n_bins_y)
        obj._count = int(data[6])
        obj._counts = (
            data[7 : 7 + n_bins_x * n_bins_y]
            .astype(np.int64)
            .reshape(n_bins_x, n_bins_y)
        )
        return obj
