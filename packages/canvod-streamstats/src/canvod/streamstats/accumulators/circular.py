"""Streaming circular (angular) statistics accumulator."""

from __future__ import annotations

import numpy as np


class CircularAccumulator:
    """O(1) streaming accumulator for circular (angular) data.

    Maintains running sums of sin and cos to compute circular mean,
    mean resultant length, circular variance, von Mises kappa, and
    Rayleigh test statistics.

    State layout: [sin_sum, cos_sum, count] (3 float64)
    """

    __slots__ = ("_cos_sum", "_count", "_sin_sum")

    def __init__(self) -> None:
        self._sin_sum: float = 0.0
        self._cos_sum: float = 0.0
        self._count: int = 0

    def update(self, angle_rad: float) -> None:
        """Incorporate a single angle in radians. NaN silently skipped."""
        if np.isnan(angle_rad):
            return
        self._sin_sum += np.sin(angle_rad)
        self._cos_sum += np.cos(angle_rad)
        self._count += 1

    def update_batch(self, angles_rad: np.ndarray) -> None:
        """Incorporate a batch of angles in radians. NaN values filtered."""
        angles = np.asarray(angles_rad, dtype=np.float64).ravel()
        mask = ~np.isnan(angles)
        valid = angles[mask]
        if len(valid) == 0:
            return
        self._sin_sum += float(np.sum(np.sin(valid)))
        self._cos_sum += float(np.sum(np.cos(valid)))
        self._count += len(valid)

    @property
    def count(self) -> int:
        return self._count

    @property
    def circular_mean(self) -> float:
        """Mean direction in radians, via atan2(S/n, C/n)."""
        if self._count == 0:
            return np.nan
        return float(
            np.arctan2(self._sin_sum / self._count, self._cos_sum / self._count)
        )

    @property
    def mean_resultant_length(self) -> float:
        """Mean resultant length R-bar in [0, 1]."""
        if self._count == 0:
            return np.nan
        return float(np.sqrt(self._sin_sum**2 + self._cos_sum**2) / self._count)

    @property
    def circular_variance(self) -> float:
        """Circular variance V = 1 - R-bar, in [0, 1]."""
        if self._count == 0:
            return np.nan
        return 1.0 - self.mean_resultant_length

    @property
    def von_mises_kappa(self) -> float:
        """Approximate concentration parameter of von Mises distribution.

        Uses the approximation kappa = R-bar * (2 - R-bar^2) / (1 - R-bar^2).
        Returns inf when R-bar = 1 (perfectly concentrated).
        """
        if self._count == 0:
            return np.nan
        r = self.mean_resultant_length
        denom = 1.0 - r * r
        if denom <= 0.0:
            return np.inf
        return r * (2.0 - r * r) / denom

    @property
    def rayleigh_statistic(self) -> float:
        """Rayleigh test statistic Z = 2 * n * R-bar^2."""
        if self._count == 0:
            return np.nan
        r = self.mean_resultant_length
        return 2.0 * self._count * r * r

    @property
    def rayleigh_p_value(self) -> float:
        """Approximate p-value for Rayleigh test: exp(-Z)."""
        if self._count == 0:
            return np.nan
        z = self.rayleigh_statistic
        return float(np.exp(-z))

    # --- Merge ---

    def merge(self, other: CircularAccumulator) -> CircularAccumulator:
        """Merge another accumulator by summing state. Returns self."""
        self._sin_sum += other._sin_sum
        self._cos_sum += other._cos_sum
        self._count += other._count
        return self

    # --- Serialization ---

    def to_array(self) -> np.ndarray:
        """Serialize to flat array: [sin_sum, cos_sum, count]. Shape (3,)."""
        return np.array(
            [self._sin_sum, self._cos_sum, float(self._count)],
            dtype=np.float64,
        )

    @classmethod
    def from_array(cls, arr: np.ndarray) -> CircularAccumulator:
        """Restore from serialized array."""
        data = np.asarray(arr, dtype=np.float64)
        obj = cls.__new__(cls)
        obj._sin_sum = float(data[0])
        obj._cos_sum = float(data[1])
        obj._count = int(data[2])
        return obj
