"""Fading accumulator for level-crossing rate and average fade duration."""

from __future__ import annotations

import math

import numpy as np

from canvod.streamstats._types import DEFAULT_FADE_THRESHOLDS


class FadingAccumulator:
    """Multi-threshold level-crossing and fade-duration tracker.

    Tracks threshold crossings and cumulative time below threshold for
    computing level-crossing rate (LCR) and average fade duration (AFD).

    State layout: [count, sum_I, sum_I², n_thresholds, reserved,
                   *per_threshold(n_crossings, cumulative_below, was_below)]
    Total: 5 + 3*T float64
    """

    __slots__ = ("_n_thresholds", "_state", "_thresholds")

    # Global state indices
    _COUNT = 0
    _SUM_I = 1
    _SUM_I2 = 2
    _N_THRESH = 3
    _RESERVED = 4
    _PER_THRESH_OFFSET = 5

    # Per-threshold offsets (relative to threshold base)
    _N_CROSSINGS = 0
    _CUMUL_BELOW = 1
    _WAS_BELOW = 2
    _PER_THRESH_SIZE = 3

    def __init__(self, thresholds: tuple[float, ...] = DEFAULT_FADE_THRESHOLDS) -> None:
        self._thresholds = tuple(thresholds)
        self._n_thresholds = len(thresholds)
        self._state = np.zeros(5 + 3 * self._n_thresholds, dtype=np.float64)
        self._state[self._N_THRESH] = float(self._n_thresholds)

    def _thresh_base(self, idx: int) -> int:
        return self._PER_THRESH_OFFSET + idx * self._PER_THRESH_SIZE

    def update(self, intensity: float) -> None:
        """Incorporate a single intensity observation."""
        if math.isnan(intensity):
            return

        s = self._state
        s[self._COUNT] += 1.0
        s[self._SUM_I] += intensity
        s[self._SUM_I2] += intensity * intensity

        # Compute current RMS
        rms = math.sqrt(s[self._SUM_I2] / s[self._COUNT])

        for i in range(self._n_thresholds):
            threshold = self._thresholds[i] * rms
            below = 1.0 if intensity < threshold else 0.0
            base = self._thresh_base(i)

            was_below = s[base + self._WAS_BELOW]
            # Crossing: transition from above to below
            if below == 1.0 and was_below == 0.0:
                s[base + self._N_CROSSINGS] += 1.0
            if below == 1.0:
                s[base + self._CUMUL_BELOW] += 1.0
            s[base + self._WAS_BELOW] = below

    def update_batch(self, intensity_array: np.ndarray) -> None:
        """Incorporate an array of intensity observations (vectorized).

        Updates running intensity stats with numpy, then computes threshold
        crossings against the post-batch RMS. Within-batch crossing detection
        uses np.diff on the below-mask for edge detection.
        """
        arr = np.asarray(intensity_array, dtype=np.float64).ravel()
        valid = arr[~np.isnan(arr)]
        n_new = len(valid)
        if n_new == 0:
            return

        s = self._state

        # Update running intensity statistics (vectorized)
        s[self._COUNT] += n_new
        s[self._SUM_I] += np.sum(valid)
        s[self._SUM_I2] += np.sum(valid * valid)

        # Compute RMS from updated running stats
        rms = math.sqrt(s[self._SUM_I2] / s[self._COUNT])

        # Vectorized crossing detection per threshold
        thresholds = np.asarray(self._thresholds, dtype=np.float64)  # (T,)
        abs_thresholds = thresholds * rms  # (T,)

        # below_matrix: (T, N) — True where sample is below threshold
        below_matrix = valid[np.newaxis, :] < abs_thresholds[:, np.newaxis]

        for i in range(self._n_thresholds):
            below = below_matrix[i]  # (N,) bool array
            base = self._thresh_base(i)
            was_below = s[base + self._WAS_BELOW] == 1.0

            # Prepend the previous was_below state for edge detection
            extended = np.empty(n_new + 1, dtype=np.bool_)
            extended[0] = was_below
            extended[1:] = below

            # Crossings: transitions from above (False) to below (True)
            transitions = np.diff(extended.astype(np.int8))
            n_crossings = int(np.sum(transitions == 1))

            s[base + self._N_CROSSINGS] += n_crossings
            s[base + self._CUMUL_BELOW] += int(np.sum(below))
            s[base + self._WAS_BELOW] = 1.0 if below[-1] else 0.0

    def merge(self, other: FadingAccumulator) -> FadingAccumulator:
        """Merge another FadingAccumulator into this one. Returns self.

        Sums crossings and cumulative_below; edge state (was_below) is
        right-biased (taken from other for temporal ordering).
        """
        if other._state[self._COUNT] == 0.0:
            return self
        if self._state[self._COUNT] == 0.0:
            self._state[:] = other._state[:]
            self._thresholds = other._thresholds
            self._n_thresholds = other._n_thresholds
            return self

        sa, sb = self._state, other._state
        sa[self._COUNT] += sb[self._COUNT]
        sa[self._SUM_I] += sb[self._SUM_I]
        sa[self._SUM_I2] += sb[self._SUM_I2]

        for i in range(self._n_thresholds):
            base = self._thresh_base(i)
            sa[base + self._N_CROSSINGS] += sb[base + self._N_CROSSINGS]
            sa[base + self._CUMUL_BELOW] += sb[base + self._CUMUL_BELOW]
            # Right-biased: take edge state from other
            sa[base + self._WAS_BELOW] = sb[base + self._WAS_BELOW]

        return self

    # --- Properties ---

    @property
    def count(self) -> int:
        return int(self._state[self._COUNT])

    @property
    def rms_intensity(self) -> float:
        n = self._state[self._COUNT]
        if n == 0:
            return float("nan")
        return float(math.sqrt(self._state[self._SUM_I2] / n))

    @property
    def n_thresholds(self) -> int:
        return self._n_thresholds

    @property
    def thresholds(self) -> tuple[float, ...]:
        return self._thresholds

    def level_crossing_rate(self, threshold_idx: int = 0) -> float:
        """Number of downward crossings per sample at given threshold."""
        n = self._state[self._COUNT]
        if n == 0:
            return float("nan")
        base = self._thresh_base(threshold_idx)
        return float(self._state[base + self._N_CROSSINGS] / n)

    def average_fade_duration(self, threshold_idx: int = 0) -> float:
        """Average consecutive samples below threshold at given threshold."""
        base = self._thresh_base(threshold_idx)
        crossings = self._state[base + self._N_CROSSINGS]
        if crossings == 0:
            return float("nan")
        return float(self._state[base + self._CUMUL_BELOW] / crossings)

    def fraction_below(self, threshold_idx: int = 0) -> float:
        """Fraction of samples below the given threshold."""
        n = self._state[self._COUNT]
        if n == 0:
            return float("nan")
        base = self._thresh_base(threshold_idx)
        return float(self._state[base + self._CUMUL_BELOW] / n)

    def n_crossings(self, threshold_idx: int = 0) -> int:
        """Number of downward crossings at given threshold."""
        base = self._thresh_base(threshold_idx)
        return int(self._state[base + self._N_CROSSINGS])

    # --- Serialization ---

    def to_array(self) -> np.ndarray:
        """Return state as a flat float64 array. Thresholds stored after state."""
        # Layout: [*state(5+3T), *thresholds(T)]
        arr = np.empty(len(self._state) + self._n_thresholds, dtype=np.float64)
        arr[: len(self._state)] = self._state
        arr[len(self._state) :] = self._thresholds
        return arr

    @classmethod
    def from_array(cls, arr: np.ndarray) -> FadingAccumulator:
        """Restore from serialized array."""
        data = np.asarray(arr, dtype=np.float64)
        n_thresh = int(data[cls._N_THRESH])
        state_len = 5 + 3 * n_thresh
        thresholds = tuple(data[state_len : state_len + n_thresh].tolist())

        obj = cls.__new__(cls)
        obj._n_thresholds = n_thresh
        obj._thresholds = thresholds
        obj._state = data[:state_len].copy()
        return obj
