"""Circular-buffer sliding-window median filter."""

from __future__ import annotations

import math

import numpy as np

from canvod.streamstats._types import DEFAULT_RUNNING_MEDIAN_WINDOW


class RunningMedianFilter:
    """Streaming median over a sliding window of the last *window* observations.

    Uses a circular buffer of fixed size.  ``median`` is computed from the
    filled portion of the buffer via ``np.nanmedian``.

    State layout: ``[count, window, pos, n_nan, *buffer]`` (4 + window float64)
    """

    __slots__ = ("_buffer", "_count", "_n_nan", "_pos", "_window")

    def __init__(self, window: int = DEFAULT_RUNNING_MEDIAN_WINDOW) -> None:
        if window < 1:
            msg = f"window must be >= 1, got {window}"
            raise ValueError(msg)
        if window % 2 == 0:
            msg = f"window must be odd, got {window}"
            raise ValueError(msg)
        self._window = window
        self._buffer = np.full(window, np.nan, dtype=np.float64)
        self._pos = 0
        self._count = 0
        self._n_nan = 0

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def count(self) -> int:
        return self._count

    @property
    def window(self) -> int:
        return self._window

    @property
    def n_nan(self) -> int:
        return self._n_nan

    @property
    def median(self) -> float:
        if self._count == 0:
            return float("nan")
        n = min(self._count, self._window)
        if n == self._window:
            vals = self._buffer
        else:
            # Only the first 'n' values written (they start at pos 0)
            vals = self._buffer[:n]
        result = np.nanmedian(vals)
        if np.isnan(result):
            return float("nan")
        return float(result)

    @property
    def buffer_values(self) -> np.ndarray:
        """Return filled portion of the buffer in chronological order."""
        n = min(self._count, self._window)
        if n == 0:
            return np.array([], dtype=np.float64)
        if self._count <= self._window:
            return self._buffer[:n].copy()
        # Circular: oldest is at _pos, wrap around
        return np.roll(self._buffer, -self._pos)[:n].copy()

    # ------------------------------------------------------------------
    # Core API
    # ------------------------------------------------------------------

    def update(self, x: float) -> None:
        """Add a single observation.  NaN increments NaN counter only."""
        if math.isnan(x):
            self._n_nan += 1
            return
        self._buffer[self._pos % self._window] = x
        self._pos = (self._pos + 1) % self._window
        self._count += 1

    def update_batch(self, values: np.ndarray) -> None:
        """Add an array of observations sequentially."""
        arr = np.asarray(values, dtype=np.float64).ravel()
        for x in arr:
            self.update(float(x))

    def merge(self, other: RunningMedianFilter) -> RunningMedianFilter:
        """Right-biased merge: adopt *other*'s buffer if it has data.

        Returns self for chaining.
        """
        if other.count == 0:
            return self
        if self._window != other._window:
            msg = f"Cannot merge filters with different windows ({self._window} vs {other._window})"
            raise ValueError(msg)
        # Right-biased: take other's buffer state
        self._buffer[:] = other._buffer
        self._pos = other._pos
        self._count += other._count
        self._n_nan += other._n_nan
        return self

    def to_array(self) -> np.ndarray:
        """Serialize to ``[count, window, pos, n_nan, *buffer]``."""
        header = np.array(
            [self._count, self._window, self._pos, self._n_nan],
            dtype=np.float64,
        )
        return np.concatenate([header, self._buffer])

    @classmethod
    def from_array(cls, arr: np.ndarray) -> RunningMedianFilter:
        """Restore from a state array."""
        arr = np.asarray(arr, dtype=np.float64)
        count = int(arr[0])
        window = int(arr[1])
        pos = int(arr[2])
        n_nan = int(arr[3])
        buf = arr[4 : 4 + window].copy()
        obj = object.__new__(cls)
        obj._window = window
        obj._buffer = buf
        obj._pos = pos
        obj._count = count
        obj._n_nan = n_nan
        return obj
