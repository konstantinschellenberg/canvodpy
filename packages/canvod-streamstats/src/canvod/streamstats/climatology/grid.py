"""DOY × TOD climatology grid backed by WelfordAccumulators."""

from __future__ import annotations

import math

import numpy as np

from canvod.streamstats._types import DEFAULT_DOY_WINDOW, DEFAULT_TOD_WINDOW
from canvod.streamstats.accumulators.welford import WelfordAccumulator

# Number of float64 values per Welford state
_WELFORD_STATE_SIZE = 8


class ClimatologyGrid:
    """2-D grid of WelfordAccumulators indexed by (DOY-bin, TOD-bin).

    Parameters
    ----------
    doy_window : int
        Width of each DOY bin in days (default 15).
    tod_window : int
        Width of each TOD bin in hours (default 1).
    """

    __slots__ = ("_bins", "_doy_window", "_n_doy", "_n_tod", "_tod_window")

    def __init__(
        self,
        doy_window: int = DEFAULT_DOY_WINDOW,
        tod_window: int = DEFAULT_TOD_WINDOW,
    ) -> None:
        if doy_window < 1 or doy_window > 366:
            msg = f"doy_window must be in [1, 366], got {doy_window}"
            raise ValueError(msg)
        if tod_window < 1 or tod_window > 24:
            msg = f"tod_window must be in [1, 24], got {tod_window}"
            raise ValueError(msg)

        self._doy_window = doy_window
        self._tod_window = tod_window
        self._n_doy = math.ceil(366 / doy_window)
        self._n_tod = math.ceil(24 / tod_window)
        self._bins: list[WelfordAccumulator] = [
            WelfordAccumulator() for _ in range(self._n_doy * self._n_tod)
        ]

    # -- properties -----------------------------------------------------------

    @property
    def doy_window(self) -> int:
        return self._doy_window

    @property
    def tod_window(self) -> int:
        return self._tod_window

    @property
    def n_doy_bins(self) -> int:
        return self._n_doy

    @property
    def n_tod_bins(self) -> int:
        return self._n_tod

    @property
    def shape(self) -> tuple[int, int]:
        return (self._n_doy, self._n_tod)

    # -- bin mapping ----------------------------------------------------------

    def _doy_bin(self, doy: int) -> int:
        """Map DOY (1–366) to a bin index, wrapping circularly."""
        return ((doy - 1) % 366) // self._doy_window

    def _tod_bin(self, hour: float) -> int:
        """Map hour-of-day [0, 24) to a bin index."""
        return min(int(hour // self._tod_window), self._n_tod - 1)

    def _flat_idx(self, doy_bin: int, tod_bin: int) -> int:
        return doy_bin * self._n_tod + tod_bin

    # -- update ---------------------------------------------------------------

    def update(self, doy: int, hour: float, value: float) -> None:
        """Route a single observation to its grid bin."""
        db = self._doy_bin(doy)
        tb = self._tod_bin(hour)
        self._bins[self._flat_idx(db, tb)].update(value)

    def update_batch(
        self,
        doys: np.ndarray,
        hours: np.ndarray,
        values: np.ndarray,
    ) -> None:
        """Vectorised bin assignment with grouped updates."""
        doys = np.asarray(doys, dtype=np.int32)
        hours = np.asarray(hours, dtype=np.float64)
        values = np.asarray(values, dtype=np.float64)

        db = ((doys - 1) % 366) // self._doy_window
        tb = np.minimum((hours // self._tod_window).astype(np.int32), self._n_tod - 1)
        flat = db * self._n_tod + tb

        # Group-by update: iterate unique bins
        for idx in np.unique(flat):
            mask = flat == idx
            self._bins[int(idx)].update_batch(values[mask])

    # -- query ----------------------------------------------------------------

    def climatology_at(self, doy: int, hour: float) -> tuple[float, float, int]:
        """Return (mean, std, count) for the bin containing (doy, hour)."""
        db = self._doy_bin(doy)
        tb = self._tod_bin(hour)
        acc = self._bins[self._flat_idx(db, tb)]
        return (acc.mean, acc.std, int(acc.count))

    def mean(self, doy_bin: int, tod_bin: int) -> float:
        """Mean for a specific grid cell."""
        return self._bins[self._flat_idx(doy_bin, tod_bin)].mean

    def std(self, doy_bin: int, tod_bin: int) -> float:
        """Standard deviation for a specific grid cell."""
        return self._bins[self._flat_idx(doy_bin, tod_bin)].std

    def count(self, doy_bin: int, tod_bin: int) -> int:
        """Observation count for a specific grid cell."""
        return int(self._bins[self._flat_idx(doy_bin, tod_bin)].count)

    # -- serialisation --------------------------------------------------------

    def to_array(self) -> np.ndarray:
        """Serialise to a 1-D float64 array.

        Layout: [doy_window, tod_window, *welford_states]
        where each Welford state is 8 values.
        """
        n_bins = self._n_doy * self._n_tod
        out = np.empty(2 + n_bins * _WELFORD_STATE_SIZE, dtype=np.float64)
        out[0] = self._doy_window
        out[1] = self._tod_window
        for i, acc in enumerate(self._bins):
            start = 2 + i * _WELFORD_STATE_SIZE
            out[start : start + _WELFORD_STATE_SIZE] = acc.to_array()
        return out

    @classmethod
    def from_array(cls, arr: np.ndarray) -> ClimatologyGrid:
        """Restore from a serialised array."""
        arr = np.asarray(arr, dtype=np.float64)
        doy_window = int(arr[0])
        tod_window = int(arr[1])
        grid = cls(doy_window=doy_window, tod_window=tod_window)
        for i, acc in enumerate(grid._bins):
            start = 2 + i * _WELFORD_STATE_SIZE
            state = arr[start : start + _WELFORD_STATE_SIZE]
            grid._bins[i] = WelfordAccumulator.from_array(state)
        return grid

    # -- merge ----------------------------------------------------------------

    def merge(self, other: ClimatologyGrid) -> ClimatologyGrid:
        """Merge another grid into this one. Must have identical dimensions.

        Returns self for chaining.
        """
        if (
            self._doy_window != other._doy_window
            or self._tod_window != other._tod_window
        ):
            msg = (
                f"Cannot merge grids with different windows: "
                f"({self._doy_window}, {self._tod_window}) vs "
                f"({other._doy_window}, {other._tod_window})"
            )
            raise ValueError(msg)
        for i in range(len(self._bins)):
            self._bins[i].merge(other._bins[i])
        return self
