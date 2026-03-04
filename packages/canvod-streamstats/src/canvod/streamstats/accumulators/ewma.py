"""Exponentially Weighted Moving Average (EWMA) streaming accumulator."""

from __future__ import annotations

import math

import numpy as np

from canvod.streamstats._types import DEFAULT_EWMA_HALFLIFE


class EWMAAccumulator:
    """O(1) streaming EWMA for mean and variance.

    Configurable via *half_life* (converted to ``alpha = 1 - 2^(-1/half_life)``)
    or explicit *alpha*.  Variance uses Roberts (1959):
    ``var_new = (1 - alpha) * (var_old + alpha * (x - mean_old)²)``.

    State layout (6 float64): ``[count, alpha, ewma_mean, ewma_var, last_value, n_nan]``
    """

    __slots__ = ("_state",)

    _COUNT = 0
    _ALPHA = 1
    _MEAN = 2
    _VAR = 3
    _LAST = 4
    _N_NAN = 5

    def __init__(
        self,
        half_life: float = DEFAULT_EWMA_HALFLIFE,
        alpha: float | None = None,
    ) -> None:
        if alpha is not None:
            if not (0.0 < alpha <= 1.0):
                msg = f"alpha must be in (0, 1], got {alpha}"
                raise ValueError(msg)
            a = alpha
        else:
            if half_life <= 0.0:
                msg = f"half_life must be > 0, got {half_life}"
                raise ValueError(msg)
            a = 1.0 - 2.0 ** (-1.0 / half_life)
        self._state = np.array([0.0, a, np.nan, np.nan, np.nan, 0.0], dtype=np.float64)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def count(self) -> int:
        return int(self._state[self._COUNT])

    @property
    def alpha(self) -> float:
        return float(self._state[self._ALPHA])

    @property
    def mean(self) -> float:
        return float(self._state[self._MEAN])

    @property
    def variance(self) -> float:
        if self.count < 2:
            return float("nan")
        return float(self._state[self._VAR])

    @property
    def std(self) -> float:
        v = self.variance
        if math.isnan(v):
            return float("nan")
        return math.sqrt(v)

    @property
    def last_value(self) -> float:
        return float(self._state[self._LAST])

    @property
    def n_nan(self) -> int:
        return int(self._state[self._N_NAN])

    # ------------------------------------------------------------------
    # Core API
    # ------------------------------------------------------------------

    def update(self, x: float) -> None:
        """Incorporate a single observation.  NaN increments NaN counter only."""
        if math.isnan(x):
            self._state[self._N_NAN] += 1.0
            return

        s = self._state
        a = s[self._ALPHA]
        s[self._COUNT] += 1.0
        s[self._LAST] = x

        if s[self._COUNT] == 1.0:
            s[self._MEAN] = x
            # variance stays NaN until count >= 2
        else:
            old_mean = s[self._MEAN]
            delta = x - old_mean
            s[self._MEAN] = old_mean + a * delta
            if s[self._COUNT] == 2.0:
                # First variance estimate
                s[self._VAR] = a * delta * delta
            else:
                s[self._VAR] = (1.0 - a) * (s[self._VAR] + a * delta * delta)

    def update_batch(self, values: np.ndarray) -> None:
        """Incorporate an array of observations sequentially (order-dependent)."""
        arr = np.asarray(values, dtype=np.float64).ravel()
        for x in arr:
            self.update(float(x))

    def merge(self, other: EWMAAccumulator) -> EWMAAccumulator:
        """Right-biased merge: adopt *other*'s EWMA state if it has data.

        Returns self for chaining.
        """
        if other.count == 0:
            return self
        if self.count == 0:
            self._state[:] = other._state
            return self
        # Right-biased: take other's ewma state, sum counts and n_nan
        self._state[self._MEAN] = other._state[self._MEAN]
        self._state[self._VAR] = other._state[self._VAR]
        self._state[self._LAST] = other._state[self._LAST]
        self._state[self._COUNT] += other._state[self._COUNT]
        self._state[self._N_NAN] += other._state[self._N_NAN]
        return self

    def to_array(self) -> np.ndarray:
        """Return state as a float64 array of shape (6,)."""
        return self._state.copy()

    @classmethod
    def from_array(cls, arr: np.ndarray) -> EWMAAccumulator:
        """Restore from a state array."""
        arr = np.asarray(arr, dtype=np.float64)
        if arr.shape[0] < 6:
            msg = f"Expected at least 6 elements, got {arr.shape[0]}"
            raise ValueError(msg)
        obj = object.__new__(cls)
        obj._state = arr[:6].copy()
        return obj
