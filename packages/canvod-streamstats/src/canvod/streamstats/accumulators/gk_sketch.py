"""Greenwald-Khanna (2001) streaming quantile sketch."""

from __future__ import annotations

import math
from bisect import insort_right

import numpy as np


class _Tuple:
    """Internal GK tuple: (value, g, delta)."""

    __slots__ = ("delta", "g", "value")

    def __init__(self, value: float, g: int, delta: int) -> None:
        self.value = value
        self.g = g
        self.delta = delta

    def __lt__(self, other: _Tuple) -> bool:
        return self.value < other.value


class GKSketch:
    """Streaming ε-approximate quantile sketch.

    After *n* insertions, ``query(φ)`` returns a value whose true rank is
    within ±εn of the desired rank φn.

    Parameters
    ----------
    epsilon : float
        Approximation guarantee (default 0.01 → 1% rank error).
    """

    __slots__ = ("_compress_threshold", "_count", "_epsilon", "_summary")

    def __init__(self, epsilon: float = 0.01) -> None:
        if epsilon <= 0.0 or epsilon >= 1.0:
            msg = f"epsilon must be in (0, 1), got {epsilon}"
            raise ValueError(msg)
        self._epsilon = epsilon
        self._summary: list[_Tuple] = []
        self._count: int = 0
        # Compress every 1/(2ε) insertions
        self._compress_threshold = max(1, int(1.0 / (2.0 * epsilon)))

    def update(self, x: float) -> None:
        """Insert a single observation (NaN values are skipped)."""
        if math.isnan(x):
            return

        self._count += 1

        if len(self._summary) == 0:
            self._summary.append(_Tuple(x, 1, 0))
            return

        # Find insertion position
        pos = 0
        for i, t in enumerate(self._summary):
            if t.value > x:
                pos = i
                break
        else:
            pos = len(self._summary)

        # Compute delta
        if pos == 0 or pos == len(self._summary):
            delta = 0
        else:
            delta = max(0, int(2.0 * self._epsilon * self._count) - 1)

        insort_right(self._summary, _Tuple(x, 1, delta))

        if self._count % self._compress_threshold == 0:
            self.compress()

    def update_batch(self, values: np.ndarray) -> None:
        """Insert an array of observations using sorted two-pointer merge.

        NaN values are skipped. Sorted batch is merged into the existing
        sorted summary in O(n + B) instead of B × O(n) ``insort_right`` calls.
        A single ``compress()`` runs at the end.
        """
        flat = np.asarray(values, dtype=np.float64).ravel()
        valid = flat[~np.isnan(flat)]
        if len(valid) == 0:
            return

        sorted_vals = np.sort(valid)
        b = len(sorted_vals)

        if len(self._summary) == 0:
            # Cold start: build summary from sorted values
            self._count = b
            self._summary = [_Tuple(float(sorted_vals[0]), 1, 0)]
            for i in range(1, b - 1):
                delta = max(0, int(2.0 * self._epsilon * self._count) - 1)
                self._summary.append(_Tuple(float(sorted_vals[i]), 1, delta))
            if b > 1:
                self._summary.append(_Tuple(float(sorted_vals[-1]), 1, 0))
            self.compress()
            return

        # Build batch tuples with appropriate deltas
        batch_tuples: list[_Tuple] = []
        new_count = self._count + b
        for i in range(b):
            if i == 0 or i == b - 1:
                # First/last in batch get delta=0 (they may be new min/max)
                delta = 0
            else:
                delta = max(0, int(2.0 * self._epsilon * new_count) - 1)
            batch_tuples.append(_Tuple(float(sorted_vals[i]), 1, delta))

        # Two-pointer merge of existing summary and sorted batch
        merged: list[_Tuple] = []
        i, j = 0, 0
        sa = self._summary
        sb = batch_tuples

        while i < len(sa) and j < len(sb):
            if sa[i].value <= sb[j].value:
                merged.append(sa[i])
                i += 1
            else:
                merged.append(sb[j])
                j += 1
        while i < len(sa):
            merged.append(sa[i])
            i += 1
        while j < len(sb):
            merged.append(sb[j])
            j += 1

        # Ensure first and last have delta=0
        if merged:
            merged[0].delta = 0
            merged[-1].delta = 0

        self._summary = merged
        self._count = new_count
        self.compress()

    def compress(self) -> None:
        """Remove redundant tuples while maintaining the ε guarantee."""
        if len(self._summary) < 3:
            return

        threshold = int(2.0 * self._epsilon * self._count)
        i = len(self._summary) - 2  # Start from second-to-last
        while i >= 1:
            t_i = self._summary[i]
            t_next = self._summary[i + 1]
            if t_i.g + t_next.g + t_next.delta <= threshold:
                # Merge t_i into t_next
                t_next.g += t_i.g
                del self._summary[i]
            i -= 1

    def query(self, phi: float) -> float:
        """Return the approximate φ-quantile.

        Parameters
        ----------
        phi : float
            Quantile probability in [0, 1].

        Returns
        -------
        float
            Approximate quantile value.
        """
        if not self._summary:
            return float("nan")
        if phi <= 0.0:
            return self._summary[0].value
        if phi >= 1.0:
            return self._summary[-1].value

        target_rank = phi * self._count
        tolerance = self._epsilon * self._count

        rank_min = 0.0
        best = self._summary[0].value

        for t in self._summary:
            rank_min += t.g
            rank_max = rank_min + t.delta
            if rank_min - tolerance <= target_rank <= rank_max + tolerance:
                best = t.value

        return best

    def snapshot(
        self, probs: tuple[float, ...] | np.ndarray | None = None
    ) -> np.ndarray:
        """Compute multiple quantiles at once.

        Parameters
        ----------
        probs : sequence of float, optional
            Quantile probabilities. Defaults to DEFAULT_QUANTILE_PROBS.

        Returns
        -------
        np.ndarray
            Array of quantile values.
        """
        if probs is None:
            from canvod.streamstats._types import DEFAULT_QUANTILE_PROBS

            probs = DEFAULT_QUANTILE_PROBS
        return np.array([self.query(p) for p in probs], dtype=np.float64)

    def merge(self, other: GKSketch) -> GKSketch:
        """Merge another sketch into this one. Returns self.

        The merged sketch has ε guarantee of max(self.ε, other.ε).
        """
        if other._count == 0:
            return self
        if self._count == 0:
            self._summary = [_Tuple(t.value, t.g, t.delta) for t in other._summary]
            self._count = other._count
            self._epsilon = max(self._epsilon, other._epsilon)
            return self

        # Merge sorted summaries
        merged: list[_Tuple] = []
        i, j = 0, 0
        sa, sb = self._summary, other._summary

        while i < len(sa) and j < len(sb):
            if sa[i].value <= sb[j].value:
                merged.append(_Tuple(sa[i].value, sa[i].g, sa[i].delta))
                i += 1
            else:
                merged.append(_Tuple(sb[j].value, sb[j].g, sb[j].delta))
                j += 1
        while i < len(sa):
            merged.append(_Tuple(sa[i].value, sa[i].g, sa[i].delta))
            i += 1
        while j < len(sb):
            merged.append(_Tuple(sb[j].value, sb[j].g, sb[j].delta))
            j += 1

        self._summary = merged
        self._count += other._count
        self._epsilon = max(self._epsilon, other._epsilon)
        self.compress()
        return self

    @property
    def count(self) -> int:
        return self._count

    @property
    def epsilon(self) -> float:
        return self._epsilon

    @property
    def size(self) -> int:
        """Number of tuples in the summary."""
        return len(self._summary)

    # --- Serialization ---

    def to_array(self) -> np.ndarray:
        """Serialize to flat array: [count, epsilon, n_tuples, v0, g0, d0, v1, g1, d1, ...]."""
        n = len(self._summary)
        arr = np.empty(3 + 3 * n, dtype=np.float64)
        arr[0] = float(self._count)
        arr[1] = self._epsilon
        arr[2] = float(n)
        for k, t in enumerate(self._summary):
            base = 3 + 3 * k
            arr[base] = t.value
            arr[base + 1] = float(t.g)
            arr[base + 2] = float(t.delta)
        return arr

    @classmethod
    def from_array(cls, arr: np.ndarray, epsilon: float | None = None) -> GKSketch:
        """Restore from serialized array."""
        data = np.asarray(arr, dtype=np.float64)
        count = int(data[0])
        eps = float(data[1]) if epsilon is None else epsilon
        n = int(data[2])

        obj = cls(epsilon=eps)
        obj._count = count
        obj._summary = []
        for k in range(n):
            base = 3 + 3 * k
            obj._summary.append(
                _Tuple(float(data[base]), int(data[base + 1]), int(data[base + 2]))
            )
        return obj
