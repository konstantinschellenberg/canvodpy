"""Streaming permutation entropy accumulator."""

from __future__ import annotations

import math

import numpy as np

from canvod.streamstats._types import (
    DEFAULT_PERMUTATION_DELAY,
    DEFAULT_PERMUTATION_ORDER,
)


def _pattern_index(pattern: np.ndarray) -> int:
    """Map an ordinal pattern to a unique index via mixed-radix (factorial) encoding."""
    d = len(pattern)
    index = 0
    for i in range(d):
        # Count how many elements after position i are smaller than pattern[i]
        rank = 0
        for j in range(i + 1, d):
            if pattern[j] < pattern[i]:
                rank += 1
        index = index * (d - i) + rank
    return index


class PermutationEntropyAccumulator:
    """Streaming accumulator for permutation entropy of ordinal patterns.

    Maintains a frequency table of d! ordinal patterns with O(d) per-sample cost.

    Parameters
    ----------
    order : int
        Embedding dimension d (number of elements per pattern).
    delay : int
        Embedding delay τ.
    """

    __slots__ = (
        "_buffer",
        "_buffer_pos",
        "_count",
        "_delay",
        "_n_patterns",
        "_order",
        "_pattern_counts",
        "_total_added",
    )

    def __init__(
        self,
        order: int = DEFAULT_PERMUTATION_ORDER,
        delay: int = DEFAULT_PERMUTATION_DELAY,
    ) -> None:
        if order < 2:
            msg = f"order must be >= 2, got {order}"
            raise ValueError(msg)
        if delay < 1:
            msg = f"delay must be >= 1, got {delay}"
            raise ValueError(msg)
        self._order = order
        self._delay = delay
        self._n_patterns = math.factorial(order)
        self._pattern_counts = np.zeros(self._n_patterns, dtype=np.int64)
        buf_size = (order - 1) * delay + 1
        self._buffer = np.full(buf_size, np.nan, dtype=np.float64)
        self._buffer_pos = 0
        self._total_added = 0
        self._count = 0  # total patterns observed

    def update(self, x: float) -> None:
        """Add a single observation."""
        if math.isnan(x):
            return
        buf_size = len(self._buffer)
        self._buffer[self._buffer_pos % buf_size] = x
        self._total_added += 1
        self._buffer_pos += 1

        if self._total_added >= buf_size:
            # Extract subsequence with delay
            indices = [
                (self._buffer_pos - buf_size + i * self._delay) % buf_size
                for i in range(self._order)
            ]
            subseq = self._buffer[indices]
            if np.all(np.isfinite(subseq)):
                pattern = np.argsort(np.argsort(subseq)).astype(np.int64)
                idx = _pattern_index(pattern)
                self._pattern_counts[idx] += 1
                self._count += 1

    def update_batch(self, values: np.ndarray) -> None:
        """Add an array of observations."""
        arr = np.asarray(values, dtype=np.float64).ravel()
        for x in arr:
            self.update(x)

    def merge(
        self, other: PermutationEntropyAccumulator
    ) -> PermutationEntropyAccumulator:
        """Merge another accumulator. Sum counts; right-bias buffer state."""
        if self._order != other._order or self._delay != other._delay:
            msg = "Cannot merge accumulators with different order/delay"
            raise ValueError(msg)
        self._pattern_counts += other._pattern_counts
        self._count += other._count
        # Right-bias buffer state from other
        self._buffer = other._buffer.copy()
        self._buffer_pos = other._buffer_pos
        self._total_added = other._total_added
        return self

    # --- Properties ---

    @property
    def order(self) -> int:
        return self._order

    @property
    def delay(self) -> int:
        return self._delay

    @property
    def count(self) -> int:
        return self._count

    @property
    def pattern_distribution(self) -> np.ndarray:
        """Probability distribution over ordinal patterns."""
        total = self._count
        if total == 0:
            return np.zeros(self._n_patterns, dtype=np.float64)
        return self._pattern_counts.astype(np.float64) / total

    @property
    def entropy(self) -> float:
        """Permutation entropy H = -sum(p_i * log2(p_i))."""
        if self._count == 0:
            return 0.0
        probs = self.pattern_distribution
        # Filter zero probabilities
        nonzero = probs[probs > 0]
        return float(-np.sum(nonzero * np.log2(nonzero)))

    @property
    def normalized_entropy(self) -> float:
        """Normalized permutation entropy H / log2(d!) ∈ [0, 1]."""
        max_entropy = math.log2(self._n_patterns)
        if max_entropy == 0:
            return 0.0
        return self.entropy / max_entropy

    # --- Serialization ---

    def to_array(self) -> np.ndarray:
        """Serialize: [order, delay, count, buffer_pos, total_added, *pattern_counts, *buffer]."""
        meta = np.array(
            [
                self._order,
                self._delay,
                self._count,
                self._buffer_pos,
                self._total_added,
            ],
            dtype=np.float64,
        )
        return np.concatenate(
            [meta, self._pattern_counts.astype(np.float64), self._buffer]
        )

    @classmethod
    def from_array(cls, arr: np.ndarray) -> PermutationEntropyAccumulator:
        """Restore from serialized array."""
        data = np.asarray(arr, dtype=np.float64)
        order = int(data[0])
        delay = int(data[1])
        obj = cls(order=order, delay=delay)
        obj._count = int(data[2])
        obj._buffer_pos = int(data[3])
        obj._total_added = int(data[4])
        n_patterns = math.factorial(order)
        obj._pattern_counts = data[5 : 5 + n_patterns].astype(np.int64)
        obj._buffer = data[5 + n_patterns :].copy()
        return obj
