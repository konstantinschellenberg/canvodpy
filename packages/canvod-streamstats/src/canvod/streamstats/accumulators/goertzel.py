"""Streaming Goertzel accumulator for monitoring power at target frequencies."""

from __future__ import annotations

import math

import numpy as np

from canvod.streamstats._types import (
    DEFAULT_GOERTZEL_FREQUENCIES,
    DEFAULT_GOERTZEL_WINDOW,
)


class GoertzelAccumulator:
    """Streaming power monitor at specific target frequencies via the Goertzel algorithm.

    Uses a circular buffer of size N. Every N samples the Goertzel recursion
    completes and power is computed for each target frequency.  O(1) per
    sample per frequency.

    State layout for ``to_array`` / ``from_array``::

        [count, n_frequencies, sample_rate, window_size, buffer_pos,
         *(coeff, s1, s2, power) per frequency,
         *buffer]
    """

    __slots__ = (
        "_buffer",
        "_buffer_pos",
        "_coeffs",
        "_count",
        "_frequencies",
        "_power",
        "_s1",
        "_s2",
        "_sample_rate",
        "_window_size",
    )

    _HEADER_SIZE = 5  # count, n_freq, sample_rate, window_size, buffer_pos
    _PER_FREQ = 4  # coeff, s1, s2, power

    def __init__(
        self,
        frequencies: tuple[float, ...] = DEFAULT_GOERTZEL_FREQUENCIES,
        sample_rate: float = 1.0,
        window_size: int = DEFAULT_GOERTZEL_WINDOW,
    ) -> None:
        self._frequencies = frequencies
        self._sample_rate = sample_rate
        self._window_size = window_size

        nf = len(frequencies)
        self._coeffs = np.empty(nf, dtype=np.float64)
        for i, f in enumerate(frequencies):
            normalised = f / sample_rate
            self._coeffs[i] = 2.0 * math.cos(2.0 * math.pi * normalised)

        self._s1 = np.zeros(nf, dtype=np.float64)
        self._s2 = np.zeros(nf, dtype=np.float64)
        self._power = np.full(nf, np.nan, dtype=np.float64)
        self._buffer = np.zeros(window_size, dtype=np.float64)
        self._buffer_pos = 0
        self._count = 0

    # --- Update ---

    def update(self, x: float) -> None:
        """Process a single sample through the Goertzel recursion."""
        if math.isnan(x):
            return

        self._buffer[self._buffer_pos] = x
        self._buffer_pos += 1
        self._count += 1

        # Goertzel recursion step
        s0 = x + self._coeffs * self._s1 - self._s2
        self._s2[:] = self._s1
        self._s1[:] = s0

        if self._buffer_pos >= self._window_size:
            self._complete_window()

    def update_batch(self, values: np.ndarray) -> None:
        """Process an array of samples, completing windows as needed."""
        arr = np.asarray(values, dtype=np.float64).ravel()
        valid = arr[~np.isnan(arr)]
        if len(valid) == 0:
            return

        pos = 0
        while pos < len(valid):
            remaining_in_window = self._window_size - self._buffer_pos
            chunk_size = min(remaining_in_window, len(valid) - pos)
            chunk = valid[pos : pos + chunk_size]

            # Store in buffer
            self._buffer[self._buffer_pos : self._buffer_pos + chunk_size] = chunk
            self._buffer_pos += chunk_size
            self._count += chunk_size

            # Vectorised Goertzel recursion over the chunk
            for sample in chunk:
                s0 = sample + self._coeffs * self._s1 - self._s2
                self._s2[:] = self._s1
                self._s1[:] = s0

            if self._buffer_pos >= self._window_size:
                self._complete_window()

            pos += chunk_size

    def _complete_window(self) -> None:
        """Compute power at end of window and reset state for the next window."""
        self._power[:] = self._s1**2 + self._s2**2 - self._coeffs * self._s1 * self._s2
        self._s1[:] = 0.0
        self._s2[:] = 0.0
        self._buffer_pos = 0

    # --- Properties ---

    @property
    def count(self) -> int:
        return self._count

    @property
    def frequencies(self) -> tuple[float, ...]:
        return self._frequencies

    @property
    def sample_rate(self) -> float:
        return self._sample_rate

    @property
    def window_size(self) -> int:
        return self._window_size

    def power(self, freq_idx: int) -> float:
        """Spectral power at a target frequency (NaN until first window completes)."""
        return float(self._power[freq_idx])

    def amplitude(self, freq_idx: int) -> float:
        """Amplitude (sqrt of power) at a target frequency."""
        p = self._power[freq_idx]
        if np.isnan(p) or p < 0.0:
            return float("nan")
        return math.sqrt(float(p))

    def power_array(self) -> np.ndarray:
        """All power values as array of shape (n_frequencies,)."""
        return self._power.copy()

    # --- Merge ---

    def merge(self, other: GoertzelAccumulator) -> GoertzelAccumulator:
        """Merge: right-biased — keeps the accumulator with more observations.

        Goertzel is inherently sequential so parallel merge is not meaningful.
        """
        if other._count == 0:
            return self
        if self._count == 0 or other._count > self._count:
            self._s1[:] = other._s1
            self._s2[:] = other._s2
            self._power[:] = other._power
            self._buffer[:] = other._buffer
            self._buffer_pos = other._buffer_pos
            self._count = other._count
        return self

    # --- Serialization ---

    def to_array(self) -> np.ndarray:
        """Serialize to flat float64 array.

        Layout: [count, n_freq, sample_rate, window_size, buffer_pos,
                 *(coeff_i, s1_i, s2_i, power_i) for each freq,
                 *buffer]
        """
        nf = len(self._frequencies)
        size = self._HEADER_SIZE + nf * self._PER_FREQ + self._window_size
        out = np.empty(size, dtype=np.float64)

        out[0] = self._count
        out[1] = nf
        out[2] = self._sample_rate
        out[3] = self._window_size
        out[4] = self._buffer_pos

        offset = self._HEADER_SIZE
        for i in range(nf):
            out[offset] = self._coeffs[i]
            out[offset + 1] = self._s1[i]
            out[offset + 2] = self._s2[i]
            out[offset + 3] = self._power[i]
            offset += self._PER_FREQ

        out[offset : offset + self._window_size] = self._buffer
        return out

    @classmethod
    def from_array(cls, arr: np.ndarray) -> GoertzelAccumulator:
        """Restore from a serialized float64 array."""
        data = np.asarray(arr, dtype=np.float64)
        count = int(data[0])
        nf = int(data[1])
        sample_rate = float(data[2])
        window_size = int(data[3])
        buffer_pos = int(data[4])

        # Recover frequencies from stored coefficients
        offset = cls._HEADER_SIZE
        frequencies = []
        coeffs = np.empty(nf, dtype=np.float64)
        s1 = np.empty(nf, dtype=np.float64)
        s2 = np.empty(nf, dtype=np.float64)
        power = np.empty(nf, dtype=np.float64)

        for i in range(nf):
            coeffs[i] = data[offset]
            s1[i] = data[offset + 1]
            s2[i] = data[offset + 2]
            power[i] = data[offset + 3]
            # Recover frequency: coeff = 2*cos(2*pi*f/fs) → f = fs * arccos(coeff/2) / (2*pi)
            freq = sample_rate * math.acos(coeffs[i] / 2.0) / (2.0 * math.pi)
            frequencies.append(freq)
            offset += cls._PER_FREQ

        buf = data[offset : offset + window_size].copy()

        obj = cls.__new__(cls)
        obj._frequencies = tuple(frequencies)
        obj._sample_rate = sample_rate
        obj._window_size = window_size
        obj._coeffs = coeffs
        obj._s1 = s1
        obj._s2 = s2
        obj._power = power
        obj._buffer = buf
        obj._buffer_pos = buffer_pos
        obj._count = count
        return obj
