"""Lomb-Scargle periodogram for unevenly sampled data."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from scipy.signal import lombscargle


@dataclass(frozen=True)
class LombScargleResult:
    """Result of a Lomb-Scargle periodogram analysis."""

    frequencies: np.ndarray  # (N_freq,) angular frequencies evaluated
    power: np.ndarray  # (N_freq,) normalised spectral power
    peak_frequency: float  # frequency of maximum power
    peak_power: float  # power at peak
    false_alarm_probability: float  # FAP for peak (Baluev 2008 approximation)


def lomb_scargle(
    times: np.ndarray,
    values: np.ndarray,
    min_freq: float | None = None,
    max_freq: float | None = None,
    n_frequencies: int = 1000,
    normalization: str = "standard",
) -> LombScargleResult:
    """Compute the Lomb-Scargle periodogram.

    Parameters
    ----------
    times : array
        Observation timestamps (must be monotonically increasing for best results).
    values : array
        Observation values.
    min_freq, max_freq : float, optional
        Frequency range in Hz.  Defaults are derived from the data span and
        Nyquist-like estimate.
    n_frequencies : int
        Number of angular frequencies to evaluate.
    normalization : str
        ``"standard"`` (default) normalises by residual variance.

    Returns
    -------
    LombScargleResult
    """
    t = np.asarray(times, dtype=np.float64).ravel()
    v = np.asarray(values, dtype=np.float64).ravel()

    # Filter NaN
    mask = np.isfinite(t) & np.isfinite(v)
    t = t[mask]
    v = v[mask]

    if len(t) < 3:
        empty = np.array([], dtype=np.float64)
        return LombScargleResult(
            frequencies=empty,
            power=empty,
            peak_frequency=float("nan"),
            peak_power=float("nan"),
            false_alarm_probability=1.0,
        )

    # Subtract mean for lombscargle (required by scipy)
    v_centered = v - np.mean(v)
    variance = np.var(v, ddof=1)

    # Frequency grid
    T_span = t[-1] - t[0]
    if T_span <= 0:
        empty = np.array([], dtype=np.float64)
        return LombScargleResult(
            frequencies=empty,
            power=empty,
            peak_frequency=float("nan"),
            peak_power=float("nan"),
            false_alarm_probability=1.0,
        )

    if min_freq is None:
        min_freq = 1.0 / T_span
    if max_freq is None:
        # Pseudo-Nyquist from median time step
        dt_median = np.median(np.diff(t))
        max_freq = 0.5 / dt_median if dt_median > 0 else 1.0

    angular_freqs = np.linspace(
        2.0 * np.pi * min_freq,
        2.0 * np.pi * max_freq,
        n_frequencies,
    )

    # Compute periodogram
    raw_power = lombscargle(t, v_centered, angular_freqs)

    # Normalise: standard normalization divides by half the sum of squared
    # centered values, so that power z ranges up to ~N/2 for a pure sinusoid.
    if normalization == "standard":
        ss = 0.5 * np.sum(v_centered**2)
        if ss > 0:
            power = raw_power / ss
        else:
            power = raw_power
    else:
        power = raw_power

    # Peak
    idx_peak = int(np.argmax(power))
    peak_freq = angular_freqs[idx_peak] / (2.0 * np.pi)
    peak_power = float(power[idx_peak])

    # False alarm probability — Baluev (2008) single-frequency approximation
    N = len(t)
    fap = _baluev_fap(peak_power, N, n_frequencies)

    return LombScargleResult(
        frequencies=angular_freqs,
        power=power,
        peak_frequency=peak_freq,
        peak_power=peak_power,
        false_alarm_probability=fap,
    )


def _baluev_fap(z: float, N: int, M: int) -> float:
    """Approximate single-peak FAP via Baluev (2008).

    With the standard normalisation (power divided by ``0.5 * sum(x_centered**2)``),
    the single-frequency survival function is:

        prob_single = (1 - z)^((N - 3) / 2)

    where *z* is the normalised power (range [0, 1]).  The multi-trial FAP is:

        FAP ≈ 1 - (1 - prob_single)^M
    """
    if N <= 3 or z <= 0 or M <= 0:
        return 1.0
    base = 1.0 - z
    if base <= 0:
        # z >= 1 means perfect fit → FAP = 0
        return 0.0
    prob_single = base ** ((N - 3) / 2.0)
    # Bonferroni-like: 1 - (1 - p_single)^M, approximated for numerical stability
    fap = 1.0 - (1.0 - prob_single) ** M
    return min(max(fap, 0.0), 1.0)
