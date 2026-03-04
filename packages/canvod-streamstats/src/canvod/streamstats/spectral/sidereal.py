"""Sidereal filtering for multipath mitigation."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from canvod.streamstats._types import SIDEREAL_DAY_SECONDS


@dataclass(frozen=True)
class SiderealFilterResult:
    """Result of sidereal filtering."""

    times: np.ndarray  # timestamps of filtered output
    residual: np.ndarray  # current - previous sidereal day
    previous_day: np.ndarray  # interpolated previous day values
    correlation: float  # Pearson correlation between current and previous day


def sidereal_filter(
    times: np.ndarray,
    values: np.ndarray,
    sidereal_offset: float = SIDEREAL_DAY_SECONDS,
    interpolation: str = "linear",
) -> SiderealFilterResult:
    """Subtract previous sidereal day's signal to remove static multipath.

    Parameters
    ----------
    times : array
        Timestamps in seconds (e.g. GPS seconds or Unix time).
    values : array
        Observations at each timestamp.
    sidereal_offset : float
        Offset in seconds between repeating sidereal passes (default: mean
        sidereal day ≈ 86164.09 s).
    interpolation : str
        Interpolation method. Currently only ``"linear"`` is supported.

    Returns
    -------
    SiderealFilterResult
    """
    t = np.asarray(times, dtype=np.float64).ravel()
    v = np.asarray(values, dtype=np.float64).ravel()

    # Filter NaN
    mask = np.isfinite(t) & np.isfinite(v)
    t_clean = t[mask]
    v_clean = v[mask]

    if len(t_clean) < 2:
        empty = np.array([], dtype=np.float64)
        return SiderealFilterResult(
            times=empty,
            residual=empty,
            previous_day=empty,
            correlation=float("nan"),
        )

    # Compute shifted times for previous sidereal day
    t_prev = t_clean - sidereal_offset

    # Interpolate previous-day values at shifted timestamps
    prev_values = np.interp(t_prev, t_clean, v_clean, left=np.nan, right=np.nan)

    # Only keep points where interpolation succeeded (within data range)
    valid = np.isfinite(prev_values)
    t_out = t_clean[valid]
    v_out = v_clean[valid]
    prev_out = prev_values[valid]

    # Residual
    residual = v_out - prev_out

    # Pearson correlation
    if len(v_out) < 2:
        corr = float("nan")
    else:
        v_mean = np.mean(v_out)
        p_mean = np.mean(prev_out)
        cov = np.mean((v_out - v_mean) * (prev_out - p_mean))
        std_v = np.std(v_out, ddof=0)
        std_p = np.std(prev_out, ddof=0)
        if std_v > 0 and std_p > 0:
            corr = float(cov / (std_v * std_p))
        else:
            corr = float("nan")

    return SiderealFilterResult(
        times=t_out,
        residual=residual,
        previous_day=prev_out,
        correlation=corr,
    )
