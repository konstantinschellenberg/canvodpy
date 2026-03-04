"""Detrended Fluctuation Analysis (DFA) and Hurst exponent."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from canvod.streamstats._types import (
    DEFAULT_DFA_MAX_SCALE,
    DEFAULT_DFA_MIN_SCALE,
    DEFAULT_DFA_N_SCALES,
    DEFAULT_DFA_ORDER,
)


@dataclass(frozen=True)
class DFAResult:
    """Result of detrended fluctuation analysis."""

    alpha: float  # DFA scaling exponent
    scales: np.ndarray  # (n_scales,) segment lengths
    fluctuations: np.ndarray  # (n_scales,) RMS fluctuation F(s)
    r_squared: float  # goodness of log-log fit
    behavior: (
        str  # "uncorrelated"/"persistent"/"flicker"/"non_stationary"/"anti_persistent"
    )


def dfa(
    values: np.ndarray,
    order: int = DEFAULT_DFA_ORDER,
    min_scale: int = DEFAULT_DFA_MIN_SCALE,
    max_scale: int | None = DEFAULT_DFA_MAX_SCALE,
    n_scales: int = DEFAULT_DFA_N_SCALES,
) -> DFAResult:
    """Compute Detrended Fluctuation Analysis scaling exponent.

    Parameters
    ----------
    values : array
        1-D time series.
    order : int
        Polynomial order for local detrending.
    min_scale : int
        Minimum segment length.
    max_scale : int, optional
        Maximum segment length. None → N // 4.
    n_scales : int
        Number of log-spaced scales.

    Returns
    -------
    DFAResult
    """
    v = np.asarray(values, dtype=np.float64).ravel()
    v = v[np.isfinite(v)]
    N = len(v)

    if N < 2 * min_scale:
        empty = np.array([], dtype=np.float64)
        return DFAResult(
            alpha=float("nan"),
            scales=empty,
            fluctuations=empty,
            r_squared=float("nan"),
            behavior="insufficient_data",
        )

    # Step 1: Compute profile (cumulative sum of deviations)
    profile = np.cumsum(v - np.mean(v))

    # Step 2: Generate log-spaced scales
    if max_scale is None:
        max_scale = N // 4
    max_scale = max(max_scale, min_scale + 1)
    scales = np.unique(
        np.logspace(np.log10(min_scale), np.log10(max_scale), n_scales).astype(np.int64)
    )
    scales = scales[scales >= min_scale]
    scales = scales[scales <= N // 2]  # need at least 2 segments

    if len(scales) < 2:
        empty = np.array([], dtype=np.float64)
        return DFAResult(
            alpha=float("nan"),
            scales=empty,
            fluctuations=empty,
            r_squared=float("nan"),
            behavior="insufficient_data",
        )

    # Step 3: Compute fluctuation for each scale
    fluctuations = np.zeros(len(scales), dtype=np.float64)
    for i, s in enumerate(scales):
        n_segments = N // s
        if n_segments < 1:
            fluctuations[i] = np.nan
            continue

        # Reshape profile into segments: (n_seg, s)
        segments = profile[: n_segments * s].reshape(n_segments, s)
        # Build Vandermonde matrix for polynomial of given order
        x_axis = np.arange(s, dtype=np.float64)
        V = np.vander(x_axis, order + 1)  # (s, order+1)
        # Solve all segments at once via least-squares
        coeffs, _, _, _ = np.linalg.lstsq(V, segments.T, rcond=None)  # (order+1, n_seg)
        trends = V @ coeffs  # (s, n_seg)
        residuals = segments.T - trends  # (s, n_seg)
        fluctuations[i] = np.sqrt(np.mean(residuals**2))

    # Remove invalid entries
    valid = np.isfinite(fluctuations) & (fluctuations > 0)
    if valid.sum() < 2:
        return DFAResult(
            alpha=float("nan"),
            scales=scales,
            fluctuations=fluctuations,
            r_squared=float("nan"),
            behavior="insufficient_data",
        )

    # Step 4: Linear fit in log-log space
    log_scales = np.log(scales[valid].astype(np.float64))
    log_fluct = np.log(fluctuations[valid])

    coeffs = np.polyfit(log_scales, log_fluct, 1)
    alpha = float(coeffs[0])

    # R²
    predicted = np.polyval(coeffs, log_scales)
    ss_res = np.sum((log_fluct - predicted) ** 2)
    ss_tot = np.sum((log_fluct - np.mean(log_fluct)) ** 2)
    r_squared = float(1.0 - ss_res / ss_tot) if ss_tot > 0 else 0.0

    behavior = _classify_behavior(alpha)

    return DFAResult(
        alpha=alpha,
        scales=scales,
        fluctuations=fluctuations,
        r_squared=r_squared,
        behavior=behavior,
    )


def hurst_exponent(
    alpha: float | None = None,
    spectral_slope: float | None = None,
) -> float:
    """Compute Hurst exponent from DFA alpha or spectral slope beta.

    H = alpha (from DFA), or H = (beta + 1) / 2 (from PSD slope).

    Exactly one of ``alpha`` or ``spectral_slope`` must be provided.
    """
    if alpha is not None and spectral_slope is not None:
        msg = "Provide exactly one of alpha or spectral_slope"
        raise ValueError(msg)
    if alpha is not None:
        return float(alpha)
    if spectral_slope is not None:
        return (spectral_slope + 1.0) / 2.0
    msg = "Provide exactly one of alpha or spectral_slope"
    raise ValueError(msg)


def _classify_behavior(alpha: float) -> str:
    """Classify DFA scaling exponent."""
    if alpha < 0.4:
        return "anti_persistent"
    if alpha < 0.6:
        return "uncorrelated"
    if alpha < 0.9:
        return "persistent"
    if alpha < 1.1:
        return "flicker"
    return "non_stationary"
