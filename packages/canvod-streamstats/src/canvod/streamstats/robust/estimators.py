"""Robust location and scale estimators — pure functions, no classes."""

from __future__ import annotations

import numpy as np

from canvod.streamstats._types import DEFAULT_MAD_SCALE_FACTOR, DEFAULT_TRIM_FRACTION


def mad(values: np.ndarray) -> float:
    """Median Absolute Deviation: median(|x_i - median(x)|).

    Parameters
    ----------
    values : array-like
        Input data. NaN values are ignored.

    Returns
    -------
    float
        The MAD. Returns NaN if fewer than 1 finite value.
    """
    arr = np.asarray(values, dtype=np.float64).ravel()
    arr = arr[~np.isnan(arr)]
    if len(arr) == 0:
        return np.nan
    median = float(np.median(arr))
    return float(np.median(np.abs(arr - median)))


def robust_std(
    values: np.ndarray,
    scale_factor: float = DEFAULT_MAD_SCALE_FACTOR,
) -> float:
    """Robust standard deviation estimate: scale_factor * MAD.

    With the default scale_factor of 1.4826, this is a consistent
    estimator of the standard deviation for Gaussian data.

    Parameters
    ----------
    values : array-like
        Input data. NaN values are ignored.
    scale_factor : float
        Multiplier for MAD (default 1.4826 for Gaussian consistency).

    Returns
    -------
    float
        Robust std estimate. Returns NaN if fewer than 1 finite value.
    """
    return scale_factor * mad(values)


def trimmed_mean(
    values: np.ndarray,
    alpha: float = DEFAULT_TRIM_FRACTION,
) -> float:
    """Mean of values within [alpha, 1-alpha] quantiles.

    Parameters
    ----------
    values : array-like
        Input data. NaN values are ignored.
    alpha : float
        Fraction to trim from each tail. Must be in [0, 0.5).

    Returns
    -------
    float
        Trimmed mean. Returns NaN if no valid values remain after trimming.

    Raises
    ------
    ValueError
        If alpha is not in [0, 0.5).
    """
    if not 0.0 <= alpha < 0.5:
        msg = f"alpha must be in [0, 0.5), got {alpha}"
        raise ValueError(msg)

    arr = np.asarray(values, dtype=np.float64).ravel()
    arr = arr[~np.isnan(arr)]
    if len(arr) == 0:
        return np.nan

    if alpha == 0.0:
        return float(np.mean(arr))

    sorted_arr = np.sort(arr)
    n = len(sorted_arr)
    lo = int(np.floor(n * alpha))
    hi = n - lo
    if hi <= lo:
        return np.nan
    return float(np.mean(sorted_arr[lo:hi]))
