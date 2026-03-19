"""Transfer entropy for causal information flow.

.. deprecated::
    Transfer entropy is marked for removal. It is highly noise-sensitive
    and discretization-dependent, with limited operational benefit for
    GNSS-T VOD monitoring. Use mutual information or cross-correlation
    instead.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass

import numpy as np

from canvod.streamstats._types import (
    DEFAULT_BIVARIATE_N_BINS,
    DEFAULT_TRANSFER_ENTROPY_LAG,
)


@dataclass(frozen=True)
class TransferEntropyResult:
    """Result of transfer entropy computation."""

    transfer_entropy: float  # T_{X→Y} in bits
    reverse_te: float  # T_{Y→X} in bits
    net_transfer: float  # T_{X→Y} - T_{Y→X}
    n_samples: int


def transfer_entropy(
    x: np.ndarray,
    y: np.ndarray,
    lag: int = DEFAULT_TRANSFER_ENTROPY_LAG,
    n_bins: int = DEFAULT_BIVARIATE_N_BINS,
) -> TransferEntropyResult:
    """Compute transfer entropy between two time series.

    .. deprecated::
        Marked for removal. Noise-sensitive with limited operational
        benefit for GNSS-T. Use :func:`mutual_information` instead.

    T_{X→Y} measures the information that past values of X provide about
    the future of Y, beyond what past Y already provides.

    Parameters
    ----------
    x, y : array
        1-D time series of equal length.
    lag : int
        Prediction horizon.
    n_bins : int
        Number of bins for discretization.

    Returns
    -------
    TransferEntropyResult
    """
    warnings.warn(
        "transfer_entropy is deprecated and will be removed in a future release. "
        "Use mutual_information() instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    x_arr = np.asarray(x, dtype=np.float64).ravel()
    y_arr = np.asarray(y, dtype=np.float64).ravel()

    # Filter paired NaNs
    valid = np.isfinite(x_arr) & np.isfinite(y_arr)
    x_arr = x_arr[valid]
    y_arr = y_arr[valid]

    n = len(x_arr)
    if n < lag + 2:
        return TransferEntropyResult(
            transfer_entropy=float("nan"),
            reverse_te=float("nan"),
            net_transfer=float("nan"),
            n_samples=n,
        )

    # Discretize into bins
    x_binned = _discretize(x_arr, n_bins)
    y_binned = _discretize(y_arr, n_bins)

    # Compute T_{X→Y}
    te_xy = _compute_te(x_binned, y_binned, lag, n_bins)
    # Compute T_{Y→X}
    te_yx = _compute_te(y_binned, x_binned, lag, n_bins)

    return TransferEntropyResult(
        transfer_entropy=float(te_xy),
        reverse_te=float(te_yx),
        net_transfer=float(te_xy - te_yx),
        n_samples=n - lag,
    )


def _discretize(values: np.ndarray, n_bins: int) -> np.ndarray:
    """Bin continuous values into discrete symbols [0, n_bins)."""
    vmin, vmax = values.min(), values.max()
    if vmax == vmin:
        return np.zeros(len(values), dtype=np.int64)
    # Scale to [0, n_bins), clamp upper edge
    binned = ((values - vmin) / (vmax - vmin) * n_bins).astype(np.int64)
    np.clip(binned, 0, n_bins - 1, out=binned)
    return binned


def _compute_te(source: np.ndarray, target: np.ndarray, lag: int, n_bins: int) -> float:
    """Compute T_{source→target} using binned conditional entropies.

    T_{X→Y} = H(Y_{t+1} | Y_t) - H(Y_{t+1} | Y_t, X_t)
    """
    n = len(source) - lag

    y_future = target[lag:][:n]
    y_past = target[:n]
    x_past = source[:n]

    # Build 3D histogram: (y_future, y_past, x_past)
    hist_3d = np.zeros((n_bins, n_bins, n_bins), dtype=np.int64)
    np.add.at(hist_3d, (y_future, y_past, x_past), 1)

    # H(Y_{t+1}, Y_t, X_t)
    h_yf_yp_xp = _entropy_from_counts(hist_3d.ravel())

    # H(Y_t, X_t) — marginalize over y_future
    hist_yp_xp = hist_3d.sum(axis=0)
    h_yp_xp = _entropy_from_counts(hist_yp_xp.ravel())

    # H(Y_{t+1}, Y_t) — marginalize over x_past
    hist_yf_yp = hist_3d.sum(axis=2)
    h_yf_yp = _entropy_from_counts(hist_yf_yp.ravel())

    # H(Y_t) — marginalize over y_future and x_past
    hist_yp = hist_3d.sum(axis=(0, 2))
    h_yp = _entropy_from_counts(hist_yp.ravel())

    # T_{X→Y} = H(Y_{t+1}, Y_t) + H(Y_t, X_t) - H(Y_{t+1}, Y_t, X_t) - H(Y_t)
    te = h_yf_yp + h_yp_xp - h_yf_yp_xp - h_yp
    return max(te, 0.0)  # clamp rounding errors


def _entropy_from_counts(counts: np.ndarray) -> float:
    """Shannon entropy from flat count array."""
    c = counts.astype(np.float64)
    total = c.sum()
    if total <= 0:
        return 0.0
    probs = c / total
    nonzero = probs[probs > 0]
    return float(-np.sum(nonzero * np.log2(nonzero)))
