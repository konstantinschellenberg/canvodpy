"""Mutual information between two variables."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np

from canvod.streamstats._types import DEFAULT_BIVARIATE_N_BINS
from canvod.streamstats.information.entropy import joint_entropy, shannon_entropy

if TYPE_CHECKING:
    from canvod.streamstats.accumulators.bivariate_histogram import (
        BivariateHistogram,
    )


@dataclass(frozen=True)
class MutualInformationResult:
    """Result of mutual information computation."""

    mutual_information: float  # I(X;Y) in bits
    entropy_x: float  # H(X)
    entropy_y: float  # H(Y)
    joint_entropy: float  # H(X,Y)
    normalized_mi: float  # I(X;Y) / min(H(X), H(Y))
    n_samples: int


def mutual_information(
    x: np.ndarray,
    y: np.ndarray,
    n_bins: int = DEFAULT_BIVARIATE_N_BINS,
) -> MutualInformationResult:
    """Compute mutual information I(X;Y) between two signals.

    Parameters
    ----------
    x, y : array
        1-D arrays of equal length.
    n_bins : int
        Number of bins per axis for the 2D histogram.

    Returns
    -------
    MutualInformationResult
    """
    x_arr = np.asarray(x, dtype=np.float64).ravel()
    y_arr = np.asarray(y, dtype=np.float64).ravel()

    valid = np.isfinite(x_arr) & np.isfinite(y_arr)
    x_arr = x_arr[valid]
    y_arr = y_arr[valid]
    n = len(x_arr)

    if n < 2:
        return MutualInformationResult(
            mutual_information=float("nan"),
            entropy_x=float("nan"),
            entropy_y=float("nan"),
            joint_entropy=float("nan"),
            normalized_mi=float("nan"),
            n_samples=n,
        )

    # Build 2D histogram
    joint_counts, _, _ = np.histogram2d(x_arr, y_arr, bins=n_bins)
    return _mi_from_counts(joint_counts, n)


def mutual_information_from_histogram(
    biv_hist: BivariateHistogram,
) -> MutualInformationResult:
    """Compute mutual information from a BivariateHistogram accumulator."""
    return _mi_from_counts(biv_hist.counts, biv_hist.total)


def _mi_from_counts(
    joint_counts: np.ndarray, n_samples: int
) -> MutualInformationResult:
    """Compute MI from a 2D count matrix."""
    h_xy = joint_entropy(joint_counts)
    marginal_x = joint_counts.sum(axis=1)
    marginal_y = joint_counts.sum(axis=0)
    h_x = shannon_entropy(marginal_x)
    h_y = shannon_entropy(marginal_y)

    mi = h_x + h_y - h_xy
    mi = max(mi, 0.0)  # clamp rounding errors

    min_h = min(h_x, h_y)
    normalized = mi / min_h if min_h > 0 else 0.0

    return MutualInformationResult(
        mutual_information=mi,
        entropy_x=h_x,
        entropy_y=h_y,
        joint_entropy=h_xy,
        normalized_mi=normalized,
        n_samples=n_samples,
    )
