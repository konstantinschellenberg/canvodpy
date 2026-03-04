"""Shannon, joint, and conditional entropy computations."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from canvod.streamstats.accumulators.histogram import StreamingHistogram


def shannon_entropy(counts: np.ndarray) -> float:
    """Shannon entropy H = -sum(p_i * log2(p_i)) from histogram bin counts.

    Parameters
    ----------
    counts : array
        1-D array of non-negative integer counts.

    Returns
    -------
    float
        Entropy in bits.
    """
    c = np.asarray(counts, dtype=np.float64).ravel()
    total = c.sum()
    if total <= 0:
        return 0.0
    probs = c / total
    nonzero = probs[probs > 0]
    return float(-np.sum(nonzero * np.log2(nonzero)))


def shannon_entropy_from_histogram(histogram: StreamingHistogram) -> float:
    """Convenience: extract counts from StreamingHistogram, compute H."""
    return shannon_entropy(histogram.counts)


def joint_entropy(joint_counts: np.ndarray) -> float:
    """H(X,Y) = -sum(p_ij * log2(p_ij)) from 2D count matrix.

    Parameters
    ----------
    joint_counts : array
        2-D array of non-negative integer counts.

    Returns
    -------
    float
        Joint entropy in bits.
    """
    c = np.asarray(joint_counts, dtype=np.float64)
    total = c.sum()
    if total <= 0:
        return 0.0
    probs = c / total
    nonzero = probs[probs > 0]
    return float(-np.sum(nonzero * np.log2(nonzero)))


def conditional_entropy(joint_counts: np.ndarray) -> float:
    """H(Y|X) = H(X,Y) - H(X) from a 2D count matrix.

    Parameters
    ----------
    joint_counts : array
        2-D array of shape (n_x, n_y) with non-negative counts.

    Returns
    -------
    float
        Conditional entropy H(Y|X) in bits.
    """
    c = np.asarray(joint_counts, dtype=np.float64)
    h_xy = joint_entropy(c)
    marginal_x = c.sum(axis=1)
    h_x = shannon_entropy(marginal_x)
    return h_xy - h_x
