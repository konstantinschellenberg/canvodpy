"""Sample entropy (SampEn) for time series complexity."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from canvod.streamstats._types import (
    DEFAULT_SAMPLE_ENTROPY_M,
    DEFAULT_SAMPLE_ENTROPY_R,
)


@dataclass(frozen=True)
class SampleEntropyResult:
    """Result of sample entropy computation."""

    sample_entropy: float  # SampEn(m, r, N)
    m: int  # template length used
    r: float  # tolerance (absolute)
    n_matches_m: int  # B: template matches of length m
    n_matches_m1: int  # A: template matches of length m+1
    complexity: str  # "regular" / "moderate" / "irregular"


def sample_entropy(
    values: np.ndarray,
    m: int = DEFAULT_SAMPLE_ENTROPY_M,
    r: float | None = None,
) -> SampleEntropyResult:
    """Compute sample entropy of a time series.

    Parameters
    ----------
    values : array
        1-D time series.
    m : int
        Template length (embedding dimension).
    r : float, optional
        Tolerance threshold (absolute). If None, uses DEFAULT_SAMPLE_ENTROPY_R * std(values).

    Returns
    -------
    SampleEntropyResult
    """
    v = np.asarray(values, dtype=np.float64).ravel()
    v = v[np.isfinite(v)]
    N = len(v)

    if N < m + 2:
        return SampleEntropyResult(
            sample_entropy=float("nan"),
            m=m,
            r=0.0 if r is None else r,
            n_matches_m=0,
            n_matches_m1=0,
            complexity="insufficient_data",
        )

    if r is None:
        std = np.std(v, ddof=1)
        r = DEFAULT_SAMPLE_ENTROPY_R * std if std > 0 else 1.0

    # Count template matches at length m and m+1
    n_matches_m = _count_matches(v, m, r)
    n_matches_m1 = _count_matches(v, m + 1, r)

    if n_matches_m == 0:
        sampen = float("inf")
    elif n_matches_m1 == 0:
        sampen = float("inf")
    else:
        sampen = -np.log(n_matches_m1 / n_matches_m)

    # Classify complexity
    if sampen < 0.3:
        complexity = "regular"
    elif sampen < 1.5:
        complexity = "moderate"
    else:
        complexity = "irregular"

    return SampleEntropyResult(
        sample_entropy=float(sampen),
        m=m,
        r=float(r),
        n_matches_m=n_matches_m,
        n_matches_m1=n_matches_m1,
        complexity=complexity,
    )


def _count_matches(v: np.ndarray, m: int, r: float) -> int:
    """Count template matches of length m using max-norm <= r.

    Uses vectorized pairwise distance computation.
    """
    N = len(v)
    n_templates = N - m
    if n_templates < 2:
        return 0

    # Build template matrix (n_templates, m)
    templates = np.lib.stride_tricks.sliding_window_view(v, m)

    count = 0
    for i in range(n_templates):
        # Compute max-norm distance from template i to all j > i
        diffs = np.abs(templates[i + 1 :] - templates[i])
        max_dists = diffs.max(axis=1)
        count += int(np.sum(max_dists <= r))

    return count
