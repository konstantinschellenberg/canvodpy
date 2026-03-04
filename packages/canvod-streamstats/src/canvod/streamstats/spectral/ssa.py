"""Singular Spectrum Analysis (SSA) decomposition."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from canvod.streamstats._types import DEFAULT_SSA_N_COMPONENTS, DEFAULT_SSA_WINDOW


@dataclass(frozen=True)
class SSAResult:
    """Result of SSA decomposition."""

    components: np.ndarray  # (n_components, N) reconstructed components
    singular_values: np.ndarray  # (n_components,) singular values
    explained_variance: np.ndarray  # (n_components,) fraction of variance explained
    trend: np.ndarray  # first component (slowly varying trend)


def ssa_decompose(
    values: np.ndarray,
    window: int = DEFAULT_SSA_WINDOW,
    n_components: int = DEFAULT_SSA_N_COMPONENTS,
) -> SSAResult:
    """Perform Singular Spectrum Analysis decomposition.

    Parameters
    ----------
    values : array
        1-D time series of length N.
    window : int
        Embedding dimension L (window length).  Must satisfy 2 <= L <= N/2.
    n_components : int
        Number of components to extract.

    Returns
    -------
    SSAResult
    """
    x = np.asarray(values, dtype=np.float64).ravel()

    # Filter NaN / Inf values
    finite_mask = np.isfinite(x)
    x = x[finite_mask]
    N = len(x)

    if N < 4:
        empty = np.array([], dtype=np.float64)
        return SSAResult(
            components=np.empty((0, N), dtype=np.float64),
            singular_values=empty,
            explained_variance=empty,
            trend=np.full(N, np.nan, dtype=np.float64) if N > 0 else empty,
        )

    # Clamp window to valid range
    L = max(2, min(window, N // 2))
    K = N - L + 1  # number of columns in trajectory matrix

    # Build trajectory matrix using stride tricks (memory efficient)
    X = np.lib.stride_tricks.sliding_window_view(x, L).T  # (L, K)

    # Truncated SVD
    n_components = min(n_components, min(L, K))
    U, s_full, Vt = np.linalg.svd(X, full_matrices=False)

    # Compute total variance from ALL singular values before truncation
    total_variance = np.sum(s_full**2)
    if total_variance <= 0:
        total_variance = 1.0

    U = U[:, :n_components]  # (L, n_comp)
    s = s_full[:n_components]  # (n_comp,)
    Vt = Vt[:n_components, :]  # (n_comp, K)

    # Explained variance uses full spectrum as denominator
    explained_variance = s**2 / total_variance

    # Reconstruct components via diagonal averaging
    components = np.empty((n_components, N), dtype=np.float64)
    for k in range(n_components):
        # Elementary matrix for component k
        elem = np.outer(U[:, k], Vt[k, :]) * s[k]  # (L, K)
        components[k] = _diagonal_average(elem, N, L, K)

    return SSAResult(
        components=components,
        singular_values=s,
        explained_variance=explained_variance,
        trend=components[0],
    )


def _diagonal_average(elem: np.ndarray, N: int, L: int, K: int) -> np.ndarray:
    """Reconstruct a time series from an elementary matrix via anti-diagonal averaging.

    Parameters
    ----------
    elem : (L, K) array
        Elementary matrix (rank-1).
    N : int
        Original time series length.
    L, K : int
        Trajectory matrix dimensions.
    """
    # Vectorised anti-diagonal averaging using np.add.at
    # If L > K, transpose so rows index the shorter dimension
    if L > K:
        elem = elem.T
        L, K = K, L

    rows, cols = np.meshgrid(np.arange(L), np.arange(K), indexing="ij")
    indices = (rows + cols).ravel()

    result = np.zeros(N, dtype=np.float64)
    np.add.at(result, indices, elem.ravel())

    counts = np.zeros(N, dtype=np.float64)
    np.add.at(counts, indices, 1)

    return result / counts
