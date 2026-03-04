"""Gaussian mixture model fitting from histogram summaries via EM.

Operates on bin centers (midpoints of edges) weighted by counts, enabling
mixture decomposition from streaming histograms without access to raw data.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np

from canvod.streamstats._types import (
    DEFAULT_MIXTURE_MAX_ITER,
    DEFAULT_MIXTURE_MIN_WEIGHT,
    DEFAULT_MIXTURE_N_COMPONENTS,
    DEFAULT_MIXTURE_TOL,
)

if TYPE_CHECKING:
    from canvod.streamstats.accumulators.histogram import StreamingHistogram


@dataclass(frozen=True)
class GaussianMixtureResult:
    """Result of Gaussian mixture EM fitting."""

    weights: np.ndarray  # (K,), sums to 1
    means: np.ndarray  # (K,)
    stds: np.ndarray  # (K,)
    log_likelihood: float
    n_iterations: int
    converged: bool
    bic: float  # -2·LL + p·log(N), p=3K-1
    n_samples: int


def fit_gaussian_mixture(
    bin_edges: np.ndarray,
    counts: np.ndarray,
    n_components: int = DEFAULT_MIXTURE_N_COMPONENTS,
    max_iter: int = DEFAULT_MIXTURE_MAX_ITER,
    tol: float = DEFAULT_MIXTURE_TOL,
    min_weight: float = DEFAULT_MIXTURE_MIN_WEIGHT,
) -> GaussianMixtureResult:
    """Fit a Gaussian mixture model to histogram bin counts via EM.

    Parameters
    ----------
    bin_edges : array of shape (n_bins+1,)
        Histogram bin edges.
    counts : array of shape (n_bins,)
        Counts per bin.
    n_components : int
        Number of Gaussian components (K).
    max_iter : int
        Maximum EM iterations.
    tol : float
        Convergence tolerance on log-likelihood change.
    min_weight : float
        Minimum component weight to prevent degenerate components.

    Returns
    -------
    GaussianMixtureResult
    """
    bin_edges = np.asarray(bin_edges, dtype=np.float64)
    counts = np.asarray(counts, dtype=np.float64)

    n_bins = len(counts)
    n_total = np.sum(counts)

    # Handle empty histogram
    if n_total == 0 or n_bins == 0:
        K = n_components
        return GaussianMixtureResult(
            weights=np.full(K, np.nan),
            means=np.full(K, np.nan),
            stds=np.full(K, np.nan),
            log_likelihood=float("nan"),
            n_iterations=0,
            converged=False,
            bic=float("nan"),
            n_samples=0,
        )

    # Bin centers as data points, weighted by counts
    centers = 0.5 * (bin_edges[:-1] + bin_edges[1:])

    # Only use occupied bins for initialisation range
    occupied = counts > 0
    if not np.any(occupied):
        K = n_components
        return GaussianMixtureResult(
            weights=np.full(K, np.nan),
            means=np.full(K, np.nan),
            stds=np.full(K, np.nan),
            log_likelihood=float("nan"),
            n_iterations=0,
            converged=False,
            bic=float("nan"),
            n_samples=0,
        )

    lo = centers[occupied].min()
    hi = centers[occupied].max()
    bin_width = bin_edges[1] - bin_edges[0]

    K = n_components

    # Initialise: evenly-spaced means, uniform weights, std = range/K
    if K == 1:
        means = np.array([0.5 * (lo + hi)])
    else:
        means = np.linspace(lo, hi, K)

    span = max(hi - lo, bin_width)
    stds = np.full(K, span / max(K, 1))
    weights = np.full(K, 1.0 / K)

    prev_ll = -np.inf
    converged = False
    n_iter = 0

    # Pre-compute constant
    _log2pi = math.log(2.0 * math.pi)

    # Broadcast-friendly shapes: means/stds/weights as (K,1), centers as (1,n_bins)
    centers_row = centers[np.newaxis, :]  # (1, n_bins)
    counts_row = counts[np.newaxis, :]  # (1, n_bins)

    for iteration in range(max_iter):
        # --- E-step: vectorised over K and n_bins ---
        safe_stds = np.maximum(stds, 1e-300)  # (K,)
        diff = centers_row - means[:, np.newaxis]  # (K, n_bins)
        log_norm = (
            -0.5 * (diff / safe_stds[:, np.newaxis]) ** 2
            - np.log(safe_stds)[:, np.newaxis]
            - 0.5 * _log2pi
        )  # (K, n_bins)
        resp = weights[:, np.newaxis] * np.exp(log_norm)  # (K, n_bins)

        # Weighted by counts
        resp_weighted = resp * counts_row  # (K, n_bins)

        # Normalise responsibilities per bin
        resp_sum = resp_weighted.sum(axis=0)  # (n_bins,)
        resp_sum = np.maximum(resp_sum, 1e-300)
        resp_norm = resp_weighted / resp_sum[np.newaxis, :]  # (K, n_bins)

        # --- Log-likelihood ---
        mask = resp_sum > 1e-300
        ll = float(np.sum(counts[mask] * np.log(resp_sum[mask])))
        n_iter = iteration + 1

        if abs(ll - prev_ll) < tol:
            converged = True
            break
        prev_ll = ll

        # --- M-step: vectorised over K ---
        nk = np.sum(resp_norm * counts_row, axis=1)  # (K,)
        active = nk >= min_weight

        # Update active components
        new_means = np.where(
            active,
            np.sum(resp_norm * counts_row * centers_row, axis=1)
            / np.maximum(nk, 1e-300),
            means,
        )
        diff_m = centers_row - new_means[:, np.newaxis]  # (K, n_bins)
        var_k = np.sum(resp_norm * counts_row * diff_m * diff_m, axis=1) / np.maximum(
            nk, 1e-300
        )
        new_stds = np.where(active, np.sqrt(np.maximum(var_k, 1e-300)), stds)
        new_weights = np.where(active, nk / n_total, min_weight)

        means = new_means
        stds = new_stds
        weights = np.maximum(new_weights, min_weight)
        weights /= weights.sum()

    # Final log-likelihood (vectorised)
    safe_stds = np.maximum(stds, 1e-300)
    diff = centers_row - means[:, np.newaxis]
    log_norm = (
        -0.5 * (diff / safe_stds[:, np.newaxis]) ** 2
        - np.log(safe_stds)[:, np.newaxis]
        - 0.5 * _log2pi
    )
    resp_final = weights[:, np.newaxis] * np.exp(log_norm)

    mix_pdf = resp_final.sum(axis=0)
    mix_pdf = np.maximum(mix_pdf, 1e-300)
    final_ll = float(np.sum(counts * np.log(mix_pdf)))

    # BIC: -2·LL + p·log(N), p = 3K - 1 (K means + K stds + K-1 free weights)
    p = 3 * K - 1
    bic = -2.0 * final_ll + p * math.log(max(n_total, 1))

    # Sort components by mean
    order = np.argsort(means)

    return GaussianMixtureResult(
        weights=weights[order].copy(),
        means=means[order].copy(),
        stds=stds[order].copy(),
        log_likelihood=final_ll,
        n_iterations=n_iter,
        converged=converged,
        bic=bic,
        n_samples=int(n_total),
    )


def fit_gaussian_mixture_from_histogram(
    histogram: StreamingHistogram,
    n_components: int = DEFAULT_MIXTURE_N_COMPONENTS,
    max_iter: int = DEFAULT_MIXTURE_MAX_ITER,
    tol: float = DEFAULT_MIXTURE_TOL,
    min_weight: float = DEFAULT_MIXTURE_MIN_WEIGHT,
) -> GaussianMixtureResult:
    """Fit Gaussian mixture from a StreamingHistogram.

    Convenience wrapper that extracts bin_edges and counts.
    """
    return fit_gaussian_mixture(
        bin_edges=histogram.bin_edges,
        counts=histogram.counts,
        n_components=n_components,
        max_iter=max_iter,
        tol=tol,
        min_weight=min_weight,
    )
