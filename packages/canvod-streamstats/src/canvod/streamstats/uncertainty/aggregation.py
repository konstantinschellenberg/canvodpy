"""Effective sample size and aggregation uncertainty with autocorrelation correction.

Computes n_eff from autocovariance / autocorrelation sequences produced by
:class:`~canvod.streamstats.accumulators.autocovariance.StreamingAutocovariance`
and combines per-observation uncertainties into an aggregate uncertainty
using inverse-variance weighting.

All functions are pure — no internal state.
"""

from __future__ import annotations

import numpy as np


def effective_sample_size(autocorrelations: np.ndarray, n: int) -> float:
    """Effective sample size from an autocorrelation sequence.

    .. math::

        n_{\\text{eff}} = \\frac{n}{1 + 2 \\sum_{\\tau=1}^{k} \\rho(\\tau)}

    The sum is truncated at the first negative autocorrelation (Geyer's
    initial monotone sequence estimator) to avoid noise amplification at
    large lags.

    Parameters
    ----------
    autocorrelations : np.ndarray
        1-D array of autocorrelation coefficients ρ(1), ρ(2), ….
        (lag-0 = 1 should **not** be included.)
    n : int
        Total number of observations.

    Returns
    -------
    float
        Effective sample size (≥ 1).
    """
    autocorrelations = np.asarray(autocorrelations, dtype=np.float64)
    if autocorrelations.size == 0 or n <= 1:
        return float(n)

    # Truncate at first negative value (Geyer's rule)
    neg_idx = np.where(autocorrelations < 0.0)[0]
    if neg_idx.size > 0:
        autocorrelations = autocorrelations[: neg_idx[0]]

    if autocorrelations.size == 0:
        return float(n)

    rho_sum = float(np.sum(autocorrelations))
    denominator = 1.0 + 2.0 * rho_sum
    # Guard against degenerate case where denominator ≤ 0
    if denominator <= 0.0:
        return 1.0
    return max(1.0, n / denominator)


def aggregation_uncertainty(
    sigma_obs: np.ndarray,
    n_eff: float | None = None,
    n: int | None = None,
) -> float:
    """Uncertainty of an inverse-variance weighted mean.

    .. math::

        \\sigma^2 = \\frac{1}{\\sum w_i} \\cdot \\frac{n}{n_{\\text{eff}}}

    where :math:`w_i = 1 / \\sigma_i^2`.

    If *n_eff* is ``None``, observations are assumed independent
    (``n_eff = n``).

    Parameters
    ----------
    sigma_obs : np.ndarray
        1-D array of per-observation standard deviations.
    n_eff : float or None
        Effective sample size (from :func:`effective_sample_size`).
    n : int or None
        Total number of observations.  Defaults to ``len(sigma_obs)``.

    Returns
    -------
    float
        Standard deviation of the weighted mean.
    """
    sigma_obs = np.asarray(sigma_obs, dtype=np.float64)
    valid = sigma_obs > 0.0
    sigma_valid = sigma_obs[valid]

    if sigma_valid.size == 0:
        return float("nan")

    if n is None:
        n = int(sigma_valid.size)
    if n_eff is None:
        n_eff = float(n)

    weights_sum = float(np.sum(1.0 / sigma_valid**2))
    if weights_sum <= 0.0:
        return float("nan")

    variance = (1.0 / weights_sum) * (n / n_eff)
    return float(np.sqrt(variance))


def effective_sample_size_from_autocovariance(
    autocovariance: np.ndarray,
) -> float:
    """Convenience: normalise autocovariance to autocorrelation, then compute n_eff.

    Parameters
    ----------
    autocovariance : np.ndarray
        1-D array of autocovariance values γ(0), γ(1), γ(2), ….
        The first element γ(0) is the variance.

    Returns
    -------
    float
        Effective sample size (≥ 1).
    """
    autocovariance = np.asarray(autocovariance, dtype=np.float64)
    if autocovariance.size < 2:
        return 1.0

    gamma_0 = autocovariance[0]
    if gamma_0 <= 0.0:
        return 1.0

    autocorrelations = autocovariance[1:] / gamma_0
    # n is unknown from autocovariance alone; use length of the full sequence
    # as a proxy (caller should use effective_sample_size directly when n is
    # known).  A reasonable heuristic is n ≫ max_lag.
    n = len(autocovariance)
    return effective_sample_size(autocorrelations, n)
