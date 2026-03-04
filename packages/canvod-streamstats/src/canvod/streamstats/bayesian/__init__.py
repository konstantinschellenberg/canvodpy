"""Bayesian inferential extensions for streaming statistics."""

from canvod.streamstats.bayesian.bocpd import BOCPDAccumulator, BOCPDResult
from canvod.streamstats.bayesian.mixture import (
    GaussianMixtureResult,
    fit_gaussian_mixture,
    fit_gaussian_mixture_from_histogram,
)
from canvod.streamstats.bayesian.spatial import (
    CARResult,
    adjacency_from_grid,
    car_smooth,
    icar_smooth,
)

__all__ = [
    "BOCPDAccumulator",
    "BOCPDResult",
    "CARResult",
    "GaussianMixtureResult",
    "adjacency_from_grid",
    "car_smooth",
    "fit_gaussian_mixture",
    "fit_gaussian_mixture_from_histogram",
    "icar_smooth",
]
