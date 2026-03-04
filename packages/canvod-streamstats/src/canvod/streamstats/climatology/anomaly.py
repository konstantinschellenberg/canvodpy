"""Anomaly z-score computation and classification."""

from __future__ import annotations

import math

import numpy as np

from canvod.streamstats._types import AnomalyClassification


def anomaly_zscore(value: float, mean: float, std: float) -> float:
    """Compute z = (x - mu) / sigma.

    Returns NaN if *std* <= 0.
    """
    if std <= 0.0 or math.isnan(std):
        return float("nan")
    return (value - mean) / std


def classify_anomaly(z: float) -> AnomalyClassification:
    """Classify by |z|: <1 Normal, <2 Mild, <3 Moderate, >=3 Severe."""
    az = abs(z)
    if math.isnan(az):
        return AnomalyClassification.NORMAL
    if az < 1.0:
        return AnomalyClassification.NORMAL
    if az < 2.0:
        return AnomalyClassification.MILD
    if az < 3.0:
        return AnomalyClassification.MODERATE
    return AnomalyClassification.SEVERE


def anomaly_zscore_batch(
    values: np.ndarray,
    means: np.ndarray,
    stds: np.ndarray,
) -> np.ndarray:
    """Vectorised z-score. Entries with std <= 0 become NaN."""
    values = np.asarray(values, dtype=np.float64)
    means = np.asarray(means, dtype=np.float64)
    stds = np.asarray(stds, dtype=np.float64)

    with np.errstate(divide="ignore", invalid="ignore"):
        z = (values - means) / stds

    # Mask entries where std <= 0
    z[stds <= 0.0] = np.nan
    return z


def classify_anomaly_batch(z_scores: np.ndarray) -> np.ndarray:
    """Vectorised classification -> array of AnomalyClassification string values."""
    z_scores = np.asarray(z_scores, dtype=np.float64)
    az = np.abs(z_scores)
    out = np.full(az.shape, AnomalyClassification.NORMAL, dtype=object)
    out[az >= 1.0] = AnomalyClassification.MILD
    out[az >= 2.0] = AnomalyClassification.MODERATE
    out[az >= 3.0] = AnomalyClassification.SEVERE
    # NaN → NORMAL (abs(NaN) is NaN, so comparisons are False → stays NORMAL)
    return out
