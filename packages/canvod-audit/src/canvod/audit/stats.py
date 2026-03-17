"""Statistical comparison functions for paired arrays.

All functions operate on flat numpy arrays and handle NaN masking
consistently: statistics are computed only over mutually non-NaN values.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np


@dataclass(frozen=True)
class VariableStats:
    """Per-variable comparison statistics."""

    name: str
    rmse: float
    bias: float
    mae: float
    max_abs_diff: float
    correlation: float
    nan_agreement_rate: float
    n_compared: int
    n_total: int
    n_nan_a: int
    n_nan_b: int
    pct_nan_a: float
    pct_nan_b: float

    def as_dict(self) -> dict[str, Any]:
        """Flat dict for DataFrame construction."""
        return {
            "variable": self.name,
            "rmse": round(self.rmse, 6),
            "bias": round(self.bias, 6),
            "mae": round(self.mae, 6),
            "max_abs_diff": round(self.max_abs_diff, 6),
            "correlation": round(self.correlation, 6),
            "nan_agreement": round(self.nan_agreement_rate, 4),
            "n_compared": self.n_compared,
            "n_total": self.n_total,
            "pct_nan_a": round(self.pct_nan_a, 4),
            "pct_nan_b": round(self.pct_nan_b, 4),
        }


def _valid_mask(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Boolean mask where both a and b are finite (not NaN/Inf)."""
    return np.isfinite(a) & np.isfinite(b)


def rmse(a: np.ndarray, b: np.ndarray) -> float:
    """Root Mean Square Error over mutually valid elements."""
    mask = _valid_mask(a, b)
    if not mask.any():
        return float("nan")
    diff = a[mask] - b[mask]
    return float(np.sqrt(np.mean(diff**2)))


def bias(a: np.ndarray, b: np.ndarray) -> float:
    """Mean difference (a - b) over mutually valid elements."""
    mask = _valid_mask(a, b)
    if not mask.any():
        return float("nan")
    return float(np.mean(a[mask] - b[mask]))


def mae(a: np.ndarray, b: np.ndarray) -> float:
    """Mean Absolute Error over mutually valid elements."""
    mask = _valid_mask(a, b)
    if not mask.any():
        return float("nan")
    return float(np.mean(np.abs(a[mask] - b[mask])))


def max_abs_diff(a: np.ndarray, b: np.ndarray) -> float:
    """Maximum absolute difference over mutually valid elements."""
    mask = _valid_mask(a, b)
    if not mask.any():
        return float("nan")
    return float(np.max(np.abs(a[mask] - b[mask])))


def correlation(a: np.ndarray, b: np.ndarray) -> float:
    """Pearson correlation over mutually valid elements."""
    mask = _valid_mask(a, b)
    if mask.sum() < 2:
        return float("nan")
    r = np.corrcoef(a[mask], b[mask])
    return float(r[0, 1])


def nan_agreement(a: np.ndarray, b: np.ndarray) -> float:
    """Fraction of elements where NaN status agrees (both NaN or both finite)."""
    nan_a = np.isnan(a)
    nan_b = np.isnan(b)
    agree = nan_a == nan_b
    return float(agree.mean())


def compute_variable_stats(
    name: str,
    a: np.ndarray,
    b: np.ndarray,
) -> VariableStats:
    """Compute all comparison statistics for a single variable.

    Parameters
    ----------
    name : str
        Variable name.
    a, b : np.ndarray
        Flat arrays of equal length to compare.
    """
    a = a.ravel().astype(np.float64)
    b = b.ravel().astype(np.float64)

    n_total = len(a)
    n_nan_a = int(np.isnan(a).sum())
    n_nan_b = int(np.isnan(b).sum())
    mask = _valid_mask(a, b)
    n_compared = int(mask.sum())

    return VariableStats(
        name=name,
        rmse=rmse(a, b),
        bias=bias(a, b),
        mae=mae(a, b),
        max_abs_diff=max_abs_diff(a, b),
        correlation=correlation(a, b),
        nan_agreement_rate=nan_agreement(a, b),
        n_compared=n_compared,
        n_total=n_total,
        n_nan_a=n_nan_a,
        n_nan_b=n_nan_b,
        pct_nan_a=n_nan_a / n_total if n_total > 0 else 0.0,
        pct_nan_b=n_nan_b / n_total if n_total > 0 else 0.0,
    )
