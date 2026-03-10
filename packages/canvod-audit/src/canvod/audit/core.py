"""Core comparison engine for canvod-audit.

The main entry point is ``compare_datasets()``, which aligns two xarray
Datasets on shared coordinates and computes per-variable statistics
with configurable tolerance tiers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np

from canvod.audit.stats import VariableStats, compute_variable_stats
from canvod.audit.tolerances import (
    Tolerance,
    ToleranceTier,
    get_tolerance,
)


@dataclass(frozen=True)
class ComparisonResult:
    """Result of comparing two datasets.

    Attributes
    ----------
    label : str
        Human-readable label for this comparison.
    variable_stats : dict[str, VariableStats]
        Per-variable statistics.
    tier : ToleranceTier
        Tolerance tier used.
    passed : bool
        True if all variables are within tolerance.
    failures : dict[str, str]
        Variables that failed, with a reason string.
    metadata : dict[str, Any]
        Free-form metadata (source paths, timestamps, configs).
    alignment : AlignmentInfo
        Information about coordinate alignment.
    """

    label: str
    variable_stats: dict[str, VariableStats]
    tier: ToleranceTier
    passed: bool
    failures: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    alignment: AlignmentInfo | None = None

    def to_polars(self) -> Any:
        """Return per-variable stats as a polars DataFrame."""
        import polars as pl

        rows = [vs.as_dict() for vs in self.variable_stats.values()]
        if not rows:
            return pl.DataFrame(schema={"variable": pl.Utf8})
        return pl.DataFrame(rows)

    def summary(self) -> str:
        """Human-readable summary string."""
        status = "PASSED" if self.passed else "FAILED"
        lines = [
            f"Comparison: {self.label}",
            f"Tier: {self.tier.value} | Status: {status}",
            f"Variables compared: {len(self.variable_stats)}",
        ]
        if self.alignment:
            lines.append(
                f"Aligned on: {self.alignment.n_shared_epochs} epochs, "
                f"{self.alignment.n_shared_sids} sids "
                f"(dropped {self.alignment.n_dropped_epochs_a + self.alignment.n_dropped_epochs_b} epochs, "
                f"{self.alignment.n_dropped_sids_a + self.alignment.n_dropped_sids_b} sids)"
            )
        if self.failures:
            lines.append("Failures:")
            for var, reason in self.failures.items():
                lines.append(f"  {var}: {reason}")
        return "\n".join(lines)


@dataclass(frozen=True)
class AlignmentInfo:
    """Information about how two datasets were aligned."""

    n_shared_epochs: int
    n_shared_sids: int
    n_epochs_a: int
    n_epochs_b: int
    n_sids_a: int
    n_sids_b: int

    @property
    def n_dropped_epochs_a(self) -> int:
        return self.n_epochs_a - self.n_shared_epochs

    @property
    def n_dropped_epochs_b(self) -> int:
        return self.n_epochs_b - self.n_shared_epochs

    @property
    def n_dropped_sids_a(self) -> int:
        return self.n_sids_a - self.n_shared_sids

    @property
    def n_dropped_sids_b(self) -> int:
        return self.n_sids_b - self.n_shared_sids


def _align_datasets(
    ds_a: Any,
    ds_b: Any,
) -> tuple[Any, Any, AlignmentInfo]:
    """Align two datasets on the intersection of epoch and sid coordinates."""
    info_kwargs: dict[str, int] = {}

    info_kwargs["n_epochs_a"] = len(ds_a.epoch) if "epoch" in ds_a.dims else 0
    info_kwargs["n_epochs_b"] = len(ds_b.epoch) if "epoch" in ds_b.dims else 0
    info_kwargs["n_sids_a"] = len(ds_a.sid) if "sid" in ds_a.dims else 0
    info_kwargs["n_sids_b"] = len(ds_b.sid) if "sid" in ds_b.dims else 0

    # Align on epoch
    if "epoch" in ds_a.dims and "epoch" in ds_b.dims:
        shared_epochs = np.intersect1d(ds_a.epoch.values, ds_b.epoch.values)
        ds_a = ds_a.sel(epoch=shared_epochs)
        ds_b = ds_b.sel(epoch=shared_epochs)
        info_kwargs["n_shared_epochs"] = len(shared_epochs)
    else:
        info_kwargs["n_shared_epochs"] = 0

    # Align on sid
    if "sid" in ds_a.dims and "sid" in ds_b.dims:
        shared_sids = np.intersect1d(ds_a.sid.values, ds_b.sid.values)
        ds_a = ds_a.sel(sid=shared_sids)
        ds_b = ds_b.sel(sid=shared_sids)
        info_kwargs["n_shared_sids"] = len(shared_sids)
    else:
        info_kwargs["n_shared_sids"] = 0

    return ds_a, ds_b, AlignmentInfo(**info_kwargs)


def _check_tolerance(
    vs: VariableStats,
    tol: Tolerance,
) -> str | None:
    """Check if a variable's stats are within tolerance. Returns failure reason or None."""
    if vs.max_abs_diff > tol.atol and tol.atol > 0:
        # Also check relative tolerance
        if tol.rtol > 0 and vs.rmse > tol.rtol:
            return (
                f"max_abs_diff={vs.max_abs_diff:.6g} > atol={tol.atol:.6g}, "
                f"rmse={vs.rmse:.6g} > rtol={tol.rtol:.6g}"
            )
        if tol.rtol == 0:
            return f"max_abs_diff={vs.max_abs_diff:.6g} > atol={tol.atol:.6g}"

    if tol.atol == 0 and tol.rtol == 0 and vs.max_abs_diff > 0:
        return f"not bit-identical: max_abs_diff={vs.max_abs_diff:.6g}"

    nan_diff = abs(vs.pct_nan_a - vs.pct_nan_b)
    if nan_diff > tol.nan_rate_atol:
        return (
            f"NaN rate disagreement: {vs.pct_nan_a:.2%} vs {vs.pct_nan_b:.2%} "
            f"(diff={nan_diff:.2%} > {tol.nan_rate_atol:.2%})"
        )

    return None


def compare_datasets(
    ds_a: Any,
    ds_b: Any,
    *,
    variables: list[str] | None = None,
    tier: ToleranceTier = ToleranceTier.NUMERICAL,
    tolerance_overrides: dict[str, Tolerance] | None = None,
    label: str = "",
    align: bool = True,
    metadata: dict[str, Any] | None = None,
) -> ComparisonResult:
    """Compare two xarray Datasets variable-by-variable.

    Parameters
    ----------
    ds_a, ds_b : xarray.Dataset
        Datasets to compare. ``ds_a`` is typically the candidate (canvodpy),
        ``ds_b`` the reference.
    variables : list[str], optional
        Variables to compare. If None, uses the intersection of both datasets'
        data variables.
    tier : ToleranceTier
        Comparison strictness level.
    tolerance_overrides : dict[str, Tolerance], optional
        Per-variable tolerance overrides.
    label : str
        Human-readable label for this comparison.
    align : bool
        If True, align datasets on shared (epoch, sid) coordinates.
    metadata : dict, optional
        Free-form metadata to attach to the result.

    Returns
    -------
    ComparisonResult
    """
    alignment = None
    if align:
        ds_a, ds_b, alignment = _align_datasets(ds_a, ds_b)

    # Determine variables to compare
    if variables is None:
        vars_a = set(ds_a.data_vars)
        vars_b = set(ds_b.data_vars)
        variables = sorted(vars_a & vars_b)

    # Compute per-variable stats
    variable_stats: dict[str, VariableStats] = {}
    failures: dict[str, str] = {}

    for var in variables:
        if var not in ds_a.data_vars or var not in ds_b.data_vars:
            continue

        a_vals = ds_a[var].values.astype(np.float64)
        b_vals = ds_b[var].values.astype(np.float64)

        if a_vals.shape != b_vals.shape:
            failures[var] = f"shape mismatch: {a_vals.shape} vs {b_vals.shape}"
            continue

        vs = compute_variable_stats(var, a_vals, b_vals)
        variable_stats[var] = vs

        tol = get_tolerance(var, tier, tolerance_overrides)
        reason = _check_tolerance(vs, tol)
        if reason:
            failures[var] = reason

    return ComparisonResult(
        label=label or f"{tier.value} comparison",
        variable_stats=variable_stats,
        tier=tier,
        passed=len(failures) == 0,
        failures=failures,
        metadata=metadata or {},
        alignment=alignment,
    )
