"""Statistical comparison functions for paired arrays.

All functions operate on flat numpy arrays and handle NaN masking
consistently: statistics are computed only over mutually non-NaN values.

This module also provides the observable-difference reporting framework:
``VariableBudget``, ``VarDiffStats``, ``compute_diff_report``, and
``print_diff_report``.  These support the scientific principle that every
observable difference must be reported with actual numbers and annotated
against a physically-grounded expected budget — never hidden in tolerances.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import numpy as np

if TYPE_CHECKING:
    import xarray as xr


# ---------------------------------------------------------------------------
# Observable-difference reporting framework
# ---------------------------------------------------------------------------


@dataclass
class VariableBudget:
    """Physically-grounded expected-difference budget for one observable.

    A budget bounds what differences are admissible between two datasets
    given a documented physical mechanism.  If no mechanism bounds the
    difference (e.g. two fundamentally different algorithms), set
    ``budget=None`` — the difference is then reported but not gated.

    Attributes
    ----------
    unit : str
        Physical unit of the variable (e.g. "dB-Hz", "deg", "m").
    source : str
        One-line citation of the physical mechanism that causes (and bounds)
        the expected difference.
    note : str
        Longer explanation with firmware page references or algorithm details.
    budget : float or None
        Maximum admissible absolute difference.  ``None`` = unbounded.
    vod_relevant : bool
        True if this variable directly enters the VOD computation.
    """

    unit: str
    source: str
    note: str
    budget: float | None = None
    vod_relevant: bool = False


@dataclass
class VarDiffStats:
    """Actual difference statistics for one variable between two datasets.

    Produced by ``compute_diff_report``; printed by ``print_diff_report``.
    """

    var: str
    n_both: int  # valid pairs (both non-NaN)
    n_a_only: int  # valid in A, NaN in B
    n_b_only: int  # valid in B, NaN in A
    max_abs: float
    mean_abs: float
    bias: float  # mean(A − B)
    rmse: float
    p50: float
    p99: float
    budget: float | None
    budget_unit: str
    budget_source: str
    exceeds_budget: bool
    vod_relevant: bool
    note: str


def compute_diff_report(
    ds_a: xr.Dataset,
    ds_b: xr.Dataset,
    budgets: dict[str, VariableBudget],
    vars_to_check: list[str] | None = None,
    label_a: str = "A",
    label_b: str = "B",
) -> list[VarDiffStats]:
    """Compute per-variable difference statistics for all shared variables.

    Every variable present in both datasets is reported.  Nothing is
    suppressed: statistics are computed regardless of whether differences
    are within or exceed the physical budget.

    Parameters
    ----------
    ds_a, ds_b : xarray.Dataset
        Datasets to compare.  Must share a ``sid`` coordinate for
        intersection alignment.
    budgets : dict[str, VariableBudget]
        Per-variable expected-difference budgets.  Variables not in the
        dict are reported without a budget (``budget=None``).
    vars_to_check : list of str, optional
        Subset of variables to include.  Defaults to all shared variables.
    label_a, label_b : str
        Names used in the ``n_a_only`` / ``n_b_only`` fields for logging.

    Returns
    -------
    list[VarDiffStats]
        One entry per variable, always including actual numeric differences.
    """
    shared_sid = np.intersect1d(ds_a.sid.values, ds_b.sid.values)
    a = ds_a.sel(sid=shared_sid)
    b = ds_b.sel(sid=shared_sid)

    if vars_to_check is None:
        vars_to_check = [str(v) for v in a.data_vars if v in b.data_vars]
    else:
        vars_to_check = [str(v) for v in vars_to_check]

    results = []
    for var in vars_to_check:
        if var not in a.data_vars or var not in b.data_vars:
            continue

        av = a[var].values.ravel()
        bv = b[var].values.ravel()
        valid_a = ~np.isnan(av)
        valid_b = ~np.isnan(bv)
        both = valid_a & valid_b

        n_both = int(np.sum(both))
        n_a_only = int(np.sum(valid_a & ~valid_b))
        n_b_only = int(np.sum(~valid_a & valid_b))

        budget_info = budgets.get(var)
        budget = budget_info.budget if budget_info else None
        unit = budget_info.unit if budget_info else ""
        source = budget_info.source if budget_info else "no budget defined"
        note = budget_info.note if budget_info else ""
        vod_rel = budget_info.vod_relevant if budget_info else False

        if n_both > 0:
            diff = av[both] - bv[both]
            absd = np.abs(diff)
            max_abs = float(np.max(absd))
            mean_abs = float(np.mean(absd))
            bias_val = float(np.mean(diff))
            rmse_val = float(np.sqrt(np.mean(diff**2)))
            p50 = float(np.percentile(absd, 50))
            p99 = float(np.percentile(absd, 99))
        else:
            max_abs = mean_abs = bias_val = rmse_val = p50 = p99 = float("nan")

        exceeds = budget is not None and not np.isnan(max_abs) and max_abs > budget

        results.append(
            VarDiffStats(
                var=var,
                n_both=n_both,
                n_a_only=n_a_only,
                n_b_only=n_b_only,
                max_abs=max_abs,
                mean_abs=mean_abs,
                bias=bias_val,
                rmse=rmse_val,
                p50=p50,
                p99=p99,
                budget=budget,
                budget_unit=unit,
                budget_source=source,
                exceeds_budget=exceeds,
                vod_relevant=vod_rel,
                note=note,
            )
        )

    return results


def print_diff_report(
    stats: list[VarDiffStats],
    group: str,
    label_a: str = "A",
    label_b: str = "B",
) -> None:
    """Print a structured difference report for one group.

    Every variable is printed with full statistics and budget annotation.
    Nothing is suppressed.
    """
    print(f"\n{'─' * 72}")
    print(f"Observable difference report: {group}  ({label_a} vs {label_b})")
    print(f"{'─' * 72}")
    for s in stats:
        vod_tag = " [VOD]" if s.vod_relevant else ""
        budget_str = (
            f"{s.budget} {s.budget_unit}" if s.budget is not None else "unbounded"
        )
        flag = "  *** EXCEEDS BUDGET ***" if s.exceeds_budget else ""
        print(f"\n  {s.var}{vod_tag}:")
        print(f"    Expected budget : {budget_str}")
        print(f"    Budget source   : {s.budget_source}")
        print(
            f"    n (both valid)  : {s.n_both}"
            f"  |  {label_a}-only: {s.n_a_only}"
            f"  |  {label_b}-only: {s.n_b_only}"
        )
        if not np.isnan(s.max_abs):
            print(f"    max |diff|      : {s.max_abs:.8g} {s.budget_unit}{flag}")
            print(f"    RMSE            : {s.rmse:.8g} {s.budget_unit}")
            print(f"    mean |diff|     : {s.mean_abs:.8g} {s.budget_unit}")
            print(f"    bias ({label_a}−{label_b}) : {s.bias:.8g} {s.budget_unit}")
            print(f"    p50 / p99       : {s.p50:.8g} / {s.p99:.8g} {s.budget_unit}")
        else:
            print("    (no valid pairs)")
    print(f"{'─' * 72}")


@dataclass(frozen=True)
class VariableStats:
    """Per-variable comparison statistics."""

    name: str
    exact_match: bool
    # ── validity counts ───────────────────────────────────────────────────
    n_total: int  # total cells in shared (epoch, sid) domain
    n_compared: int  # both non-NaN (valid pairs)
    n_a_only: int  # valid in A, NaN in B
    n_b_only: int  # valid in B, NaN in A
    n_neither: int  # both NaN
    n_nonzero_diff: int  # valid pairs where |diff| > 0
    # ── difference statistics (NaN when exact_match=True or n_compared=0) ─
    max_abs_diff: float
    rmse: float
    mae: float
    bias: float
    correlation: float
    # ── percentiles of |diff| over valid pairs ────────────────────────────
    p50: float
    p90: float
    p99: float
    # ── NaN rates ─────────────────────────────────────────────────────────
    n_nan_a: int
    n_nan_b: int
    pct_nan_a: float
    pct_nan_b: float
    nan_agreement_rate: float

    def as_dict(self) -> dict[str, Any]:
        """Flat dict for DataFrame construction."""
        return {
            "variable": self.name,
            "exact_match": self.exact_match,
            "n_total": self.n_total,
            "n_compared": self.n_compared,
            "n_a_only": self.n_a_only,
            "n_b_only": self.n_b_only,
            "n_neither": self.n_neither,
            "n_nonzero_diff": self.n_nonzero_diff,
            "max_abs_diff": round(self.max_abs_diff, 8),
            "rmse": round(self.rmse, 8),
            "mae": round(self.mae, 8),
            "bias": round(self.bias, 8),
            "correlation": round(self.correlation, 6),
            "p50": round(self.p50, 8),
            "p90": round(self.p90, 8),
            "p99": round(self.p99, 8),
            "pct_nan_a": round(self.pct_nan_a, 4),
            "pct_nan_b": round(self.pct_nan_b, 4),
            "nan_agreement": round(self.nan_agreement_rate, 4),
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
    exact_match: bool | None = None,
) -> VariableStats:
    """Compute all comparison statistics for a single variable.

    Parameters
    ----------
    name : str
        Variable name.
    a, b : np.ndarray
        Arrays of equal shape to compare.
    exact_match : bool, optional
        Pre-computed exact equality (e.g. from ``da.equals()``).
        If None, inferred as ``max_abs_diff == 0``.
    """
    a = a.ravel().astype(np.float64)
    b = b.ravel().astype(np.float64)

    n_total = len(a)
    mask_a = np.isfinite(a)
    mask_b = np.isfinite(b)
    mask = mask_a & mask_b

    n_nan_a = int(np.isnan(a).sum())
    n_nan_b = int(np.isnan(b).sum())
    n_compared = int(mask.sum())
    n_a_only = int(np.sum(mask_a & ~mask_b))
    n_b_only = int(np.sum(~mask_a & mask_b))
    n_neither = int(np.sum(~mask_a & ~mask_b))

    if n_compared == 0:
        _nan = float("nan")
        return VariableStats(
            name=name,
            exact_match=exact_match if exact_match is not None else True,
            n_total=n_total,
            n_compared=0,
            n_a_only=n_a_only,
            n_b_only=n_b_only,
            n_neither=n_neither,
            n_nonzero_diff=0,
            max_abs_diff=_nan,
            rmse=_nan,
            mae=_nan,
            bias=_nan,
            correlation=_nan,
            p50=_nan,
            p90=_nan,
            p99=_nan,
            n_nan_a=n_nan_a,
            n_nan_b=n_nan_b,
            pct_nan_a=n_nan_a / n_total if n_total > 0 else 0.0,
            pct_nan_b=n_nan_b / n_total if n_total > 0 else 0.0,
            nan_agreement_rate=nan_agreement(a, b),
        )

    # Compute diff once, reuse for all stats
    diff = a[mask] - b[mask]
    absdiff = np.abs(diff)
    n_nonzero_diff = int(np.sum(absdiff > 0))

    _max_abs = float(np.max(absdiff))
    if exact_match is None:
        exact_match = _max_abs == 0.0

    return VariableStats(
        name=name,
        exact_match=exact_match,
        n_total=n_total,
        n_compared=n_compared,
        n_a_only=n_a_only,
        n_b_only=n_b_only,
        n_neither=n_neither,
        n_nonzero_diff=n_nonzero_diff,
        max_abs_diff=_max_abs,
        rmse=float(np.sqrt(np.mean(diff**2))),
        mae=float(np.mean(absdiff)),
        bias=float(np.mean(diff)),
        correlation=correlation(a, b),
        p50=float(np.percentile(absdiff, 50)),
        p90=float(np.percentile(absdiff, 90)),
        p99=float(np.percentile(absdiff, 99)),
        n_nan_a=n_nan_a,
        n_nan_b=n_nan_b,
        pct_nan_a=n_nan_a / n_total if n_total > 0 else 0.0,
        pct_nan_b=n_nan_b / n_total if n_total > 0 else 0.0,
        nan_agreement_rate=nan_agreement(a, b),
    )
