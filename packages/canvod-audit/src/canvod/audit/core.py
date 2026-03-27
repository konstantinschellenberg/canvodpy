"""Core comparison engine for canvod-audit.

The main entry point is ``compare_datasets()``, which aligns two xarray
Datasets on shared coordinates and computes per-variable statistics
with configurable tolerance tiers.
"""

from __future__ import annotations

import warnings
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
        True if all variables are within tolerance for the given tier.
        For EXACT tier this is equivalent to bit-identical; for SCIENTIFIC
        tier it means within the stated per-variable tolerances.
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
    coverage: CoverageReport | None = None

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
            f"── Comparison: {self.label} ──",
            f"Tier: {self.tier.value} | Status: {status} | Variables: {len(self.variable_stats)}",
        ]

        # Alignment info
        if self.alignment:
            a = self.alignment
            lines.append(
                f"Domain: {a.n_shared_epochs:,} epochs × {a.n_shared_sids} sids"
                + (
                    f"  (dropped: {a.n_dropped_epochs_a + a.n_dropped_epochs_b} epochs, "
                    f"{a.n_dropped_sids_a + a.n_dropped_sids_b} sids)"
                    if a.n_dropped_epochs_a
                    + a.n_dropped_epochs_b
                    + a.n_dropped_sids_a
                    + a.n_dropped_sids_b
                    else ""
                )
            )

        # Coverage: vars unique to each dataset
        if self.coverage:
            c = self.coverage
            if c.vars_a_only:
                lines.append(f"A-only vars : {c.vars_a_only}")
            if c.vars_b_only:
                lines.append(f"B-only vars : {c.vars_b_only}")

        # Per-variable stats table
        if self.variable_stats:
            lines.append(
                f"\n{'var':<14} {'exact':>5}  {'n_cmp':>9}  {'max_abs':>12}  "
                f"{'rmse':>12}  {'bias':>12}  {'p99':>12}  {'nan_agr':>7}"
            )
            lines.append("─" * 92)
            for vname, vs in self.variable_stats.items():
                exact_str = "✓" if vs.exact_match else "✗"
                if vs.n_compared == 0:
                    lines.append(
                        f"  {vname:<12} {exact_str:>5}  {'—':>9}  {'—':>12}  {'—':>12}  {'—':>12}  {'—':>12}  {'—':>7}"
                    )
                else:
                    lines.append(
                        f"  {vname:<12} {exact_str:>5}  {vs.n_compared:>9,}  "
                        f"{vs.max_abs_diff:>12.6g}  {vs.rmse:>12.6g}  "
                        f"{vs.bias:>12.6g}  {vs.p99:>12.6g}  {vs.nan_agreement_rate:>7.4f}"
                    )

        # Coverage: per-variable valid counts (only show non-trivial rows)
        if self.coverage:
            c = self.coverage
            asymmetric = [
                v
                for v in c.valid_both
                if c.valid_a_only.get(v, 0) or c.valid_b_only.get(v, 0)
            ]
            if asymmetric:
                lines.append("\nValidity asymmetry (valid in one, NaN in other):")
                for var in asymmetric:
                    lines.append(
                        f"  {var:<14}  both={c.valid_both[var]:>9,}  "
                        f"A-only={c.valid_a_only[var]:>8,}  "
                        f"B-only={c.valid_b_only[var]:>8,}  "
                        f"neither={c.neither_valid[var]:>8,}"
                    )

        # Failures / annotations
        if self.failures:
            lines.append("\nAnnotations / failures:")
            for var, reason in self.failures.items():
                lines.append(f"  {var}: {reason}")

        return "\n".join(lines)


@dataclass(frozen=True)
class CoverageReport:
    """Symmetric coverage report: what each dataset has that the other doesn't.

    All per-variable counts are over the **shared (epoch, sid) domain** after
    alignment. Dimension-level drops (orphan epochs / SIDs) are captured in
    :class:`AlignmentInfo`.
    """

    vars_a_only: list[str]
    """Variables present in A but absent from B."""
    vars_b_only: list[str]
    """Variables present in B but absent from A."""
    valid_both: dict[str, int]
    """Cells where both A and B are non-NaN."""
    valid_a_only: dict[str, int]
    """Cells valid in A, NaN in B (A has data B does not)."""
    valid_b_only: dict[str, int]
    """Cells valid in B, NaN in A (B has data A does not)."""
    neither_valid: dict[str, int]
    """Cells where both are NaN."""


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


def _is_string_dtype(dtype: Any) -> bool:
    """True for any string-like dtype: <U*, object, or numpy StringDType."""
    if dtype.kind in ("U", "O"):  # fixed-length unicode or object
        return True
    # numpy 2.0 StringDType has kind 'T'
    return getattr(dtype, "kind", None) == "T"


def _array_equal(a: Any, b: Any) -> bool:
    """Compare two numpy arrays safely for any dtype.

    Handles numeric (with NaN), fixed-width unicode, object strings, and
    numpy 2.0 StringDType — all of which can appear in xarray coordinates
    after Zarr V3 round-trips.
    """
    if _is_string_dtype(getattr(a, "dtype", None) or np.asarray(a).dtype):
        return np.array_equal(
            np.asarray(a.tolist() if hasattr(a, "tolist") else a, dtype=object),
            np.asarray(b.tolist() if hasattr(b, "tolist") else b, dtype=object),
        )
    return bool(np.array_equal(a, b, equal_nan=True))


def _structural_diff(da_a: Any, da_b: Any) -> str:
    """Return a concise description of structural differences between two DataArrays.

    Checks coordinate presence, dtype, and values.  Called only when values
    are numerically identical but ``da.equals()`` returned False.

    String-dtype mismatches (``<U*`` vs ``object`` vs ``StringDType``) are
    Zarr V3 serialization artifacts — the warning about FixedLengthUTF32 not
    having a stable V3 spec causes different zarr versions to read string coords
    back with different dtypes.  For such coords, only the VALUES are checked;
    a pure dtype mismatch between string-like types is reported separately as
    a storage issue rather than a science difference.
    """
    msgs: list[str] = []
    storage_notes: list[str] = []

    coords_a = set(da_a.coords)
    coords_b = set(da_b.coords)
    only_a = sorted(coords_a - coords_b)
    only_b = sorted(coords_b - coords_a)

    if only_a:
        msgs.append(f"coords only in A: {only_a}")
    if only_b:
        msgs.append(f"coords only in B: {only_b}")

    for c in sorted(coords_a & coords_b):
        ca, cb = da_a.coords[c], da_b.coords[c]
        both_string = _is_string_dtype(ca.dtype) and _is_string_dtype(cb.dtype)
        if ca.dtype != cb.dtype:
            if both_string:
                # Zarr V3 FixedLengthUTF32 serialization artifact — check values
                vals_equal = _array_equal(ca.values, cb.values)
                if not vals_equal:
                    msgs.append(
                        f"coord '{c}' string values differ (dtype: {ca.dtype} vs {cb.dtype})"
                    )
                else:
                    storage_notes.append(c)
            else:
                msgs.append(f"coord '{c}' dtype: {ca.dtype} vs {cb.dtype}")
        elif not _array_equal(ca.values, cb.values):
            msgs.append(f"coord '{c}' values differ")

    if storage_notes:
        msgs.append(
            f"string dtype mismatch (values identical, Zarr V3 artifact): {storage_notes}"
        )

    return "; ".join(msgs) if msgs else "unknown structural difference"


def _strip_attrs(da: Any) -> Any:
    """Return a DataArray with attrs cleared on itself and all its coordinates.

    ``da.equals()`` is documented to ignore attrs, but stripping them makes
    the contract explicit and guards against any edge-case where a coord's
    attrs affect equality (e.g. unit metadata triggering pint hooks).
    Coordinate values and dtypes are preserved so structural differences
    (different satellite-position coords, grid assignments, dtype mismatches)
    are still detected.
    """
    coords = {k: v.assign_attrs({}) for k, v in da.coords.items()}
    return da.assign_attrs({}).assign_coords(coords)


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
        # Normalize datetime64 resolution to avoid empty intersections when
        # one dataset uses ns and the other uses us (e.g. pandas vs numpy).
        epochs_a = ds_a.epoch.values
        epochs_b = ds_b.epoch.values
        if np.issubdtype(epochs_a.dtype, np.datetime64) and np.issubdtype(
            epochs_b.dtype, np.datetime64
        ):
            common_dtype = np.result_type(epochs_a.dtype, epochs_b.dtype)
            epochs_a = epochs_a.astype(common_dtype)
            epochs_b = epochs_b.astype(common_dtype)
        shared_epochs = np.intersect1d(epochs_a, epochs_b)
        ds_a = ds_a.sel(epoch=shared_epochs)
        ds_b = ds_b.sel(epoch=shared_epochs)
        info_kwargs["n_shared_epochs"] = len(shared_epochs)
    else:
        # One or both datasets lack the epoch dimension — treat as no shared epochs.
        # "both lack it" is fine (e.g. scalar data); "only one lacks it" is a
        # data-contract violation but we let the outer guard catch it via n_shared=0.
        info_kwargs["n_shared_epochs"] = 0

    # Align on sid
    if "sid" in ds_a.dims and "sid" in ds_b.dims:
        shared_sids = np.intersect1d(ds_a.sid.values, ds_b.sid.values)
        ds_a = ds_a.sel(sid=shared_sids)
        ds_b = ds_b.sel(sid=shared_sids)
        info_kwargs["n_shared_sids"] = len(shared_sids)
    else:
        # Same rationale as epoch: let the outer guard surface the failure.
        info_kwargs["n_shared_sids"] = 0

    return ds_a, ds_b, AlignmentInfo(**info_kwargs)


def _check_tolerance(
    vs: VariableStats,
    tol: Tolerance,
) -> str | None:
    """Check if a variable's stats are within tolerance. Returns failure reason or None.

    Tolerance check: a variable passes if ALL of the following hold:
    1. max_abs_diff <= atol      (absolute bound on worst-case error)
    2. mae <= mae_atol           (absolute bound on typical error, when mae_atol > 0)
    3. NaN rate difference <= nan_rate_atol

    For EXACT tier (atol=0, mae_atol=0): requires bit-identical values.
    """
    reasons = []

    # Guard: no valid pairs to compare (all NaN or empty arrays)
    if vs.n_compared == 0:
        nan_diff = abs(vs.pct_nan_a - vs.pct_nan_b)
        if nan_diff > tol.nan_rate_atol:
            # One side has data, the other doesn't — real disagreement
            return (
                f"no valid pairs: NaN rates disagree "
                f"({vs.pct_nan_a:.2%} vs {vs.pct_nan_b:.2%})"
            )
        # Both sides are all-NaN — both tools agree there is no data.
        # Vacuously pass: cannot evaluate correctness without data.
        return None

    # Bit-identical check
    if tol.atol == 0 and tol.mae_atol == 0:
        if vs.max_abs_diff > 0:
            reasons.append(f"not bit-identical: max_abs_diff={vs.max_abs_diff:.6g}")
    else:
        # Absolute tolerance: worst-case single-element error
        if tol.atol > 0 and vs.max_abs_diff > tol.atol:
            reasons.append(f"max_abs_diff={vs.max_abs_diff:.6g} > atol={tol.atol:.6g}")

        # Mean absolute error tolerance: typical error
        if tol.mae_atol > 0 and vs.mae > tol.mae_atol:
            reasons.append(f"mae={vs.mae:.6g} > mae_atol={tol.mae_atol:.6g}")

    # NaN rate agreement
    nan_diff = abs(vs.pct_nan_a - vs.pct_nan_b)
    if nan_diff > tol.nan_rate_atol:
        reasons.append(
            f"NaN rate disagreement: {vs.pct_nan_a:.2%} vs {vs.pct_nan_b:.2%} "
            f"(diff={nan_diff:.2%} > {tol.nan_rate_atol:.2%})"
        )

    return "; ".join(reasons) if reasons else None


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
    report_coverage: bool = False,
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

        # Guard: fail if alignment produced zero overlap
        if alignment.n_shared_epochs == 0 or alignment.n_shared_sids == 0:
            return ComparisonResult(
                label=label or f"{tier.value} comparison",
                variable_stats={},
                tier=tier,
                passed=False,
                failures={"_alignment": "No shared coordinates after alignment"},
                metadata=metadata or {},
                alignment=alignment,
            )
        # Warn when >5 % of the union is dropped — significant data loss
        total_epochs = (
            alignment.n_shared_epochs
            + alignment.n_dropped_epochs_a
            + alignment.n_dropped_epochs_b
        )
        total_sids = (
            alignment.n_shared_sids
            + alignment.n_dropped_sids_a
            + alignment.n_dropped_sids_b
        )
        drop_epoch_pct = (
            (alignment.n_dropped_epochs_a + alignment.n_dropped_epochs_b) / total_epochs
            if total_epochs > 0
            else 0.0
        )
        drop_sid_pct = (
            (alignment.n_dropped_sids_a + alignment.n_dropped_sids_b) / total_sids
            if total_sids > 0
            else 0.0
        )
        if drop_epoch_pct > 0.05 or drop_sid_pct > 0.05:
            warnings.warn(
                f"[{label or 'compare_datasets'}] Alignment dropped "
                f"{drop_epoch_pct:.1%} of epochs "
                f"({alignment.n_dropped_epochs_a} from A, "
                f"{alignment.n_dropped_epochs_b} from B) and "
                f"{drop_sid_pct:.1%} of SIDs "
                f"({alignment.n_dropped_sids_a} from A, "
                f"{alignment.n_dropped_sids_b} from B). "
                "Results cover only the intersection.",
                stacklevel=2,
            )

    # Determine variables to compare
    vars_a = set(ds_a.data_vars)
    vars_b = set(ds_b.data_vars)
    if variables is None:
        variables = sorted(vars_a & vars_b)

    # Compute per-variable stats
    variable_stats: dict[str, VariableStats] = {}
    failures: dict[str, str] = {}
    cov_valid_both: dict[str, int] = {}
    cov_valid_a_only: dict[str, int] = {}
    cov_valid_b_only: dict[str, int] = {}
    cov_neither: dict[str, int] = {}

    for var in variables:
        if var not in ds_a.data_vars or var not in ds_b.data_vars:
            continue

        try:
            a_vals = ds_a[var].values.astype(np.float64)
            b_vals = ds_b[var].values.astype(np.float64)
        except ValueError, TypeError:
            failures[var] = "non-numeric dtype, skipped"
            continue

        if a_vals.shape != b_vals.shape:
            failures[var] = f"shape mismatch: {a_vals.shape} vs {b_vals.shape}"
            continue

        # Strip attrs from the DataArray and all its coordinates before
        # comparing — da.equals() already ignores attrs per xarray spec, but
        # this makes the contract explicit.  Coordinate VALUES and dtypes are
        # still checked; if they differ between stores that is meaningful.
        exact_match = bool(_strip_attrs(ds_a[var]).equals(_strip_attrs(ds_b[var])))

        vs = compute_variable_stats(var, a_vals, b_vals, exact_match=exact_match)
        variable_stats[var] = vs

        # Tolerance check is annotation-only — never overrides exact_match for `passed`
        tol = get_tolerance(var, tier, tolerance_overrides)
        reason = _check_tolerance(vs, tol)
        if reason:
            failures[var] = reason

        # Structural diagnostic: values are identical but exact_match=False —
        # use xr.testing.assert_equal to pinpoint what structural element
        # differs (coord dtype, missing coord, dimension order, etc.).
        # Only runs when no numeric or NaN-rate failure was already recorded,
        # so it doesn't shadow a real science difference.
        if not exact_match and vs.max_abs_diff == 0.0 and var not in failures:
            msg = _structural_diff(ds_a[var], ds_b[var])
            failures[var] = f"structural (values identical): {msg}"

        if report_coverage:
            mask_a = ~np.isnan(a_vals)
            mask_b = ~np.isnan(b_vals)
            cov_valid_both[var] = int(np.sum(mask_a & mask_b))
            cov_valid_a_only[var] = int(np.sum(mask_a & ~mask_b))
            cov_valid_b_only[var] = int(np.sum(~mask_a & mask_b))
            cov_neither[var] = int(np.sum(~mask_a & ~mask_b))

    coverage = (
        CoverageReport(
            vars_a_only=sorted(vars_a - vars_b),
            vars_b_only=sorted(vars_b - vars_a),
            valid_both=cov_valid_both,
            valid_a_only=cov_valid_a_only,
            valid_b_only=cov_valid_b_only,
            neither_valid=cov_neither,
        )
        if report_coverage
        else None
    )

    # passed iff every compared variable is within tolerance for the given tier.
    # For EXACT tier (atol=0): a variable with any diff is in failures, so
    # len(failures)==0 implies bit-identical.  For SCIENTIFIC tier: implies
    # within stated tolerances.  exact_match is still recorded per-variable for
    # informational purposes (shown in the Typst ✓/✗ column).
    passed = bool(variable_stats) and len(failures) == 0

    return ComparisonResult(
        label=label or f"{tier.value} comparison",
        variable_stats=variable_stats,
        tier=tier,
        passed=passed,
        failures=failures,
        metadata=metadata or {},
        alignment=alignment,
        coverage=coverage,
    )
