"""Tier 1a: Compare SBF and RINEX stores from the same receivers.

The same Septentrio receiver writes both SBF (binary) and RINEX (text)
files from the same raw observations.  After processing both through
canvodpy, the outputs should agree within physically-grounded budgets.

Scientific philosophy
----------------------
Differences must not be hidden in tolerances.  Every observable is
compared and reported with full statistics (max, mean, bias, p50, p99).
A tolerance is only set when a specific, documented physical mechanism
explains the difference and bounds its magnitude.  Anything exceeding
that budget is flagged as requiring investigation regardless of whether
the observable enters VOD computation.

Expected-difference budgets (physically grounded)
--------------------------------------------------
Variable     Budget          Source
-----------  --------------  -----------------------------------------------
SNR          < 0.001 dB-Hz   F14.3 rounding of RINEX S observable.
                             Both sbf2rin and canvodpy apply the IDENTICAL
                             CN0HighRes correction (MeasExtra.Misc bits 0-2,
                             0.03125 dB-Hz/LSB, RefGuide-4.14.0 p.265).
                             C/N₀ is a power ratio, unaffected by the
                             RxClkBias time-scale transformation.
                             Verified: NotebookLM vs firmware PDFs,
                             2026-03-26.

Doppler      ≤ 0.5 mHz       sbf2rin F14.3 truncation.  SBF i4 encodes
                             Doppler at 0.1 mHz/LSB; RINEX F14.3 has
                             1 mHz precision.  Max rounding = 0.5 mHz.
                             (RefGuide-4.14.0, p.263)

Pseudorange  unbounded       sbf2rin applies: (a) RxClkBias×c subtraction
                             converting to GNSS-referenced range;
                             (b) removal of 1 ms clock-jump artefacts
                             (~299,792 m per jump, RefGuide-4.14.0 p.262).
                             canvodpy preserves raw receiver-time range.
                             Not used for VOD.

Phase        unbounded       sbf2rin interpolates to nominal GNSS epoch
                             (Phase += Doppler×RxClkBias) and removes
                             clock-jump phase steps (~1575 cycles GPS L1).
                             canvodpy preserves raw phase.
                             Not used for VOD.

phi/theta    epoch-offset    Same SP3/CLK, same interpolation code.
             dependent       Any difference is from the ~2 s epoch offset
                             between receiver-time (SBF) and GNSS-grid
                             (RINEX) timestamps.  Satellite moves
                             < ~0.001 rad/s at low elevations.

sat_x/y/z    ~100 m          Same SP3/CLK.  ~2 s epoch offset at orbital
                             velocity ~3.9 km/s → ~8 m typical; 100 m
                             used as conservative bound.

NaN rates    ≤ 5%            SBF Type2 sub-block dropout vs. flat RINEX
                             signal listing (RefGuide-4.14.0, pp.263-264).

Pass/fail gate
--------------
The overall PASS/FAIL verdict is gated on SNR and geometry (phi, theta,
sat_x/y/z) because these are the VOD-relevant observables.  Pseudorange,
Phase, and Doppler differences are reported with their expected budget but
do NOT gate the verdict — their differences are large, explained, and
irrelevant to VOD.  If any variable exceeds its budget the finding is
printed and stored regardless.

References
----------
- RefGuide-4.14.0: Septentrio AsteRx SB3 ProBase Firmware v4.14.0
- RefGuide-4.15.1: Septentrio AsteRx SB3 ProBase Firmware v4.15.1
- RxTools-1.10.0: Septentrio RxTools Manual v1.10.0, §11.8.2 (sbf2rin)
  https://geodesy.noaa.gov/pub/abilich/antcalCorbin/RxTools_Manual_v1.10.0.pdf

Usage::

    from canvod.audit.runners import audit_sbf_vs_rinex

    result = audit_sbf_vs_rinex(
        sbf_store="/path/to/sbf_store",
        rinex_store="/path/to/rinex_store",
    )
    print(result.summary())
"""

from __future__ import annotations

from canvod.audit.core import compare_datasets
from canvod.audit.runners.common import (
    AuditResult,
    find_shared_groups,
    load_group,
    open_store,
)
from canvod.audit.stats import (
    VarDiffStats,
    VariableBudget,
    compute_diff_report,
    print_diff_report,
)
from canvod.audit.tolerances import Tolerance, ToleranceTier

__all__ = [
    "EXCLUDED_VARS",
    "FORMAL_VARS",
    "SBF_RINEX_TOLERANCES",
    "VARIABLE_BUDGETS",
    "VarDiffStats",
    "VariableBudget",
    "audit_sbf_vs_rinex",
    "compute_diff_report",
    "print_diff_report",
]


# Per-variable expected-difference budgets grounded in firmware documentation.
VARIABLE_BUDGETS: dict[str, VariableBudget] = {
    "SNR": VariableBudget(
        budget=0.001,
        unit="dB-Hz",
        source="F14.3 rounding of RINEX S observable (max 0.0005 dB-Hz)",
        note=(
            "Both sbf2rin and canvodpy apply the identical CN0HighRes correction "
            "(MeasExtra.Misc bits 0-2, 0.03125 dB-Hz/LSB, RefGuide-4.14.0 p.265). "
            "C/N0 is a power ratio unaffected by RxClkBias time-scale transformation. "
            "Any difference > 0.001 dB-Hz indicates a CN0 decoding error. "
            "Verified via NotebookLM against firmware PDFs, 2026-03-26."
        ),
        vod_relevant=True,
    ),
    "Doppler": VariableBudget(
        budget=0.0005,
        unit="Hz",
        source="sbf2rin F14.3 truncation: SBF 0.1 mHz/LSB → RINEX 1 mHz precision",
        note=(
            "SBF i4 Doppler at 0.0001 Hz/LSB; sbf2rin writes RINEX F14.3 (0.001 Hz). "
            "Max quantisation error = 0.5 mHz = 0.0005 Hz. "
            "Any difference > 0.0005 Hz is unexplained. "
            "Doppler is not used for VOD."
        ),
        vod_relevant=False,
    ),
    "Pseudorange": VariableBudget(
        budget=None,
        unit="m",
        source=(
            "sbf2rin: (a) subtracts RxClkBias×c; "
            "(b) removes 1 ms clock-jump artefacts (~299,792 m per jump)"
        ),
        note=(
            "canvodpy preserves raw receiver-time pseudorange. "
            "Differences are large (up to ~300 km at clock-jump epochs), expected, "
            "and not indicative of any bug. Pseudorange is not used for VOD. "
            "RefGuide-4.14.0 p.262 (CumClkJumps), p.337 (RxClkBias)."
        ),
        vod_relevant=False,
    ),
    "Phase": VariableBudget(
        budget=None,
        unit="cycles",
        source=(
            "sbf2rin: (a) interpolates to nominal GNSS epoch via Doppler×RxClkBias; "
            "(b) removes clock-jump phase steps (~1575 cycles GPS L1)"
        ),
        note=(
            "canvodpy preserves raw carrier phase. "
            "Differences are large at clock-jump epochs and systematically "
            "offset by Doppler×RxClkBias at every epoch. Not a bug. "
            "Phase is not used for VOD."
        ),
        vod_relevant=False,
    ),
    "phi": VariableBudget(
        budget=0.05,
        unit="rad",
        source="~2 s epoch offset; satellite angular velocity < ~0.001 rad/s",
        note=(
            "Same SP3/CLK ephemeris and CubicHermiteSpline interpolation code. "
            "Difference is entirely from the epoch offset between receiver-time "
            "(SBF) and GNSS-grid (RINEX) timestamps. "
            "Any difference > 0.05 rad is unexplained."
        ),
        vod_relevant=True,
    ),
    "theta": VariableBudget(
        budget=0.05,
        unit="rad",
        source="~2 s epoch offset; satellite angular velocity < ~0.001 rad/s",
        note=("Same as phi. Any difference > 0.05 rad is unexplained."),
        vod_relevant=True,
    ),
    "sat_x": VariableBudget(
        budget=100.0,
        unit="m",
        source="~2 s epoch offset at orbital velocity ~3.9 km/s → ~8 m typical",
        note=(
            "Same SP3/CLK. 100 m is a conservative bound; typical difference ~8 m. "
            "NaN rate may differ because SBF/RINEX discover different SID sets."
        ),
        vod_relevant=False,
    ),
    "sat_y": VariableBudget(
        budget=100.0,
        unit="m",
        source="Same as sat_x.",
        note="Same as sat_x.",
        vod_relevant=False,
    ),
    "sat_z": VariableBudget(
        budget=100.0,
        unit="m",
        source="Same as sat_x.",
        note="Same as sat_x.",
        vod_relevant=False,
    ),
}

# SBF vs RINEX formal tolerances — used by compare_datasets pass/fail gate.
# Each atol matches the documented physical budget above (not a generous margin).
# Variables without a bounded budget (Pseudorange, Phase) are excluded from
# compare_datasets entirely; their statistics are reported separately.
SBF_RINEX_TOLERANCES: dict[str, Tolerance] = {
    var: Tolerance(
        atol=b.budget,
        mae_atol=0.0,
        nan_rate_atol=0.05,
        description=b.source,
    )
    for var, b in VARIABLE_BUDGETS.items()
    if b.budget is not None
}

# Variables with a defined budget → formal pass/fail via compare_datasets.
FORMAL_VARS: list[str] = list(SBF_RINEX_TOLERANCES.keys())

# Variables excluded from pass/fail (unbounded expected differences).
EXCLUDED_VARS: tuple[str, ...] = tuple(
    var for var, b in VARIABLE_BUDGETS.items() if b.budget is None
)


def audit_sbf_vs_rinex(
    sbf_store,
    rinex_store,
    *,
    groups: list[str] | None = None,
) -> AuditResult:
    """Compare SBF and RINEX stores group by group.

    Computes and reports full difference statistics for ALL shared
    variables.  The overall PASS/FAIL verdict is gated on SNR and
    geometry (variables with a defined physical budget).  Pseudorange
    and Phase are reported with their expected-difference context but
    do not gate the verdict — their differences are large, physically
    explained, and irrelevant to VOD.

    Parameters
    ----------
    sbf_store : str or Path
        Path to the canvodpy SBF Icechunk store.
    rinex_store : str or Path
        Path to the canvodpy RINEX Icechunk store.
    groups : list of str, optional
        Which groups to compare. Defaults to all shared groups.

    Returns
    -------
    AuditResult
        ``result.results[f"sbf_vs_rinex_{group}"]`` contains the
        ComparisonResult for formal variables.
        ``result.results[f"diff_stats_{group}"]`` contains the list of
        VarDiffStats for all variables.
    """
    store_sbf = open_store(sbf_store)
    store_rinex = open_store(rinex_store)
    result = AuditResult()

    if groups is None:
        groups = find_shared_groups(store_sbf, store_rinex)
        print(f"Found {len(groups)} shared groups: {groups}")

    for group in groups:
        print(f"\nComparing SBF vs RINEX: {group} ...")
        ds_sbf = load_group(store_sbf, group)
        ds_rinex = load_group(store_rinex, group)

        # Full observable report — every variable, actual numbers, no suppression
        diff_stats = compute_diff_report(
            ds_sbf, ds_rinex, VARIABLE_BUDGETS, label_a="SBF", label_b="RINEX"
        )
        print_diff_report(diff_stats, group, label_a="SBF", label_b="RINEX")

        # Formal pass/fail gate on bounded-budget variables only
        r = compare_datasets(
            ds_sbf,
            ds_rinex,
            variables=FORMAL_VARS,
            tier=ToleranceTier.SCIENTIFIC,
            tolerance_overrides=SBF_RINEX_TOLERANCES,
            label=f"{group}: SBF vs RINEX",
        )
        result.results[f"sbf_vs_rinex_{group}"] = r

        exceeding = [s for s in diff_stats if s.exceeds_budget]
        if exceeding:
            print(
                f"\n  *** {len(exceeding)} variable(s) exceed their physical budget: "
                + ", ".join(s.var for s in exceeding)
            )

    print()
    print(result.summary())
    return result
