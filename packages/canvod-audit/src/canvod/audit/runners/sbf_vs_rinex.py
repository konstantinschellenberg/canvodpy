"""Tier 1a: Compare SBF and RINEX stores from the same receivers.

The same Septentrio receiver writes both SBF (binary) and RINEX (text)
files from the same raw observations. After processing both through
canvodpy, the outputs should agree within known instrumentation limits.

Known differences (not bugs):
  - SNR: SBF quantises to 0.25 dB steps, RINEX keeps ~0.001 dB precision
  - Epochs: SBF records receiver clock time (+2s offset vs RINEX nominal)
  - Satellite coverage: SBF reader discovers fewer SIDs than RINEX header

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
from canvod.audit.tolerances import Tolerance, ToleranceTier

# SBF vs RINEX tolerances — justified by hardware/format differences
SBF_RINEX_TOLERANCES = {
    "SNR": Tolerance(
        atol=0.25,
        rtol=0.0,
        nan_rate_atol=0.05,
        description="SBF quantises SNR to 0.25 dB; RINEX ~0.001 dB. "
        "Hardware limitation, not a bug.",
    ),
    "phi": Tolerance(
        atol=0.05,
        rtol=0.0,
        nan_rate_atol=0.05,
        description="Elevation angle differences from ~2s epoch offset "
        "(satellites move ~0.5 deg/s at low elevations).",
    ),
    "theta": Tolerance(
        atol=0.05,
        rtol=0.0,
        nan_rate_atol=0.05,
        description="Azimuth angle differences from ~2s epoch offset.",
    ),
    "sat_x": Tolerance(
        atol=100.0,
        rtol=0.0,
        nan_rate_atol=0.1,
        description="Satellite X position: ~2s epoch offset → ~100m difference "
        "at orbital velocity (~3.9 km/s). NaN rate differs because SBF/RINEX "
        "discover different satellite sets.",
    ),
    "sat_y": Tolerance(
        atol=100.0,
        rtol=0.0,
        nan_rate_atol=0.1,
        description="Satellite Y position: same as sat_x.",
    ),
    "sat_z": Tolerance(
        atol=100.0,
        rtol=0.0,
        nan_rate_atol=0.1,
        description="Satellite Z position: same as sat_x.",
    ),
}


def audit_sbf_vs_rinex(
    sbf_store,
    rinex_store,
    *,
    groups=None,
    variables=None,
):
    """Compare SBF and RINEX stores group by group.

    Parameters
    ----------
    sbf_store : str or Path
        Path to the canvodpy SBF Icechunk store.
    rinex_store : str or Path
        Path to the canvodpy RINEX Icechunk store.
    groups : list of str, optional
        Which groups to compare. If not given, auto-discovers shared groups.
    variables : list of str, optional
        Which variables to compare. If not given, compares all shared.

    Returns
    -------
    AuditResult
    """
    store_sbf = open_store(sbf_store)
    store_rinex = open_store(rinex_store)
    result = AuditResult()

    if groups is None:
        groups = find_shared_groups(store_sbf, store_rinex)
        print(f"Found {len(groups)} shared groups: {groups}")

    for group in groups:
        print(f"Comparing SBF vs RINEX: {group} ...")
        ds_sbf = load_group(store_sbf, group)
        ds_rinex = load_group(store_rinex, group)

        r = compare_datasets(
            ds_sbf,
            ds_rinex,
            variables=variables,
            tier=ToleranceTier.SCIENTIFIC,
            tolerance_overrides=SBF_RINEX_TOLERANCES,
            label=f"{group}: SBF vs RINEX",
        )
        result.results[f"sbf_vs_rinex_{group}"] = r

    print()
    print(result.summary())
    return result
