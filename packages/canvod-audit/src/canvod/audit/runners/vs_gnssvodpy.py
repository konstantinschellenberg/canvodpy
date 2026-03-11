"""Tier 0: Compare canvodpy stores against gnssvodpy (truth).

Both implementations process the same RINEX files. Expected results:
  - SNR, canopy phi/theta, VOD: bit-identical (zero difference)
  - Reference phi/theta: known coordinate conversion difference (~2.43 deg max),
    does NOT affect VOD

Usage::

    from canvod.audit.runners import audit_vs_gnssvodpy

    result = audit_vs_gnssvodpy(
        canvodpy_rinex="/path/to/canvodpy_store",
        gnssvodpy_rinex="/path/to/gnssvodpy_store",
        canvodpy_vod="/path/to/canvodpy_vod",       # optional
        gnssvodpy_vod="/path/to/gnssvodpy_vod",      # optional
    )

    print(result.summary())   # human-readable report
    print(result.passed)      # True / False
    df = result.to_polars()   # all stats as a table
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

# Reference phi/theta are allowed to differ — this is a known coordinate
# conversion difference between canvodpy and gnssvodpy (ECEF → spherical).
# It does NOT affect VOD because VOD uses canopy angles, not reference angles.
REFERENCE_TOLERANCE_OVERRIDES = {
    "phi": Tolerance(
        atol=0.05,
        mae_atol=0.0,
        nan_rate_atol=0.01,
        description="Reference phi: coordinate conversion difference "
        "(ECEF-to-spherical), max 2.43 deg observed. Does not affect VOD.",
    ),
    "theta": Tolerance(
        atol=0.01,
        mae_atol=0.0,
        nan_rate_atol=0.01,
        description="Reference theta: small coordinate conversion difference.",
    ),
}


def audit_vs_gnssvodpy(
    canvodpy_rinex,
    gnssvodpy_rinex,
    canvodpy_vod=None,
    gnssvodpy_vod=None,
    *,
    rinex_groups=None,
    vod_groups=None,
    variables=None,
):
    """Compare canvodpy and gnssvodpy stores, group by group.

    Parameters
    ----------
    canvodpy_rinex, gnssvodpy_rinex : str or Path
        Paths to the RINEX Icechunk stores.
    canvodpy_vod, gnssvodpy_vod : str or Path, optional
        Paths to the VOD stores. If both provided, VOD is compared too.
    rinex_groups : list of str, optional
        Which groups to compare (e.g. ["canopy_01", "reference_01"]).
        If not given, automatically finds groups that exist in both stores.
    vod_groups : list of str, optional
        Same, for VOD stores.
    variables : list of str, optional
        Which variables to compare (e.g. ["SNR", "phi"]).
        If not given, compares all variables shared between both datasets.

    Returns
    -------
    AuditResult
        Contains one ComparisonResult per group, with .passed, .summary(),
        and .to_polars().
    """
    store_canv = open_store(canvodpy_rinex)
    store_gnss = open_store(gnssvodpy_rinex)
    result = AuditResult()

    # --- RINEX stores ---

    if rinex_groups is None:
        rinex_groups = find_shared_groups(store_canv, store_gnss)
        print(f"Found {len(rinex_groups)} shared RINEX groups: {rinex_groups}")

    for group in rinex_groups:
        print(f"Comparing RINEX: {group} ...")
        ds_canv = load_group(store_canv, group)
        ds_gnss = load_group(store_gnss, group)

        # Reference groups have the known phi/theta difference —
        # use SCIENTIFIC tier with relaxed tolerances.
        # All other groups should be bit-identical (EXACT).
        is_reference = "reference" in group
        tier = ToleranceTier.SCIENTIFIC if is_reference else ToleranceTier.EXACT
        overrides = REFERENCE_TOLERANCE_OVERRIDES if is_reference else None

        r = compare_datasets(
            ds_canv,
            ds_gnss,
            variables=variables,
            tier=tier,
            tolerance_overrides=overrides,
            label=f"{group}: canvodpy vs gnssvodpy (RINEX)",
        )
        result.results[f"rinex_{group}"] = r

    # --- VOD stores (optional) ---

    if canvodpy_vod is not None and gnssvodpy_vod is not None:
        store_canv_vod = open_store(canvodpy_vod)
        store_gnss_vod = open_store(gnssvodpy_vod)

        if vod_groups is None:
            vod_groups = find_shared_groups(store_canv_vod, store_gnss_vod)
            print(f"Found {len(vod_groups)} shared VOD groups: {vod_groups}")

        for group in vod_groups:
            print(f"Comparing VOD: {group} ...")
            ds_canv = load_group(store_canv_vod, group)
            ds_gnss = load_group(store_gnss_vod, group)

            r = compare_datasets(
                ds_canv,
                ds_gnss,
                variables=variables,
                tier=ToleranceTier.EXACT,
                label=f"{group}: canvodpy vs gnssvodpy (VOD)",
            )
            result.results[f"vod_{group}"] = r

    print()
    print(result.summary())
    return result
