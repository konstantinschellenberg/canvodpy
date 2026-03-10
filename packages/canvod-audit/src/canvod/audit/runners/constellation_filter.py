"""Infrastructure: Constellation filtering consistency.

When you process all constellations (GPS + GLONASS + Galileo + BeiDou),
the GPS subset should be identical to what you get when processing
GPS only. If it's not, constellation filtering is leaking — e.g. a
shared accumulator, incorrect SID indexing, or cross-constellation
coordinate contamination.

Usage::

    from canvod.audit.runners import audit_constellation_filter

    result = audit_constellation_filter(
        all_constellations_store="/path/to/all_constellations",
        filtered_store="/path/to/gps_only",
        system_prefix="G",  # GPS satellites start with "G"
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
from canvod.audit.tolerances import ToleranceTier


def audit_constellation_filter(
    all_constellations_store,
    filtered_store,
    *,
    system_prefix="G",
    groups=None,
    variables=None,
):
    """Compare a filtered store against the same constellation subset from a full store.

    Loads both stores, filters the all-constellation store to only SIDs
    starting with ``system_prefix``, then compares against the filtered store.

    Parameters
    ----------
    all_constellations_store : str or Path
        Store processed with all constellations enabled.
    filtered_store : str or Path
        Store processed with only one constellation (e.g. GPS-only).
    system_prefix : str
        SID prefix for the target constellation. GPS = "G", GLONASS = "R",
        Galileo = "E", BeiDou = "C". Default is "G" (GPS).
    groups : list of str, optional
        Which groups to compare. If not given, auto-discovers.
    variables : list of str, optional
        Which variables to compare. If not given, compares all shared.

    Returns
    -------
    AuditResult
    """
    store_all = open_store(all_constellations_store)
    store_filt = open_store(filtered_store)
    result = AuditResult()

    if groups is None:
        groups = find_shared_groups(store_all, store_filt)
        print(f"Found {len(groups)} shared groups: {groups}")

    for group in groups:
        print(f"Constellation filter test: {group} ...")
        ds_all = load_group(store_all, group)
        ds_filt = load_group(store_filt, group)

        # Filter the all-constellation dataset to matching SIDs
        matching_sids = [
            sid for sid in ds_all.sid.values if str(sid).startswith(system_prefix)
        ]
        ds_all_subset = ds_all.sel(sid=matching_sids)

        print(
            f"  All constellations: {len(ds_all.sid)} sids → "
            f"{len(matching_sids)} {system_prefix}* sids"
        )
        print(f"  Filtered store: {len(ds_filt.sid)} sids")

        r = compare_datasets(
            ds_all_subset,
            ds_filt,
            variables=variables,
            tier=ToleranceTier.EXACT,
            label=f"{group}: all-constellation {system_prefix}* subset "
            f"vs {system_prefix}-only processing",
        )
        result.results[f"constellation_{system_prefix}_{group}"] = r

    print()
    print(result.summary())
    return result
