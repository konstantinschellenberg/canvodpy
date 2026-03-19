"""API level consistency: verify L1, L2, L3, L4 produce identical stores.

canvodpy has four API levels that are different ways to call the same
pipeline. They must all produce bit-identical output. Any difference
is a wiring bug, not a scientific issue.

Usage::

    from canvod.audit.runners import audit_api_levels

    result = audit_api_levels({
        "l1": "/path/to/l1_rinex_store",
        "l2": "/path/to/l2_rinex_store",
        "l4": "/path/to/l4_rinex_store",
    })

    print(result.summary())
    print(result.passed)
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

# Human-readable names for each level
LEVEL_NAMES = {
    "l1": "L1 (convenience)",
    "l2": "L2 (fluent)",
    "l3": "L3 (site/pipeline)",
    "l4": "L4 (functional)",
}


def audit_api_levels(
    stores,
    *,
    reference_level="l1",
    groups=None,
    variables=None,
    store_type="rinex",
):
    """Compare stores from different API levels against a reference level.

    Parameters
    ----------
    stores : dict
        Mapping of level key to store path. Example::

            {"l1": "/path/to/l1_store", "l2": "/path/to/l2_store"}

        Not all levels need to be present — only provided ones are compared.
    reference_level : str
        Which level to compare against (default: "l1").
    groups : list of str, optional
        Which groups to compare. If not given, auto-discovers.
    variables : list of str, optional
        Which variables to compare. If not given, compares all shared.
    store_type : str
        Label: "rinex" or "vod" (for the summary output).

    Returns
    -------
    AuditResult
    """
    if reference_level not in stores:
        available = list(stores.keys())
        raise ValueError(
            f"Reference level '{reference_level}' not found. Available: {available}"
        )

    ref_store = open_store(stores[reference_level])
    ref_name = LEVEL_NAMES.get(reference_level, reference_level)
    other_levels = [k for k in stores if k != reference_level]

    if not other_levels:
        print("Only one API level provided — nothing to compare.")
        return AuditResult()

    # Find groups that exist in all stores
    if groups is None:
        all_stores = [open_store(stores[k]) for k in stores]
        groups = find_shared_groups(*all_stores)
        print(f"Found {len(groups)} shared groups: {groups}")

    result = AuditResult()

    for level_key in other_levels:
        other_store = open_store(stores[level_key])
        other_name = LEVEL_NAMES.get(level_key, level_key)

        for group in groups:
            print(f"Comparing {ref_name} vs {other_name} — {group} ({store_type}) ...")

            ds_ref = load_group(ref_store, group)
            ds_other = load_group(other_store, group)

            r = compare_datasets(
                ds_ref,
                ds_other,
                variables=variables,
                tier=ToleranceTier.EXACT,
                label=f"{group}: {ref_name} vs {other_name} ({store_type})",
            )
            result.results[f"{store_type}_{level_key}_{group}"] = r

    print()
    print(result.summary())
    return result
