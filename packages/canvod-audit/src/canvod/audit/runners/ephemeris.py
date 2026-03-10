"""Tier 1b: Compare broadcast vs agency ephemeris augmentation.

GNSS satellite positions can come from two sources:

  - **Broadcast ephemeris** — transmitted by the satellites in real-time.
    Orbit accuracy ~1-2 metres.
  - **Agency products (SP3/CLK)** — computed by IGS analysis centres days
    later. Orbit accuracy ~2 centimetres.

Both should place satellites in roughly the same location. The agency
products are more accurate, but broadcast is always available.

Usage::

    from canvod.audit.runners import audit_ephemeris_sources

    result = audit_ephemeris_sources(
        broadcast_store="/path/to/broadcast_store",
        agency_store="/path/to/agency_store",
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

# Broadcast vs agency tolerances — justified by orbit accuracy difference
EPHEMERIS_TOLERANCES = {
    "sat_x": Tolerance(
        atol=5.0,
        rtol=0.0,
        nan_rate_atol=0.1,
        description="Satellite X: broadcast ~1-2m accuracy vs agency ~2cm. "
        "NaN rate differs because broadcast/SP3 cover different satellite sets.",
    ),
    "sat_y": Tolerance(
        atol=5.0,
        rtol=0.0,
        nan_rate_atol=0.1,
        description="Satellite Y: same as sat_x.",
    ),
    "sat_z": Tolerance(
        atol=5.0,
        rtol=0.0,
        nan_rate_atol=0.1,
        description="Satellite Z: same as sat_x.",
    ),
    "phi": Tolerance(
        atol=0.01,
        rtol=0.0,
        nan_rate_atol=0.1,
        description="Elevation angle: small difference from orbit accuracy. "
        "At ~20,000 km altitude, a few metres of orbit error → <0.01 deg.",
    ),
    "theta": Tolerance(
        atol=0.01,
        rtol=0.0,
        nan_rate_atol=0.1,
        description="Azimuth angle: same as phi.",
    ),
}

# When comparing ephemeris sources, we only care about geometry variables
DEFAULT_VARIABLES = ["sat_x", "sat_y", "sat_z", "phi", "theta"]


def audit_ephemeris_sources(
    broadcast_store,
    agency_store,
    *,
    groups=None,
    variables=None,
):
    """Compare broadcast and agency ephemeris stores group by group.

    Parameters
    ----------
    broadcast_store : str or Path
        Path to store augmented with broadcast ephemeris.
    agency_store : str or Path
        Path to store augmented with agency products (SP3/CLK).
    groups : list of str, optional
        Which groups to compare. If not given, auto-discovers.
    variables : list of str, optional
        Which variables to compare. Defaults to satellite coordinate
        variables: sat_x, sat_y, sat_z, phi, theta.

    Returns
    -------
    AuditResult
    """
    store_broadcast = open_store(broadcast_store)
    store_agency = open_store(agency_store)
    result = AuditResult()

    if groups is None:
        groups = find_shared_groups(store_broadcast, store_agency)
        print(f"Found {len(groups)} shared groups: {groups}")

    if variables is None:
        variables = DEFAULT_VARIABLES

    for group in groups:
        print(f"Comparing broadcast vs agency: {group} ...")
        ds_broadcast = load_group(store_broadcast, group)
        ds_agency = load_group(store_agency, group)

        # Filter to variables that actually exist in both datasets
        available = [
            v
            for v in variables
            if v in ds_broadcast.data_vars and v in ds_agency.data_vars
        ]
        if not available:
            print(f"  {group}: no shared variables found, skipping")
            continue

        r = compare_datasets(
            ds_broadcast,
            ds_agency,
            variables=available,
            tier=ToleranceTier.SCIENTIFIC,
            tolerance_overrides=EPHEMERIS_TOLERANCES,
            label=f"{group}: broadcast vs agency ephemeris",
        )
        result.results[f"ephemeris_{group}"] = r

    print()
    print(result.summary())
    return result
