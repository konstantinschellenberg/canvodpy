"""Tier 1: Broadcast vs agency ephemeris — internal consistency comparison.

Compares canvodpy SBF stores produced with broadcast ephemeris (from SBF
SatVisibility records) vs agency final products (SP3/CLK from CODE).
Both stores use the same SBF input files.

Expected differences
--------------------
- SNR: identical (ephemeris source does not affect raw observables)
- phi/theta: ~0.001-0.01 deg differences from ~1-2 m orbit accuracy
  difference between broadcast and final products
- NaN rates may differ: broadcast and SP3 cover different satellite sets

Prerequisites
-------------
1. SBF agency store from ``produce_sbf_store_final.py``
2. SBF broadcast store from ``produce_sbf_store_broadcast.py``
"""

from __future__ import annotations

import numpy as np

from canvod.audit.core import compare_datasets
from canvod.audit.runners.common import load_group, open_store
from canvod.audit.tolerances import ToleranceTier

# ── Store paths ──────────────────────────────────────────────────────────
BROADCAST_STORE = (
    "/Volumes/ExtremePro/canvod_audit_output"
    "/tier1_broadcast_vs_agency/Rosalia/canvodpy_SBF_broadcast_store"
)
AGENCY_STORE = (
    "/Volumes/ExtremePro/canvod_audit_output"
    "/tier1_sbf_vs_rinex/Rosalia/canvodpy_SBF_allvars_store"
)

GROUPS = ["canopy_01", "reference_01_canopy_01"]


def main() -> None:
    s_broad = open_store(BROADCAST_STORE)
    s_agency = open_store(AGENCY_STORE)

    for group in GROUPS:
        print("=" * 60)
        print(f"Broadcast vs Agency: {group}")
        print("=" * 60)

        ds_broad = load_group(s_broad, group)
        ds_agency = load_group(s_agency, group)

        print(f"Broadcast: {dict(ds_broad.sizes)}, vars={list(ds_broad.data_vars)}")
        print(f"Agency:    {dict(ds_agency.sizes)}, vars={list(ds_agency.data_vars)}")

        # ── Formal comparison at APPROXIMATE tier ──────────────────────
        r = compare_datasets(
            ds_broad,
            ds_agency,
            tier=ToleranceTier.SCIENTIFIC,
            label=f"Broadcast vs Agency: {group}",
        )

        print(f"\nPassed: {r.passed}")
        print(f"Alignment: {r.alignment}")
        print(f"Failures: {r.failures}")
        for vname, vs in r.variable_stats.items():
            print(
                f"  {vname}: rmse={vs.rmse:.6g}, max_abs={vs.max_abs_diff:.6g}, "
                f"bias={vs.bias:.6g}, nan_agree={vs.nan_agreement_rate:.6f}, "
                f"n_compared={vs.n_compared}"
            )

        # ── Deep dive per variable ─────────────────────────────────────
        print(f"\n--- {group} deep dive ---")
        shared_epochs = np.intersect1d(ds_broad.epoch.values, ds_agency.epoch.values)
        shared_sids = np.intersect1d(ds_broad.sid.values, ds_agency.sid.values)
        print(f"Shared: {len(shared_epochs)} epochs, {len(shared_sids)} sids")

        b = ds_broad.sel(epoch=shared_epochs, sid=shared_sids)
        a = ds_agency.sel(epoch=shared_epochs, sid=shared_sids)

        for var in ["SNR", "Doppler", "Phase", "Pseudorange", "phi", "theta"]:
            bv = b[var].values
            av = a[var].values
            both_valid = ~np.isnan(bv) & ~np.isnan(av)
            diff = bv[both_valid] - av[both_valid]
            nonzero = int(np.sum(np.abs(diff) > 0))
            total = len(diff)
            print(f"\n{var}: {nonzero}/{total} non-zero ({100 * nonzero / total:.2f}%)")
            if nonzero > 0:
                absdiff = np.abs(diff[np.abs(diff) > 0])
                unit = "rad" if var in ("phi", "theta") else ""
                print(f"  max={np.max(absdiff):.8f} {unit}")
                print(f"  mean={np.mean(absdiff):.8f} {unit}")
                print(
                    f"  p50={np.percentile(absdiff, 50):.8f} {unit}, "
                    f"  p99={np.percentile(absdiff, 99):.8f} {unit}"
                )
                if var in ("phi", "theta"):
                    print(
                        f"  (in deg: max={np.degrees(np.max(absdiff)):.4f}°, "
                        f"mean={np.degrees(np.mean(absdiff)):.4f}°, "
                        f"p99={np.degrees(np.percentile(absdiff, 99)):.4f}°)"
                    )

            # NaN coverage
            broad_valid = int(np.sum(~np.isnan(bv)))
            agency_valid = int(np.sum(~np.isnan(av)))
            print(f"  Valid cells: broadcast={broad_valid}, agency={agency_valid}")


if __name__ == "__main__":
    main()
