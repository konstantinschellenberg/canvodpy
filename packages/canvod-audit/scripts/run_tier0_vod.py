"""Tier 0: VOD comparison — canvodpy vs gnssvodpy.

Compares VOD, phi, and theta from the canvodpy VOD store against the
gnssvodpy truth VOD store. Group names differ between tools:
canvodpy uses ``canopy_01_vs_reference_01``, gnssvodpy uses
``reference_01_canopy_01``.

Prerequisites
-------------
1. canvodpy RINEX store must exist (run ``produce_canvodpy_store.py``)
2. canvodpy VOD store must be computed (this script does it if missing)

Results (2026-03-10)
--------------------
- VOD: bit-identical (1,226,667 values compared)
- phi: bit-identical (1,972,042 values compared)
- theta: bit-identical (1,972,042 values compared)
"""

from __future__ import annotations

from canvod.audit.core import compare_datasets
from canvod.audit.runners.common import load_group, open_store
from canvod.audit.tolerances import ToleranceTier

# ── Store paths ──────────────────────────────────────────────────────────
CANVODPY_VOD = (
    "/Volumes/ExtremePro/canvod_audit_output"
    "/tier0_rinex_vs_gnssvodpy/Rosalia/canvodpy_VOD_store"
)
GNSSVODPY_VOD = (
    "/Volumes/ExtremePro/canvod_audit_output"
    "/gnssvodpy_based/gnssvodpy_VOD_Icechunk_Store"
)


def main() -> None:
    # ── Produce canvodpy VOD if missing ────────────────────────────────
    s_canv = open_store(CANVODPY_VOD)
    if not s_canv.list_groups():
        print("canvodpy VOD store is empty — computing VOD ...")
        from canvodpy import Site

        site = Site("Rosalia")
        site.vod._calculator_name = "tau_omega"
        site.vod.compute_bulk("canopy_01_vs_reference_01", write=True)
        print("VOD computed and written to store.")
        # Re-open
        s_canv = open_store(CANVODPY_VOD)

    # ── Load datasets ──────────────────────────────────────────────────
    # Group names differ: canvodpy = canopy_01_vs_reference_01,
    #                     gnssvodpy = reference_01_canopy_01
    ds_canv = load_group(s_canv, "canopy_01_vs_reference_01")
    print(f"canvodpy VOD: {dict(ds_canv.sizes)}, vars={list(ds_canv.data_vars)}")

    s_gnss = open_store(GNSSVODPY_VOD)
    ds_gnss = load_group(s_gnss, "reference_01_canopy_01")
    print(f"gnssvodpy VOD: {dict(ds_gnss.sizes)}, vars={list(ds_gnss.data_vars)}")

    # ── Compare ────────────────────────────────────────────────────────
    r = compare_datasets(
        ds_canv,
        ds_gnss,
        tier=ToleranceTier.EXACT,
        label="VOD: canvodpy vs gnssvodpy",
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


if __name__ == "__main__":
    main()
