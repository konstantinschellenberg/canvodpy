"""Tier 1: SBF vs RINEX — internal consistency comparison.

Compares canvodpy stores produced from SBF and RINEX files recorded by
the same receiver during the same observation session. Both use agency
(final) ephemeris for satellite coordinate computation.

Key finding: SBF and RINEX have misaligned epoch grids (constant 2-second
offset). SBF epochs fall on :02, :07, :12... while RINEX on :00, :05, :10...
This is because Septentrio's RINEX converter normalizes epochs to the
nominal sampling grid, while the SBF reader preserves the receiver's
actual measurement times.

The script re-indexes SBF epochs to the nearest RINEX epoch (snapping
the constant 2s offset) so that the underlying observations can be
compared directly.

Expected differences
--------------------
- SNR: up to 0.25 dB (SBF quantises to 0.25 dB steps)
- phi/theta: identical (same SP3 interpolation, same receiver position)
- NaN coverage: SBF may have fewer valid cells (fewer sids observed)

Prerequisites
-------------
1. RINEX store from ``produce_canvodpy_store.py`` (Tier 0)
2. SBF store from ``produce_sbf_store_final.py`` (this tier)
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from canvod.audit.core import compare_datasets
from canvod.audit.runners.common import load_group, open_store
from canvod.audit.tolerances import ToleranceTier

# ── Store paths ──────────────────────────────────────────────────────────
SBF_STORE = (
    "/Volumes/ExtremePro/canvod_audit_output"
    "/tier1_sbf_vs_rinex/Rosalia/canvodpy_SBF_allvars_store"
)
RINEX_STORE = (
    "/Volumes/ExtremePro/canvod_audit_output"
    "/tier1_sbf_vs_rinex/Rosalia/canvodpy_RINEX_allvars_store"
)

GROUPS = ["canopy_01", "reference_01_canopy_01"]


def snap_sbf_to_rinex(ds_sbf, ds_rnx):
    """Re-index SBF epochs to nearest RINEX epoch.

    The SBF receiver clock has a constant offset from the RINEX nominal
    grid. This function snaps each SBF epoch to the closest RINEX epoch
    (must be ≤ 2.5s), then re-indexes the SBF dataset to share the
    RINEX epoch coordinate.

    Returns the re-indexed SBF dataset and alignment stats.
    """
    sbf_ns = ds_sbf.epoch.values.astype("int64")
    rnx_ns = ds_rnx.epoch.values.astype("int64")

    # For each SBF epoch, find nearest RINEX epoch
    indices = np.searchsorted(rnx_ns, sbf_ns)
    indices = np.clip(indices, 1, len(rnx_ns) - 1)

    left = np.abs(sbf_ns - rnx_ns[indices - 1])
    right = np.abs(sbf_ns - rnx_ns[indices])
    use_left = left < right
    best_idx = np.where(use_left, indices - 1, indices)
    best_diff_ns = np.where(use_left, left, right)

    # Filter to ≤ 2.5s tolerance
    tolerance_ns = int(2.5e9)
    mask = best_diff_ns <= tolerance_ns

    # Build new epoch coordinate from matched RINEX epochs
    matched_rnx_epochs = ds_rnx.epoch.values[best_idx[mask]]
    diffs_s = best_diff_ns[mask] / 1e9

    # Re-index: select matched SBF rows, assign RINEX epoch coord
    ds_snapped = ds_sbf.isel(epoch=np.where(mask)[0])
    ds_snapped = ds_snapped.assign_coords(epoch=matched_rnx_epochs)

    stats = {
        "n_matched": int(np.sum(mask)),
        "n_total": len(sbf_ns),
        "offset_mean": float(np.mean(diffs_s)),
        "offset_std": float(np.std(diffs_s)),
    }
    return ds_snapped, stats


def main() -> None:
    s_sbf = open_store(SBF_STORE)
    s_rnx = open_store(RINEX_STORE)

    for group in GROUPS:
        print("=" * 60)
        print(f"SBF vs RINEX: {group}")
        print("=" * 60)

        ds_sbf = load_group(s_sbf, group)
        ds_rnx = load_group(s_rnx, group)

        print(f"SBF:   {dict(ds_sbf.sizes)}, vars={list(ds_sbf.data_vars)}")
        print(f"RINEX: {dict(ds_rnx.sizes)}, vars={list(ds_rnx.data_vars)}")

        # ── Epoch alignment ──────────────────────────────────────────────
        sbf_epochs = pd.DatetimeIndex(ds_sbf.epoch.values)
        rnx_epochs = pd.DatetimeIndex(ds_rnx.epoch.values)
        print(f"\nSBF first 3:   {[str(e) for e in sbf_epochs[:3]]}")
        print(f"RINEX first 3: {[str(e) for e in rnx_epochs[:3]]}")
        print(f"Exact shared: {len(np.intersect1d(sbf_epochs, rnx_epochs))}")

        # ── Snap SBF epochs to RINEX grid ────────────────────────────────
        ds_sbf_snapped, snap_stats = snap_sbf_to_rinex(ds_sbf, ds_rnx)
        print(
            f"Snapped: {snap_stats['n_matched']}/{snap_stats['n_total']} "
            f"(offset={snap_stats['offset_mean']:.3f}±{snap_stats['offset_std']:.3f}s)"
        )

        # ── Formal comparison ────────────────────────────────────────────
        r = compare_datasets(
            ds_sbf_snapped,
            ds_rnx,
            tier=ToleranceTier.SCIENTIFIC,
            label=f"SBF vs RINEX: {group}",
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

        # ── Deep dive per variable ───────────────────────────────────────
        print(f"\n--- {group} deep dive ---")
        shared_epochs = np.intersect1d(ds_sbf_snapped.epoch.values, ds_rnx.epoch.values)
        shared_sids = np.intersect1d(ds_sbf_snapped.sid.values, ds_rnx.sid.values)
        print(
            f"Shared after snap: {len(shared_epochs)} epochs, {len(shared_sids)} sids"
        )

        s = ds_sbf_snapped.sel(epoch=shared_epochs, sid=shared_sids)
        r2 = ds_rnx.sel(epoch=shared_epochs, sid=shared_sids)

        for var in ["SNR", "Doppler", "Phase", "Pseudorange", "phi", "theta"]:
            sv = s[var].values
            rv = r2[var].values
            both_valid = ~np.isnan(sv) & ~np.isnan(rv)
            n_valid = int(np.sum(both_valid))
            diff = sv[both_valid] - rv[both_valid]
            nonzero = int(np.sum(np.abs(diff) > 0))
            total = len(diff)

            sbf_only = int(np.sum(~np.isnan(sv) & np.isnan(rv)))
            rnx_only = int(np.sum(np.isnan(sv) & ~np.isnan(rv)))

            pct = f"({100 * nonzero / total:.2f}%)" if total > 0 else "(no data)"
            print(f"\n{var}: {nonzero}/{total} non-zero {pct}")
            print(
                f"  Valid: both={n_valid}, SBF-only={sbf_only}, RINEX-only={rnx_only}"
            )
            if nonzero > 0:
                absdiff = np.abs(diff[np.abs(diff) > 0])
                print(f"  max={np.max(absdiff):.8f}")
                print(f"  mean={np.mean(absdiff):.8f}")
                print(
                    f"  p50={np.percentile(absdiff, 50):.8f}, "
                    f"  p99={np.percentile(absdiff, 99):.8f}"
                )

                if var == "SNR":
                    within_025 = np.sum(absdiff <= 0.25)
                    print(
                        f"  Within 0.25 dB (SBF quant): "
                        f"{within_025}/{len(absdiff)} ({100 * within_025 / len(absdiff):.1f}%)"
                    )


if __name__ == "__main__":
    main()
