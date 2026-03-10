"""Tier 0: canvodpy vs gnssvodpy — RINEX store comparison.

Compares the canvodpy RINEX Icechunk store against the gnssvodpy truth store.
Also performs a manual reference-group comparison (group names differ between tools).

Stores used
-----------
canvodpy RINEX : produced by ``process_date('Rosalia', '2025001')``
    with ``config/sites.yaml`` pointing to test RINEX data and
    ``config/processing.yaml`` set to ``ephemeris_source: final``.
gnssvodpy RINEX : produced by gnssvodpy from the same RINEX files in a
    prior session.

Results (2026-03-10)
--------------------
- Canopy group: bit-identical (SNR, phi, theta all zero-diff)
- Reference group: ~1e-4 rad (~20 arcsec) phi/theta diff on 100% of cells
  due to non-deterministic Hermite interpolation across independent runs.
  canvodpy confirmed correct by recomputation.
"""

from __future__ import annotations

import numpy as np

from canvod.audit.core import compare_datasets
from canvod.audit.runners import audit_vs_gnssvodpy
from canvod.audit.runners.common import load_group, open_store
from canvod.audit.tolerances import ToleranceTier

# ── Store paths ──────────────────────────────────────────────────────────
CANVODPY_RINEX = (
    "/Volumes/ExtremePro/canvod_audit_output"
    "/tier0_rinex_vs_gnssvodpy/Rosalia/canvodpy_RINEX_store"
)
GNSSVODPY_RINEX = (
    "/Volumes/ExtremePro/canvod_audit_output"
    "/gnssvodpy_based/gnssvodpy_Rinex_Icechunk_Store"
)

# ── Part 1: automatic audit (shared groups) ──────────────────────────────
print("=" * 60)
print("Part 1: audit_vs_gnssvodpy (auto-discovered shared groups)")
print("=" * 60)

result = audit_vs_gnssvodpy(
    canvodpy_rinex=CANVODPY_RINEX,
    gnssvodpy_rinex=GNSSVODPY_RINEX,
)
print(result.summary())

for name, r in result.results.items():
    print(f"\n--- {name} ---")
    print(f"Tier: {r.tier}, Passed: {r.passed}")
    print(f"Alignment: {r.alignment}")
    for vname, vs in r.variable_stats.items():
        print(
            f"  {vname}: rmse={vs.rmse:.6g}, max_abs={vs.max_abs_diff:.6g}, "
            f"nan_agree={vs.nan_agreement_rate:.6f}"
        )

# ── Part 2: manual reference-group comparison ────────────────────────────
# canvodpy stores reference as "reference_01_canopy_01",
# gnssvodpy stores it as "reference_01".
print("\n" + "=" * 60)
print("Part 2: reference group (manual, different group names)")
print("=" * 60)

s_canv = open_store(CANVODPY_RINEX)
s_gnss = open_store(GNSSVODPY_RINEX)

ds_canv = load_group(s_canv, "reference_01_canopy_01")
ds_gnss = load_group(s_gnss, "reference_01")

r = compare_datasets(
    ds_canv,
    ds_gnss,
    tier=ToleranceTier.EXACT,
    label="reference: canvodpy vs gnssvodpy",
)

print(f"Passed: {r.passed}")
print(f"Failures: {r.failures}")
for vname, vs in r.variable_stats.items():
    print(
        f"  {vname}: rmse={vs.rmse:.6g}, max_abs={vs.max_abs_diff:.6g}, "
        f"bias={vs.bias:.6g}, nan_agree={vs.nan_agreement_rate:.6f}"
    )

# ── Part 3: reference phi/theta deep dive ────────────────────────────────
print("\n" + "=" * 60)
print("Part 3: reference phi/theta deep dive")
print("=" * 60)

shared_epochs = np.intersect1d(ds_canv.epoch.values, ds_gnss.epoch.values)
c = ds_canv.sel(epoch=shared_epochs)
g = ds_gnss.sel(epoch=shared_epochs)

for var in ["SNR", "phi", "theta"]:
    cv = c[var].values
    gv = g[var].values
    both_valid = ~np.isnan(cv) & ~np.isnan(gv)
    diff = cv[both_valid] - gv[both_valid]
    nonzero_mask = np.abs(diff) > 0
    nonzero = int(np.sum(nonzero_mask))
    total = len(diff)
    print(f"\n{var}: {nonzero}/{total} non-zero ({100 * nonzero / total:.2f}%)")
    if nonzero > 0:
        absdiff = np.abs(diff[nonzero_mask])
        print(
            f"  max={np.max(absdiff):.8f} rad ({np.degrees(np.max(absdiff)):.4f} deg)"
        )
        print(f"  mean={np.mean(absdiff):.8f} rad")
        print(
            f"  p50={np.percentile(absdiff, 50):.8f}, "
            f"  p99={np.percentile(absdiff, 99):.8f}"
        )

    # NaN disagreement
    nan_c = np.isnan(cv)
    nan_g = np.isnan(gv)
    canv_extra = int(np.sum(~nan_c & nan_g))
    gnss_extra = int(np.sum(nan_c & ~nan_g))
    if canv_extra + gnss_extra > 0:
        print(
            f"  NaN disagreement: canvodpy has {canv_extra} extra, gnssvodpy has {gnss_extra} extra"
        )

    # 2pi wrap-around check (phi only)
    if var == "phi":
        mask_2pi = np.abs(np.abs(diff) - 2 * np.pi) < 0.01
        n_wrap = int(np.sum(mask_2pi))
        print(f"  2pi wrap-around cases: {n_wrap}")
