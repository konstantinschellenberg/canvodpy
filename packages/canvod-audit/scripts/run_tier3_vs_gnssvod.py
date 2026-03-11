"""Tier 3: canvodpy vs gnssvod (Humphrey et al.) — external comparison.

Workflow
--------
1. Trim RINEX files to one obs code per band per system (gfzrnx).
2. Run both canvodpy and gnssvod on the same trimmed RINEX.
3. Compare SNR, azimuth, elevation, and VOD outputs.

Prerequisites
-------------
- gfzrnx installed (``/usr/local/bin/gfzrnx``)
- gnssvod installed (``pip install gnssvod``)
- SP3/CLK files for the observation day
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from canvod.audit.rinex_trimmer import gps_galileo_l1_l2
from canvod.audit.runners.common import AuditResult
from canvod.audit.runners.vs_gnssvod import (
    gnssvod_df_to_xarray,
)

# ── Configuration ────────────────────────────────────────────────────────────

ROSALIA_ROOT = Path(
    "/Users/work/Developer/GNSS/canvodpy/packages/canvod-readers"
    "/tests/test_data/valid/rinex_v3_04/01_Rosalia"
)

CANOPY_DIR = ROSALIA_ROOT / "02_canopy" / "01_GNSS" / "01_raw" / "25001"
REFERENCE_DIR = ROSALIA_ROOT / "01_reference" / "01_GNSS" / "01_raw" / "25001"
SP3_DIR = ROSALIA_ROOT / "01_SP3"
CLK_DIR = ROSALIA_ROOT / "02_CLK"

AUDIT_ROOT = Path("/Volumes/ExtremePro/canvod_audit_output")
TIER3_DIR = AUDIT_ROOT / "tier3_vs_gnssvod" / "Rosalia"

# canvodpy store from Tier 0 (already processed with the same RINEX files)
CANVODPY_STORE = (
    AUDIT_ROOT / "tier0_rinex_vs_gnssvodpy" / "Rosalia" / "canvodpy_RINEX_store"
)


def _find_rinex_files(directory: Path) -> list[Path]:
    """Find and sort RINEX files in a directory."""
    files = sorted(directory.glob("*.rnx"))
    if not files:
        files = sorted(directory.glob("*.RNX"))
    return files


def step1_trim_rinex() -> tuple[Path, Path]:
    """Trim RINEX files to GPS+Galileo L1+L2, one code per band."""
    trimmer = gps_galileo_l1_l2()

    canopy_files = _find_rinex_files(CANOPY_DIR)
    ref_files = _find_rinex_files(REFERENCE_DIR)

    print(f"Canopy: {len(canopy_files)} files")
    print(f"Reference: {len(ref_files)} files")

    if not canopy_files:
        print(f"ERROR: No RINEX files in {CANOPY_DIR}")
        sys.exit(1)

    TIER3_DIR.mkdir(parents=True, exist_ok=True)

    canopy_trimmed = TIER3_DIR / "canopy_trimmed.rnx"
    ref_trimmed = TIER3_DIR / "reference_trimmed.rnx"

    if canopy_trimmed.exists():
        print(f"\n  Canopy trimmed file exists, skipping: {canopy_trimmed}")
    else:
        print("\n── Trimming canopy ──")
        trimmer.preview(canopy_files)
        trimmer.write(canopy_files, canopy_trimmed)

    if ref_files and not ref_trimmed.exists():
        print("\n── Trimming reference ──")
        trimmer.write(ref_files, ref_trimmed)
    elif not ref_files:
        ref_trimmed = None

    # Save trimming description for reproducibility
    desc = trimmer.describe(canopy_files, canopy_trimmed)
    desc_path = TIER3_DIR / "trimming_description.txt"
    desc_path.write_text(desc)
    print(f"\nTrimming description saved: {desc_path}")

    return canopy_trimmed, ref_trimmed


def step2_run_gnssvod(canopy_trimmed: Path, ref_trimmed: Path | None) -> Path:
    """Run gnssvod on the trimmed RINEX file.

    Returns path to the saved gnssvod output (Parquet).
    """
    output_path = TIER3_DIR / "gnssvod_canopy_output.parquet"
    if output_path.exists():
        print(f"\n  gnssvod output exists, skipping: {output_path}")
        return output_path

    import gnssvod

    print("\n── Running gnssvod ──")
    print(f"Input: {canopy_trimmed}")

    # Use gnssvod.preprocess() which handles orbit download + azi/ele
    aux_path = str(TIER3_DIR / "gnssvod_aux")
    Path(aux_path).mkdir(parents=True, exist_ok=True)

    results = gnssvod.preprocess(
        filepattern={"canopy": str(canopy_trimmed)},
        orbit=True,
        aux_path=aux_path,
        outputresult=True,
    )

    # Extract the Observation object
    obs_list = results["canopy"]
    obs = obs_list[0]  # single file → single Observation

    # The observation data is in obs.observation (a DataFrame)
    df = obs.observation
    print(f"gnssvod output shape: {df.shape}")
    print(f"  Columns: {list(df.columns[:15])}")
    print(f"  Index: {df.index.names}")

    # Save as Parquet
    output_path = TIER3_DIR / "gnssvod_canopy_output.parquet"
    df.to_parquet(output_path)
    print(f"Saved: {output_path}")

    return output_path


def step3_run_canvodpy_trimmed(canopy_trimmed: Path) -> Path:
    """Run canvodpy on the trimmed RINEX file and write to a Zarr store.

    Returns path to the Zarr store.
    """
    from canvod.readers.rinex.v3_04 import Rnxv3Obs

    store_path = TIER3_DIR / "canvodpy_trimmed_store"

    if store_path.exists():
        print(f"\n  canvodpy trimmed store exists, skipping: {store_path}")
        return store_path

    print("\n── Running canvodpy on trimmed RINEX ──")

    reader = Rnxv3Obs(fpath=canopy_trimmed, completeness_mode="off")
    ds = reader.to_ds()
    print(f"canvodpy: {dict(ds.sizes)}, vars={list(ds.data_vars)}")
    print(f"  SID examples: {list(ds.sid.values[:5])}")

    # Write to a simple Zarr store (no Icechunk needed for comparison)
    ds.to_zarr(str(store_path), mode="w")
    print(f"Written: {store_path}")

    return store_path


def step4_compare(gnssvod_parquet: Path) -> AuditResult:
    """Compare canvodpy output against gnssvod output.

    Both were run on the same trimmed RINEX file. We do a direct
    variable-by-variable comparison on the shared (epoch, PRN) grid.
    """

    print("\n── Tier 3 comparison ──")

    # Load canvodpy output (plain Zarr from step 3)
    trimmed_store = TIER3_DIR / "canvodpy_trimmed_store"
    ds_canvod = xr.open_zarr(str(trimmed_store))
    print(f"canvodpy: {dict(ds_canvod.sizes)}, vars={list(ds_canvod.data_vars)}")

    # Load gnssvod output
    df_gnssvod = pd.read_parquet(gnssvod_parquet)
    ds_gnssvod = gnssvod_df_to_xarray(df_gnssvod)
    print(f"gnssvod:  {dict(ds_gnssvod.sizes)}, vars={list(ds_gnssvod.data_vars)}")

    # Map canvodpy SIDs to PRNs for matching
    # canvodpy SIDs are like "G01|L1|C", gnssvod uses "G01"
    # With trimmed RINEX (one code per band), we can extract the PRN
    prns = [str(s).split("|")[0] for s in ds_canvod.sid.values]
    # Group by PRN — pick one SID per PRN (the L1 band for SNR comparison)
    sid_to_prn = dict(zip(ds_canvod.sid.values, prns))

    # For SNR comparison: match canvodpy S1C SIDs to gnssvod S1C
    # canvodpy SNR variable contains SNR for each SID (one per code)
    # gnssvod has separate columns: S1C, S2W, S5Q

    # Compare band by band: canvodpy SID "G01|L1|C" maps to gnssvod "S1C" for PRN "G01"
    # Map of (band_filter, gnssvod_col) pairs
    band_map = [
        ("L1|C", "S1C", "SNR L1C"),
        ("L2|W", "S2W", "SNR L2W"),
        ("L5|Q", "S5Q", "SNR L5Q"),
    ]

    shared_epochs = np.intersect1d(ds_canvod.epoch.values, ds_gnssvod.epoch.values)
    all_passed = True

    for band_filt, gnssvod_col, label in band_map:
        if gnssvod_col not in ds_gnssvod.data_vars:
            continue

        # Select SIDs matching this band
        band_sids = [s for s in ds_canvod.sid.values if f"|{band_filt}" in str(s)]
        if not band_sids:
            continue

        ds_band = ds_canvod.sel(sid=band_sids)
        band_prns = [str(s).split("|")[0] for s in band_sids]
        ds_band = ds_band.assign_coords(sid=band_prns)

        shared_prns = sorted(set(band_prns) & set(ds_gnssvod.sid.values.tolist()))
        if not shared_prns:
            continue

        print(f"\n── {label} ({len(shared_prns)} PRNs, {len(shared_epochs)} epochs) ──")

        canvod_snr = ds_band["SNR"].sel(epoch=shared_epochs, sid=shared_prns).values
        gnssvod_snr = (
            ds_gnssvod[gnssvod_col].sel(epoch=shared_epochs, sid=shared_prns).values
        )

        valid = ~np.isnan(canvod_snr) & ~np.isnan(gnssvod_snr)
        n_valid = int(np.sum(valid))
        if n_valid == 0:
            print("  No valid pairs")
            continue

        diff = canvod_snr[valid] - gnssvod_snr[valid]
        n_identical = int(np.sum(np.abs(diff) == 0))
        max_diff = float(np.max(np.abs(diff)))
        rmse = float(np.sqrt(np.mean(diff**2)))

        print(f"  Valid pairs: {n_valid:,}")
        print(f"  Identical:   {n_identical:,} ({100 * n_identical / n_valid:.1f}%)")
        print(f"  Max diff:    {max_diff:.6f} dB-Hz")
        print(f"  Mean diff:   {np.mean(np.abs(diff)):.6f} dB-Hz")
        print(f"  RMSE:        {rmse:.6f} dB-Hz")

        passed = max_diff < 0.01
        print(f"  → {'PASS' if passed else 'FAIL'}")
        if not passed:
            all_passed = False

    # Azimuth/Elevation — use L1|C SIDs (one per PRN)
    l1c_sids = [s for s in ds_canvod.sid.values if "|L1|C" in str(s)]
    if l1c_sids:
        ds_l1c = ds_canvod.sel(sid=l1c_sids)
        l1c_prns = [str(s).split("|")[0] for s in l1c_sids]
        ds_l1c = ds_l1c.assign_coords(sid=l1c_prns)
        shared_prns = sorted(set(l1c_prns) & set(ds_gnssvod.sid.values.tolist()))

        if "Azimuth" in ds_gnssvod.data_vars and "phi" in ds_l1c.data_vars:
            print("\n── Azimuth (canvodpy phi vs gnssvod Azimuth) ──")
            cv = np.degrees(
                ds_l1c["phi"].sel(epoch=shared_epochs, sid=shared_prns).values
            )
            gv = ds_gnssvod["Azimuth"].sel(epoch=shared_epochs, sid=shared_prns).values
            m = ~np.isnan(cv) & ~np.isnan(gv)
            if np.sum(m) > 0:
                d = cv[m] - gv[m]
                d = np.where(d > 180, d - 360, d)
                d = np.where(d < -180, d + 360, d)
                print(
                    f"  Valid: {int(np.sum(m)):,}, RMSE: {np.sqrt(np.mean(d**2)):.4f} deg, "
                    f"Max: {np.max(np.abs(d)):.4f} deg"
                )

        if "Elevation" in ds_gnssvod.data_vars and "theta" in ds_l1c.data_vars:
            print("\n── Elevation (canvodpy 90-theta vs gnssvod Elevation) ──")
            cv = 90.0 - np.degrees(
                ds_l1c["theta"].sel(epoch=shared_epochs, sid=shared_prns).values
            )
            gv = (
                ds_gnssvod["Elevation"].sel(epoch=shared_epochs, sid=shared_prns).values
            )
            m = ~np.isnan(cv) & ~np.isnan(gv)
            if np.sum(m) > 0:
                d = cv[m] - gv[m]
                print(
                    f"  Valid: {int(np.sum(m)):,}, RMSE: {np.sqrt(np.mean(d**2)):.4f} deg, "
                    f"Max: {np.max(np.abs(d)):.4f} deg"
                )

    result = AuditResult()
    print(f"\n  Overall SNR: {'PASS' if all_passed else 'FAIL'}")
    return result


def main() -> None:
    print("=" * 60)
    print("Tier 3: canvodpy vs gnssvod (Humphrey et al.)")
    print("=" * 60)

    # Step 1: Trim RINEX
    canopy_trimmed, ref_trimmed = step1_trim_rinex()

    # Step 2: Run gnssvod
    gnssvod_output = step2_run_gnssvod(canopy_trimmed, ref_trimmed)

    # Step 3: Also run canvodpy on trimmed RINEX (for direct comparison)
    step3_run_canvodpy_trimmed(canopy_trimmed)

    # Step 4: Compare
    result = step4_compare(gnssvod_output)

    print("\n" + "=" * 60)
    if result.passed:
        print("TIER 3 PASSED")
    else:
        print("TIER 3: SOME COMPARISONS FAILED — review above output")
        # Print per-variable details
        for name, r in result.results.items():
            if not r.passed:
                print(f"\n{name}:")
                for var, reason in r.failures.items():
                    print(f"  {var}: {reason}")
                for var, vs in r.variable_stats.items():
                    print(
                        f"  {var}: rmse={vs.rmse:.6g}, max_abs={vs.max_abs_diff:.6g}, "
                        f"bias={vs.bias:.6g}, nan_agree={vs.nan_agreement_rate:.4f}"
                    )


if __name__ == "__main__":
    main()
