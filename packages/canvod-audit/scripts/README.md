# Audit scripts

Reproducible scripts for each audit comparison. Run with `uv run python <script>`.

## Execution order

Run in this order to reproduce the full audit from scratch:

### Tier 0 (RINEX config — see `produce_canvodpy_store.py` docstring)

1. `uv run python produce_canvodpy_store.py` — creates the canvodpy RINEX store
2. `uv run python run_tier0_vs_gnssvodpy.py` — compares canvodpy vs gnssvodpy RINEX
3. `uv run python run_tier0_vod.py` — compares canvodpy vs gnssvodpy VOD (auto-computes if needed)
4. `uv run python run_round_trip.py` — verifies store round-trip integrity

### Tier 1 (SBF config — see `produce_sbf_store_final.py` docstring)

5. `uv run python produce_sbf_store_final.py` — creates SBF store with agency ephemeris
6. `uv run python run_tier1_sbf_vs_rinex.py` — compares SBF vs RINEX stores
7. `uv run python produce_sbf_store_broadcast.py` — creates SBF store with broadcast ephemeris
8. `uv run python run_tier1_broadcast_vs_agency.py` — compares broadcast vs agency ephemeris

## Scripts

### `produce_canvodpy_store.py` — Store production

Runs `process_date('Rosalia', '2025001')` to process 192 RINEX v3.04 files
(96 canopy + 96 reference) through the canvodpy pipeline with agency ephemeris
(SP3/CLK from CODE). Produces an Icechunk store with groups `canopy_01` and
`reference_01_canopy_01`, each with dims `(epoch=17280, sid=321)` and variables
`SNR`, `phi`, `theta`.

Requires config changes documented in the script's docstring.

### `run_tier0_vs_gnssvodpy.py` — Tier 0: canvodpy vs gnssvodpy

Three-part comparison:

- **Part 1**: Automatic audit via `audit_vs_gnssvodpy()`. Discovers shared groups
  (finds `canopy_01`) and compares at EXACT tier. Expected result: **PASS** —
  SNR, phi, theta are bit-identical.

- **Part 2**: Manual reference-group comparison. canvodpy stores the reference as
  `reference_01_canopy_01`, gnssvodpy as `reference_01`. Compares at EXACT tier.
  Expected result: **FAIL** on phi and theta — known ~20 arcsecond difference from
  non-deterministic Hermite interpolation across independent runs. SNR is bit-identical.

- **Part 3**: Deep-dive statistics for the reference group. Reports per-variable:
  count of non-zero diffs, max/mean/percentile absolute differences, NaN disagreement
  counts, and 2π wrap-around cases for phi.

Expected output (2026-03-10):

```
canopy_01 (auto):  PASS — all three variables bit-identical
reference (manual): FAIL —
  SNR:   0 / 1,896,979 non-zero (0.00%)
  phi:   1,971,909 / 1,971,909 non-zero (100%), max 6.28 rad (2π wrap), mean 1.2e-4 rad
  theta: 1,971,909 / 1,971,909 non-zero (100%), max 1.2e-4 rad (~20 arcsec)
  NaN disagreement: 165 cells (133 canvodpy-extra, 32 gnssvodpy-extra)
  2π wrap-around: 9 cells
```

### `run_tier0_vod.py` — Tier 0: VOD comparison

Compares VOD, phi, and theta from the canvodpy VOD store against the gnssvodpy
truth VOD store. Group names differ between tools: canvodpy uses
`canopy_01_vs_reference_01`, gnssvodpy uses `reference_01_canopy_01`.

If the canvodpy VOD store is empty, the script computes VOD automatically via
`site.vod.compute_bulk()`.

Expected result: **PASS** — all three variables (VOD, phi, theta) bit-identical.
1,226,667 VOD values, 1,972,042 phi/theta values compared.

### `run_round_trip.py` — Infrastructure: store round-trip

Reads each group from the canvodpy store, writes to a temporary NetCDF file,
reads back, and verifies bit-identical data via `audit_store_round_trip()`.

Expected result: **2/2 PASS** (canopy_01 + reference_01_canopy_01).

### `produce_sbf_store_final.py` — SBF store production (agency ephemeris)

Runs `process_date('Rosalia', '2025001')` on 378 SBF files (189 canopy + 189
reference) with SP3/CLK final products. Requires config changes to point to
SBF test data and `tier1_sbf_vs_rinex` output directory.

### `produce_sbf_store_broadcast.py` — SBF store production (broadcast ephemeris)

Same as above but with `ephemeris_source: broadcast`, using satellite positions
from SBF SatVisibility records instead of SP3/CLK files. Output goes to
`tier1_broadcast_vs_agency`.

### `run_tier1_sbf_vs_rinex.py` — Tier 1: SBF vs RINEX

Compares SBF and RINEX stores (both with all variables: SNR, Doppler, Phase,
Pseudorange, phi, theta) after snapping SBF epochs to the RINEX grid (constant
2.000 s receiver clock bias offset).

Key finding: phi/theta are bit-identical. All observables (SNR, Doppler, Phase,
Pseudorange) differ because the Septentrio RINEX converter applies a receiver
clock correction (`c × dT`) to epochs and observables.

### `run_tier1_broadcast_vs_agency.py` — Tier 1: broadcast vs agency ephemeris

Compares SBF stores produced with broadcast vs agency ephemeris. SNR should be
identical (ephemeris does not affect observables). phi/theta expected to differ
by ~0.001–0.01 deg from ~1–2 m orbit accuracy difference.

## Store layout

All audit stores live under `/Volumes/ExtremePro/canvod_audit_output/`, organized by scenario:

```
canvod_audit_output/
├── gnssvodpy_based/                              # truth stores (pre-existing)
│   ├── gnssvodpy_Rinex_Icechunk_Store/
│   └── gnssvodpy_VOD_Icechunk_Store/
├── tier0_rinex_vs_gnssvodpy/                     # Tier 0
│   └── Rosalia/
│       ├── canvodpy_RINEX_store/
│       └── canvodpy_VOD_store/
├── tier1_sbf_vs_rinex/                           # Tier 1: SBF vs RINEX
│   └── Rosalia/
│       └── canvodpy_SBF_store/                   # (agency ephemeris)
└── tier1_broadcast_vs_agency/                    # Tier 1: ephemeris sources
    └── Rosalia/
        └── canvodpy_SBF_broadcast_store/
```

| Store | Path | Groups |
|-------|------|--------|
| canvodpy RINEX | `.../tier0_rinex_vs_gnssvodpy/Rosalia/canvodpy_RINEX_store` | `canopy_01`, `reference_01_canopy_01` |
| canvodpy VOD | `.../tier0_rinex_vs_gnssvodpy/Rosalia/canvodpy_VOD_store` | `canopy_01_vs_reference_01` |
| gnssvodpy RINEX (truth) | `.../gnssvodpy_based/gnssvodpy_Rinex_Icechunk_Store` | `canopy_01`, `reference_01` |
| gnssvodpy VOD (truth) | `.../gnssvodpy_based/gnssvodpy_VOD_Icechunk_Store` | `reference_01_canopy_01` |
| canvodpy RINEX (allvars) | `.../tier1_sbf_vs_rinex/Rosalia/canvodpy_RINEX_allvars_store` | `canopy_01`, `reference_01_canopy_01` |
| canvodpy SBF (allvars) | `.../tier1_sbf_vs_rinex/Rosalia/canvodpy_SBF_allvars_store` | `canopy_01`, `reference_01_canopy_01` |
| canvodpy SBF (broadcast) | `.../tier1_broadcast_vs_agency/Rosalia/canvodpy_SBF_broadcast_store` | `canopy_01`, `reference_01_canopy_01` |

## Input data

### RINEX v3.04

Test files: `packages/canvod-readers/tests/test_data/valid/rinex_v3_04/01_Rosalia/`

| Directory | Files | Receiver |
|-----------|-------|----------|
| `01_reference/01_GNSS/01_raw/25001/` | 96 × ROSR01TUW_R_*.rnx | Reference (open sky) |
| `02_canopy/01_GNSS/01_raw/25001/` | 96 × ROSA01TUW_R_*.rnx | Canopy (under vegetation) |
| `01_SP3/` | `COD0MGXFIN_20250010000_01D_05M_ORB.SP3` | CODE final orbits |
| `02_CLK/` | `COD0MGXFIN_20250010000_01D_30S_CLK.CLK` | CODE final clocks |

### SBF

Test files: `packages/canvod-readers/tests/test_data/valid/sbf/01_Rosalia/`

| Directory | Files | Receiver |
|-----------|-------|----------|
| `01_reference/25001/` | 189 × ROSR01TUW_R_*.sbf | Reference (open sky) |
| `02_canopy/25001/` | 189 × ROSA01TUW_R_*.sbf | Canopy (under vegetation) |
