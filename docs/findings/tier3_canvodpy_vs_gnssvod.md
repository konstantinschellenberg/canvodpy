---
title: "Tier 3: canvodpy vs gnssvod (Humphrey et al.)"
description: External validation of canvodpy against the reference gnssvod implementation
---

# Tier 3: canvodpy vs gnssvod

External comparison of canvodpy against **gnssvod** (Humphrey et al.), the
reference GNSS-T VOD implementation. This is the highest tier of the audit
suite — it validates canvodpy against an independently developed tool on
identical input data.

---

## Methodology

### Test data

- **Site**: Rosalia (Austria), DOY 2025-001
- **Receivers**: canopy (ROSA01) + reference (ROSA02), Septentrio PolaRx5
- **Input**: 96 × 15-minute RINEX v3.04 files per receiver, spliced and trimmed
  to one observation code per band per constellation via **gfzrnx**

### Trimming (gfzrnx)

Both tools receive byte-identical RINEX files trimmed to:

| Constellation | Band | Obs code |
|---|---|---|
| GPS (G) | L1 | S1C, C1C, L1C |
| GPS (G) | L2 | S2W, C2W, L2W |
| Galileo (E) | L1 | S1C, C1C, L1C |
| Galileo (E) | L5 | S5Q, C5Q, L5Q |

Trimming eliminates the SID-vs-PRN ambiguity: with one code per band,
canvodpy's SID `G01|L1|C` maps 1:1 to gnssvod's PRN `G01`.

### Ephemeris

Both tools use byte-identical **GFZ rapid** SP3/CLK products
(`GFZ0MGXRAP`) downloaded to a shared directory. gnssvod forces GFZ rapid
for GPS week >= 2038; canvodpy is configured with `agency="GFZ",
product_type="rapid"` to match.

Although the SP3 files are identical, the tools use **different
interpolation algorithms**:

| Property | canvodpy | gnssvod |
|---|---|---|
| **Method** | `scipy.CubicHermiteSpline` (piecewise cubic) | `numpy.polyfit` degree-16 on 4-hour windows |
| **SP3 velocities** | Yes (used as Hermite derivatives) | No (velocities derived by finite differencing) |
| **Clock** | Piecewise linear with jump detection | Cubic resample or degree-2 polyfit |
| **Parallelism** | `ThreadPoolExecutor` per SV | Sequential loop |
| **Caching** | Zarr store (`aux_{date}.zarr`), rebuilt each run | None (in-memory DataFrame) |

This produces systematic (not random) differences in satellite ECEF
positions, and therefore in the derived azimuth, elevation, and zenith
angles.

### Elevation cutoff

gnssvod applies a hard cutoff at elevation <= -10° (drops rows).
canvodpy masks below-horizon observations to NaN but retains them.
The comparison aligns on shared (epoch, SID) pairs and computes
statistics on mutually valid values only.

---

## Results

### 3A: SNR comparison

| Band | RMSE (dB) | MAE (dB) | Max diff (dB) | Bias (dB) | Valid pairs |
|---|---|---|---|---|---|
| L1 (S1C) | 1.01 × 10⁻⁶ | 8.56 × 10⁻⁷ | 1.89 × 10⁻⁶ | -1.04 × 10⁻⁹ | 138,985 |
| L2 (S2W) | 7.70 × 10⁻⁷ | 5.95 × 10⁻⁷ | 1.89 × 10⁻⁶ | -7.00 × 10⁻¹⁰ | 106,414 |

**Root cause**: canvodpy stores SNR as `float32` (~7 significant digits),
gnssvod uses `float64`. The RINEX file contains SNR values with ~0.001 dB
precision (3 decimal places). `float32` truncation introduces max ~2 × 10⁻⁶ dB
error — **1000× below measurement resolution**. This is a deliberate design
choice: `float32` halves memory for the large `(epoch, sid)` arrays.

**Verdict**: Effectively bit-identical. No scientific impact.

### 3A: Angular comparison

| Variable | RMSE | MAE | Max diff | Bias | Valid pairs |
|---|---|---|---|---|---|
| Azimuth (°) | 0.00201 | 0.000131 | 0.105 | -4.74 × 10⁻⁵ | 138,984 |
| Elevation (°) | 0.000156 | 4.38 × 10⁻⁵ | 0.00478 | -7.84 × 10⁻⁶ | 138,984 |

**Root cause**: Different SP3 interpolation methods (see table above). Both
approaches are valid — Hermite cubic (canvodpy) uses more information
(SP3 velocities) while degree-16 polyfit (gnssvod) uses a longer fitting
window. The differences are systematic and reproducible.

**Verdict**: RMSE 0.002° is **1000× smaller** than the 2° grid cell size.

### 3B: VOD comparison

| Band | RMSE | MAE | Max diff | Bias | Valid pairs |
|---|---|---|---|---|---|
| VOD L1 | 0.00605 | 0.00453 | 0.0224 | -2.99 × 10⁻⁶ | 138,855 |
| VOD L2 | 0.00676 | 0.00528 | 0.0230 | 1.03 × 10⁻⁶ | 106,395 |

**Root cause**: The VOD formula is:

    VOD = -ln(10^((SNR_canopy - SNR_ref) / 10)) × cos(θ_canopy)

SNR is identical (same RINEX, same formula), so transmittance *T* is
identical. The only difference is in cos(θ), which inherits the angular
differences from SP3 interpolation. The bias is effectively zero (< 10⁻⁵),
confirming that both implementations compute VOD equivalently.

**Verdict**: RMSE 0.006 is well below typical VOD measurement uncertainty
(~0.1 for forest canopies).

### 3C: Grid cell assignment comparison

To determine whether angular differences affect the gridded VOD product,
both tools' observations were mapped to two different 2° grids:

| Grid | Same cell | Different cell | Agreement |
|---|---|---|---|
| gnssvod 2° equi-angular | 138,643 | **7** | 99.995% |
| canvodpy 2° equal-area | 138,979 | **5** | 99.996% |

Out of ~139,000 valid observation pairs, only **5–7 observations** land in
a different grid cell. These are boundary cases where the ~0.002° angular
difference pushes the observation across a cell edge.

**Verdict**: Grid cell assignment is unaffected. The angular differences
from different SP3 interpolation methods have **zero practical impact**
on the gridded VOD retrieval.

---

## Summary

| Observable | Status | Root cause | Scientific impact |
|---|---|---|---|
| **SNR** | PASS | float32 vs float64 dtype | None (1000× below measurement precision) |
| **Azimuth** | PASS | Different SP3 interpolation algorithms | None (1000× below grid resolution) |
| **Elevation** | PASS | Different SP3 interpolation algorithms | None (1000× below grid resolution) |
| **VOD** | PASS | Propagated θ difference through cos(θ) | None (below measurement uncertainty) |
| **Grid cells** | PASS | 5–7 boundary crossings out of 139k | None (99.996% agreement) |

canvodpy produces **scientifically equivalent results** to gnssvod
(Humphrey et al.) for identical input data and ephemeris products.

---

## Reproducibility

### Scripts

```bash
# Step 1–4: Main comparison (SNR, angles, VOD)
uv run python packages/canvod-audit/scripts/run_tier3_vs_gnssvod.py

# Step 4C: Grid cell comparison
uv run python packages/canvod-audit/scripts/run_tier3_grid_comparison.py
```

### Prerequisites

- `gfzrnx` installed at `/usr/local/bin/gfzrnx`
- `gnssvod` installed (`uv pip install gnssvod`)
- Test data submodule initialized
- External drive mounted at `/Volumes/ExtremePro/`

### Output

All outputs are written to:
`/Volumes/ExtremePro/canvod_audit_output/tier3_vs_gnssvod/Rosalia/`

| File | Contents |
|---|---|
| `canopy_trimmed.rnx` | gfzrnx-trimmed RINEX (canopy) |
| `reference_trimmed.rnx` | gfzrnx-trimmed RINEX (reference) |
| `trimming_description.txt` | gfzrnx parameters for reproducibility |
| `gnssvod_canopy_output.parquet` | gnssvod preprocessed canopy |
| `gnssvod_reference_output.parquet` | gnssvod preprocessed reference |
| `gnssvod_vod_output.parquet` | gnssvod VOD (L1 + L2) |
| `canvodpy_trimmed_store/` | canvodpy Zarr store (SNR, phi, theta, VOD) |
| `tier3_comparison_stats.csv` | Per-variable comparison statistics |
| `tier3c_grid_comparison.csv` | Grid cell assignment comparison |
| `shared_aux/` | Shared GFZ rapid SP3/CLK files |
