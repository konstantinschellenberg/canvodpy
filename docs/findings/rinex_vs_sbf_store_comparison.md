# Multi-Layer Store Comparison Report

**Date**: 2026-03-07

## Stores Compared

| Store | Path |
|---|---|
| gnssvodpy RINEX | `/Volumes/ExtremePro/comparison_stores_readers_packages/gnssvodpy_Rinex_Icechunk_Store` |
| gnssvodpy VOD | `/Volumes/ExtremePro/comparison_stores_readers_packages/gnssvodpy_Rinex_VOD_Icechunk_Store` |
| canvodpy RINEX | `/Volumes/ExtremePro/comparison_stores_readers_packages/Rosalia/canvodpy_Rinex_Icechunk_Store` |
| canvodpy VOD | `/Volumes/ExtremePro/comparison_stores_readers_packages/Rosalia/canvodpy_Rinex_VOD_Icechunk_Store` |
| canvodpy SBF | `/Volumes/ExtremePro/comparison_stores_readers_packages/Rosalia/canvodpy_SBF_Icechunk_Store` |

**Source data**: Same Septentrio receivers, same 7-day period (2025-01-01 to 2025-01-07),
5-second logging interval. Each day has 96 x 15-min files in both `.25o` (RINEX) and `.25_` (SBF) formats.
Day 25001 (Jan 1) also contains a daily concatenation file (`ract0010_daily.25o`).

---

## Layer 1: gnssvodpy vs canvodpy (RINEX) — Truth Comparison

gnssvodpy is the reference ("truth") implementation.

### RINEX Store

| Metric | gnssvodpy | canvodpy | Match? |
|---|---|---|---|
| reference group epochs | 120,960 | 120,960 | Yes |
| reference group SIDs | 321 | 321 | Yes |
| canopy group epochs | 138,240 (17,280 dup) | 138,240 (17,280 dup) | Yes (both have bug) |
| SNR values | - | - | **Identical** (0 diff, 0 NaN mismatch) |
| canopy phi/theta | - | - | **Identical** (0 diff) |
| reference phi | - | - | max wrap-aware diff = 0.042 rad (2.43 deg), 64,374 > 0.001 rad |
| reference theta | - | - | max diff = 0.000117 rad (0.007 deg) |
| reference phi/theta NaN mismatch | - | - | 1,346 cells |

### VOD Store

| Metric | gnssvodpy | canvodpy | Match? |
|---|---|---|---|
| Epochs | 120,960 | 120,960 | Yes |
| SIDs | 321 | 321 | Yes |
| **VOD values** | - | - | **Identical** (0 diff, 0 NaN mismatch) |
| **phi values** | - | - | **Identical** (0 diff) |
| **theta values** | - | - | **Identical** (0 diff) |

**Conclusion**: canvodpy reproduces gnssvodpy results exactly for SNR, canopy phi/theta,
and VOD. The phi/theta differences exist only in the **reference** receiver group and come
from a known coordinate conversion difference between the packages (slightly different
ECEF-to-spherical implementation). These differences do not propagate to the VOD store
because the VOD calculator (`calculator.py:232-239`) uses `canopy_ds["phi"]` and
`canopy_ds["theta"]`, not the reference receiver's angles.

---

## Layer 2: canvodpy RINEX vs SBF — Format Comparison

See [detailed SBF findings](#sbf-specific-findings) below.

---

## Issue Summary

| # | Issue | Severity | Affects | Status |
|---|---|---|---|---|
| 1 | [Duplicate epochs in canopy_01](#1-duplicate-epochs-critical) | **CRITICAL** | gnssvodpy + canvodpy | Bug in overlap detection |
| 2 | [SBF SID coordinates empty (235/321)](#2-sbf-sid-coordinates-empty) | **HIGH** | canvodpy SBF | Bug in SBF reader |
| 3 | [SNR quantization (0.25 dB)](#3-snr-quantization-difference) | **INFO** | canvodpy SBF | Expected (format limitation) |
| 4 | [Epoch offset (~2s)](#4-epoch-offset) | **INFO** | canvodpy SBF | Expected (receiver clock) |
| 5 | [Chunk shape inconsistency](#5-chunk-shape-inconsistency) | **MEDIUM** | canvodpy SBF | Design gap |
| 6 | [Attribute schema mismatch](#6-attribute-schema-mismatch) | **LOW** | canvodpy SBF | Cosmetic |

---

## 1. Duplicate Epochs (CRITICAL)

### Observation

**Both gnssvodpy AND canvodpy** have duplicate epochs in `canopy_01`:

```
gnssvodpy canopy_01: 138,240 epochs, 120,960 unique  *** DUPLICATES ***
canvodpy  canopy_01: 138,240 epochs, 120,960 unique  *** DUPLICATES ***
```

January 1st appears twice (34,560 = 2 x 17,280). The epoch index is non-monotonic:
```
idx 17279: 2025-01-01T23:59:55 -> 2025-01-01T00:00:00  (backward jump)
```

Reference groups and VOD stores are clean (120,960 unique epochs each). The reference
data directory does not contain a daily concatenation file.

### Root Cause

The canopy data directory contains **both** a daily file and the 15-min files:
```
02_canopy/25001/
  ract0010_daily.25o    <-- daily concatenation (17,280 epochs, hash=96c26969ebfbf18c)
  ract001a00.25o        <-- 15-min file (180 epochs, hash=3bdd24df025d1e2e)
  ract001a15.25o        <-- (180 epochs, hash=492f389c471938a6)
  ...96 more 15-min files...
```

**The overlap detection has three layers, all of which failed:**

#### Layer 1: `metadata_row_exists()` — exact `(start, end)` match

The function (`store.py:1497`) checks for an exact match of `(start, end)`:
```python
matches = df.filter(
    (pl.col("start") == start) & (pl.col("end") == end)
)
```

The daily file has `(start=00:00:00, end=23:59:55)`.
The 15-min files have `(start=00:00:00, end=00:14:55)`, `(start=00:15:00, end=00:29:55)`, etc.

**These are different tuples, so no overlap is detected.**

#### Layer 2: `batch_check_existing()` — hash lookup

The function (`store.py:1581`) checks if a file hash already exists:
```python
existing = df.filter(pl.col("rinex_hash").is_in(file_hashes))
```

The daily file has a unique hash (`96c26969ebfbf18c`), so it passes this check.

#### Layer 3: Strategy logic — `(exists, strategy)` match

The store strategy (`skip`/`overwrite`/`append`) only acts on the `exists` flag from
`metadata_row_exists()`. Since `exists=False` for non-matching `(start, end)`, the
daily file is treated as new data and appended.

**None of these layers perform temporal overlap detection.**

### Evidence from Metadata Tables

Both stores have 673 metadata entries for `canopy_01`:
- Entry [0]: `ract0010_daily.25o`, start=2025-01-01T00:00:00, end=2025-01-01T23:59:55
- Entries [1-96]: 15-min files covering 2025-01-01T00:00:00 to 2025-01-01T23:59:55
- Entries [97-672]: 15-min files for days 2-7

All 673 entries have `exists='False'` — meaning every file passed the deduplication check.

### Impact

- Non-monotonic epoch index breaks `xarray.sel(epoch=slice(...))` operations
- Duplicate epochs cause double-counting in temporal aggregation and statistics
- VOD calculations on duplicated data would be incorrect (VOD store is clean because
  it only uses the reference group which has no daily file)

### Recommended Fix: Temporal Overlap Detection

The metadata table already has `start` and `end` columns. Add a temporal overlap check
to `metadata_row_exists()` or create a new method `check_temporal_overlap()`:

```python
def check_temporal_overlap(
    self,
    group_name: str,
    start: np.datetime64,
    end: np.datetime64,
    branch: str = "main",
) -> tuple[bool, pl.DataFrame]:
    """Check if incoming [start, end] overlaps any existing metadata interval."""
    with self.readonly_session(branch) as session:
        try:
            zmeta = zarr.open_group(session.store, mode="r")[
                f"{group_name}/metadata/table"
            ]
        except Exception:
            return False, pl.DataFrame()

        data = {col: zmeta[col][:] for col in zmeta.array_keys()}
        df = pl.DataFrame(data).with_columns([
            pl.col("start").cast(pl.Datetime("ns")),
            pl.col("end").cast(pl.Datetime("ns")),
        ])

        # Standard interval overlap: [A.start, A.end] overlaps [B.start, B.end]
        # iff A.start <= B.end AND A.end >= B.start
        overlaps = df.filter(
            (pl.col("start") <= np.datetime64(end, "ns"))
            & (pl.col("end") >= np.datetime64(start, "ns"))
        )

        if overlaps.is_empty():
            return False, overlaps

        return True, overlaps
```

This should be called **before** the strategy match block. If overlap is detected:
- For `skip` strategy: skip the file and log a warning with the overlapping intervals
- For `overwrite` strategy: proceed (overwrite is intentional)
- For `append` strategy: trim the incoming dataset to non-overlapping epochs

The check uses only the metadata table (no data reads), so it's cheap.

---

## 2. SBF SID Coordinates Empty

### Observation

In the SBF store, **235 out of 321 SIDs have empty/NaN coordinate metadata** (band, code,
sv, system, freq_center, freq_min, freq_max). Only 86 SIDs have populated coordinates.

In the RINEX store, all 321 SIDs with actual observation data have populated coordinates.

### Root Cause

The two readers use fundamentally different SID discovery strategies:

**RINEX reader** -- header-based discovery (`v3_04.py:_precompute_sids_from_header()`):
- Reads the RINEX header which declares ALL observation codes for each constellation upfront
- Pre-computes coordinates for every theoretical SV x band x code combination
- Result: ~300-350 SIDs with populated coordinates per file, even if no observations exist for some

**SBF reader** -- observation-based discovery (`sbf/reader.py:iter_epochs()`):
- Only discovers SIDs when they appear in actual MeasEpoch binary blocks
- Only SIDs with real observations get coordinate values
- Result: ~86 SIDs with populated coordinates per file (only currently visible satellites)

When `pad_to_global_sid()` reindexes to the universal 3,658-SID dimension, new SIDs get
NaN coordinates. On append to Icechunk, coordinates from later files do NOT update NaN
values set by earlier files -- `to_icechunk(append_dim="epoch")` only appends along the
epoch dimension and does not merge coordinate arrays.

### Impact

- Downstream analysis cannot filter by band/system/code for 235 SIDs in SBF stores
- VOD processing may skip or mishandle these SIDs
- The SID string itself (e.g., `C05|B1I|I`) encodes the info, but structured coordinate
  access is broken

### Recommendation

The SBF reader should populate SID coordinates from the GNSS signal specification tables
(same as RINEX does), not from observed data. The SID string already contains all needed
information to look up band, code, sv, system, and frequency values.

---

## 3. SNR Quantization Difference

### Observation

**SBF SNR values are 100% quantized to 0.25 dB resolution.**
**RINEX SNR values have ~0.001 dB resolution.**

```
Verification across multiple SIDs and constellations (same 15-min file, same receiver):

G02|L1|C: n=171, mean_diff=0.121 dB, max_abs=0.247 dB
  SBF quantized to 0.25 dB: 100%,  RINEX: 1%

G03|L1|C: n=180, mean_diff=0.116 dB, max_abs=0.249 dB
  SBF quantized to 0.25 dB: 100%,  RINEX: 1%

C06|B1I|I: n=109, mean_diff=0.130 dB, max_abs=0.248 dB
  SBF quantized to 0.25 dB: 100%,  RINEX: 0%
```

### Root Cause

This is a known property of the SBF binary format. The Septentrio receiver stores SNR in
its binary MeasEpoch blocks with 0.25 dB quantization (CN0 field is stored as `uint8 / 4`).
When the same receiver exports to RINEX, it uses a higher-precision internal representation.

### Impact

- Maximum SNR error between formats is bounded at 0.25 dB (half-quantization step)
- For VOD calculations, this introduces a small but systematic noise floor
- **This is expected behavior and not a bug** -- it is a hardware/format limitation

---

## 4. Epoch Offset

### Observation

SBF epochs are offset by ~2 seconds from RINEX epochs for the same receiver and time period:

```
RINEX: 2025-01-01T00:00:00.000 to 2025-01-07T23:59:55.000
SBF:   2024-12-31T23:59:42.000 to 2025-01-07T23:59:37.000
```

The SBF file starts ~18 seconds before midnight and epochs are spaced exactly 5 seconds
apart, but offset by 2 seconds from the RINEX grid.

### Impact

- Direct epoch matching between stores yields 0 common epochs
- Nearest-neighbor matching with 3-second tolerance matches all epochs
- phi/theta (satellite angles) differ by ~0.02 rad due to satellite motion in the 2-second gap
- **This is expected behavior** -- SBF and RINEX use different epoch timestamp conventions

---

## 5. Chunk Shape Inconsistency

### Observation

```
RINEX canopy_01:  SNR chunks = (2160, 81)
SBF canopy_01:    SNR chunks = (180, 321)
```

The RINEX store is ~1 GB while the SBF store is ~37 MB for the same data volume.

### Root Cause

Icechunk chunk shape is determined by the **first dataset written** to a group via
`to_icechunk()`. No explicit chunk encoding is specified in `write_initial_group()`.

- RINEX first write: 12 files concatenated = 2,160 epochs x 81 non-NaN SIDs -> chunks `(2160, 81)`
- SBF first write: 1 file = 180 epochs x 321 padded SIDs -> chunks `(180, 321)`

The `(180, 321)` chunks with ~80% NaN compress to nearly nothing under zstd.

### Recommendation

Explicitly specify chunk encoding in `write_initial_group()` based on the configured
`chunk_strategies` from `processing.yaml`.

---

## 6. Attribute Schema Mismatch

### Observation

RINEX store attributes include rich header metadata:
```
Author, Email, Institution, ..., RINEX Version, RINEX Type, Observer, Agency,
Marker Name, Marker Number, Marker Type, Approximate Position,
Receiver Type, Receiver Version, Receiver Number, Antenna Type, Antenna Number
```

SBF store attributes are minimal:
```
Author, Email, Institution, ..., File Hash,
APPROX POSITION X, APPROX POSITION Y, APPROX POSITION Z
```

### Root Cause

The SBF reader writes position as three separate float attributes (`APPROX POSITION X/Y/Z`)
while the RINEX reader writes a formatted string (`Approximate Position`). Other RINEX
header metadata (receiver type, antenna, marker, etc.) is not extracted by the SBF reader.

### Impact

- Schema inconsistency between stores from different formats
- Missing metadata for SBF stores (receiver/antenna info IS available in SBF ReceiverSetup blocks)
- Position attribute key mismatch may cause issues in downstream code that expects one format

---

## Verification Commands

```bash
# Check duplicate epochs in any store
uv run python -c "
import icechunk, xarray as xr, numpy as np
repo = icechunk.Repository.open(icechunk.local_filesystem_storage('<STORE_PATH>'))
session = repo.readonly_session('main')
ds = xr.open_zarr(session.store, group='canopy_01', consolidated=False)
epochs = ds.epoch.values
print(f'Total: {len(epochs)}, Unique: {len(np.unique(epochs))}')
diffs = np.diff(epochs.astype(np.int64))
non_mono = np.where(diffs <= 0)[0]
print(f'Non-monotonic jumps: {len(non_mono)}')
"

# Compare two stores (SNR, phi, theta, VOD)
uv run python -c "
import icechunk, xarray as xr, numpy as np
def load(path, group):
    repo = icechunk.Repository.open(icechunk.local_filesystem_storage(path))
    return xr.open_zarr(repo.readonly_session('main').store, group=group, consolidated=False)
a = load('<STORE_A>', '<GROUP>')
b = load('<STORE_B>', '<GROUP>')
for v in a.data_vars:
    av, bv = a[v].load().values, b[v].load().values
    both = np.isfinite(av) & np.isfinite(bv)
    d = np.abs(av[both] - bv[both])
    nm = (np.isnan(av) != np.isnan(bv)).sum()
    print(f'{v}: max_diff={d.max():.2e}, NaN_mismatch={nm}, identical={d.max()==0}')
"
```
