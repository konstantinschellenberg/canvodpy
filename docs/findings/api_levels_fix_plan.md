# API Levels Fix Plan

Date: 2026-03-08
Status: Planning (not yet implemented)

Tracks all issues found during the 4-level API test and the decisions made.

---

## Issues

### 1. VOD Computation Architecture

**Problem:** `TauOmegaZerothOrder.from_datasets()` on Dask-backed datasets (yielded by
`process_range()`) kills workers — OOM at 2GB per worker. One day is ~3GB across
both receivers (17280 epochs x 3658 sids x multiple float64 variables).

**Decision:** Create a `VodComputer` helper class with two explicit strategies:

- `compute_day(datasets, analysis_name, calculator=None)` — inline per-day.
  Calls `.load()` on the Dask-backed datasets to pull into main-process memory,
  then computes VOD single-threaded. For daily cron / Airflow.
- `compute_bulk(analysis_name, calculator=None, start=None, end=None)` — from
  RINEX store. Opens groups directly, reads full time range, dedup/sort,
  computes. For backfill or reprocessing.

Shared core: `_compute_and_write(canopy_ds, sky_ds, analysis_name, calculator_name)`
does rechunk `{epoch: 34560, sid: -1}`, clear encodings, write via
`store_vod_analysis()`.

Calculator is a parameter (default `"tau_omega_zeroth"`), resolved via
`VODFactory.create(calculator_name, ...)`. Future calculators register with
`VODFactory.register("dual_freq_vod", DualFreqCalculator)`.

Access: `site.vod` lazy property on `Site`.

```python
vod = site.vod

# Inline
with site.pipeline() as pipeline:
    for date_key, datasets in pipeline.process_range(...):
        vod.compute_day(datasets, "canopy_01_vs_reference_01")

# Bulk
vod.compute_bulk("canopy_01_vs_reference_01")
```

**Files to create/modify:**
- NEW: `canvodpy/src/canvodpy/vod_computer.py`
- MODIFY: `canvodpy/src/canvodpy/api.py` — add `vod` property to `Site`
- MODIFY: `canvodpy/src/canvodpy/cli/run.py` — use `vod.compute_day()`

---

### 2. Theta/Phi Missing in Levels 2 and 4

**Problem:** Levels 2 (FluentWorkflow) and 4 (Functional) produce only `[SNR]`
because they bypass the orchestrator's aux data pipeline (SP3/CLK download,
Hermite interpolation, coordinate transformation). Without theta/phi, VOD
cannot be computed.

**Decision:** Option B — implement `EphemerisProvider` ABC with three backends.
All levels use the same interface for theta/phi augmentation.

**EphemerisProvider ABC:**

```python
class EphemerisProvider(ABC):
    @abstractmethod
    def augment_dataset(self, ds, receiver_position) -> xr.Dataset:
        """Add theta, phi (and optionally r) to dataset."""

    @abstractmethod
    def preprocess_day(self, date, site_config) -> Path | None:
        """Download/prepare ephemeris for a day. Returns cache path or None."""
```

**Three implementations:**

1. `AgencyEphemerisProvider` — SP3/CLK from COD/ESA/IGS, Hermite interpolation.
   Refactored from current orchestrator logic (`_ensure_aux_data_preprocessed`
   + `compute_spherical_coordinates` in `preprocess_with_hermite_aux`).
2. `SbfBroadcastProvider` — reads theta/phi from SBF SatVisibility block.
   Refactored from current `use_sbf_geometry` fast path in processor.py.
3. `RinexNavProvider` — parses `.25p` RINEX NAV files (Keplerian elements
   for GPS/Galileo/BeiDou, state vectors for GLONASS), propagates orbits.
   Not yet implemented. Needs NAV parser (georinex or custom).

**Usage across levels:**

```python
# Level 4 functional
ds = read_rinex("file.rnx")
ds = augment_with_ephemeris(ds, source="final")

# Level 2 fluent
canvodpy.workflow("Rosalia").read("2025001").augment(source="broadcast").result()

# Level 1/3 — orchestrator refactored to use provider internally
```

**Phased implementation:**

- Phase 1: Define ABC + `AgencyEphemerisProvider` (extract from orchestrator)
- Phase 2: `SbfBroadcastProvider` (extract from processor.py broadcast path)
- Phase 3: `RinexNavProvider` (new — NAV parser + orbit propagation)
- Phase 4: Wire into all 4 API levels

**Files to create:**
- NEW: `packages/canvod-auxiliary/src/canvod/auxiliary/ephemeris/base.py` — ABC
- NEW: `packages/canvod-auxiliary/src/canvod/auxiliary/ephemeris/agency.py`
- NEW: `packages/canvod-auxiliary/src/canvod/auxiliary/ephemeris/sbf_broadcast.py`
- NEW: `packages/canvod-auxiliary/src/canvod/auxiliary/ephemeris/rinex_nav.py`
- MODIFY: `canvodpy/src/canvodpy/orchestrator/processor.py` — use provider
- MODIFY: `canvodpy/src/canvodpy/fluent.py` — `.augment()` step
- MODIFY: `canvodpy/src/canvodpy/functional.py` — `augment_with_ephemeris()`

---

### 3. vod_analyses Returns Dicts Instead of Pydantic Models

**Problem:** `GnssResearchSite.vod_analyses` calls `.model_dump()` on
`VodAnalysisConfig` objects, returning `dict[str, dict]`. All consumers must
use `cfg["canopy_receiver"]` instead of `cfg.canopy_receiver`. Loses type
safety and IDE support.

**Decision:** Option A — fix at source. `GnssResearchSite.vod_analyses` returns
`dict[str, VodAnalysisConfig]` directly (drop `.model_dump()`). All consumers
get attribute access (`cfg.canopy_receiver`) and type safety.

**Files to modify:**
- `packages/canvod-store/src/canvod/store/manager.py` line 124-128 — drop `.model_dump()`
- `packages/canvod-store/src/canvod/store/manager.py` line 132-138 — update type hint + `.get()` → attribute
- `canvodpy/src/canvodpy/cli/run.py` — switch from dict to attribute access
- `canvodpy/src/canvodpy/vod_computer.py` — new, uses models from the start

---

### 4. File Discovery Inconsistency Across Levels

**Problem:** Levels 1 and 3 use `FilenameMapper` (canvod.virtualiconvname) for
file discovery with naming convention awareness. Level 2 uses naive
`glob("*.25o")` — caught the daily file duplication bug (DOY 2025001 canopy
had 34560 epochs instead of 17280). Level 4 expects the caller to provide
file paths.

The `.25o` files on disk are **physical names** (e.g. `rref001a00.25o`), not
canonical. The orchestrator virtualizes them; the factory APIs don't.

**Decision:** L2 uses `FilenameMapper` internally (it has site context). L4 is
"bring your own file" by design — but callers use `FilenameMapper` to build
the file list before passing paths.

- L2 (`FluentWorkflow.read()`): replace naive `glob("*.25o")` with
  `FilenameMapper` discovery. Handles naming conventions, dedup, daily files.
- L4 (functional): no change to the functions themselves. Document that
  callers should use `FilenameMapper` for proper file discovery.
- Expose `FilenameMapper` as a first-class utility in the public API.

```python
# L4 pattern with proper file discovery
from canvod.virtualiconvname import FilenameMapper
mapper = FilenameMapper(site="Rosalia", receiver="canopy_01")
files = mapper.discover("2025001")
for f in files:
    ds = read_rinex(f)
```

**Files to modify:**
- `canvodpy/src/canvodpy/fluent.py` — `.read()` step: use FilenameMapper
- `canvodpy/src/canvodpy/__init__.py` — re-export FilenameMapper for convenience

---

### 5. Factory API Bugs (Fixed)

**Problem:** Multiple bugs in the factory-based APIs that were never tested
with real data.

**Fixed (2026-03-08):**
- `functional.py`: `path=` → `fpath=`, `.read()` → `.to_ds()`
- `fluent.py`: `GnssResearchSite.get_rinex_path()` doesn't exist — rewrote
  to use config data root + receiver directory + glob
- `fluent.py`: `.preprocess()` calls `preprocess_aux_for_interpolation()` on
  raw data (crashes — expects augmented datasets)
- `workflow.py`: same `path=`/`.read()` fix

**Status:** Code fixed, not yet committed.

---

### 6. Level Behavioral Consistency

**Problem:** The 4 levels produce fundamentally different outputs for the same
input data:
- L1/L3: `[SNR, theta, phi]` — augmented, deduplicated, written to store
- L2: `[SNR]` — raw, no dedup, daily file duplication possible
- L4: `[SNR]` — raw, per-file, caller responsible for everything

This is not transparent to users and will cause bugs downstream.

**Decision:** TBD — depends on resolution of issues #2, #4. Core question:
should all levels produce equivalent output, or should L2/L4 be explicitly
"raw reader" APIs?

**Related:** Issue #2 (theta/phi), Issue #4 (file discovery)

---

### 7. Architectural Direction: Decompose the Orchestrator

**Problem:** `PipelineOrchestrator` (processor.py, ~3000 lines) conflates
file discovery, Dask management, aux data, store writes, dedup, and metadata
into one monolithic class. It was the prototype top-level API that grew into
production. All processing concerns are tangled.

**Decision:** Gradual decomposition, not a rewrite. The orchestrator stays as
the internal engine for L1/L3 (it works), but new components are built as
**independent, composable pieces** that don't depend on it:

- `VodComputer` — standalone VOD computation + store write
- `EphemerisProvider` — standalone ephemeris augmentation (theta/phi)
- `FilenameMapper` — already standalone file discovery
- `MyIcechunkStore` — already standalone store operations

Over time, the orchestrator is refactored to call these components instead of
owning the logic. It becomes a thin coordinator: discover → read → augment →
write, with Dask parallelism. Each step is independently usable by L2/L4.

**Design principle:** The store write is an **explicit step**, not a side
effect of processing. The user decides when and whether to persist:

```python
# Explicit — user controls persistence
datasets = pipeline.process_date("2025001")  # read + augment only
site.rinex_store.write(datasets)             # persist (optional)
vod.compute_day(datasets, analysis_name)     # VOD (optional)
```

**No immediate action required.** This is the direction. The immediate fixes
(VodComputer, EphemerisProvider, FilenameMapper in L2, vod_analyses fix) are
already designed as standalone components that move us toward this architecture.

---

## Implementation Priority

| Priority | Issue | Complexity | Impact |
|----------|-------|------------|--------|
| 1 | #1 VodComputer | Done | Unblocks daily VOD pipeline |
| 2 | #3 vod_analyses Pydantic | Done | Type safety, quick fix |
| 3 | #5 Factory bugs | Done | Already fixed |
| 4 | #2 Theta/phi in L2/L4 | High | Requires EphemerisProvider design |
| 5 | #4 File discovery | Done | Prevents data duplication |
| 6 | #6 Level consistency | High | Architectural decision, depends on #2 and #4 |

---

## Test Results (2026-03-08)

| Level | Status | Time | Epochs/day | Variables |
|-------|--------|------|------------|-----------|
| Plain | Partial (disk full) | — | 17280 | SNR, theta, phi |
| Level 1 | PASS | 617s | 17280 | SNR, theta, phi |
| Level 2 | PASS (after fixes) | 111s | 17280* | SNR |
| Level 3 | PASS | 1399s | 17280 | SNR, theta, phi |
| Level 4 | PASS | 148s | 17280 | SNR |

*DOY 2025001 canopy had 34560 epochs due to daily file duplication.

SNR is bit-identical across all levels. Icechunk store: 481MB for 7 days.
