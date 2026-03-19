# canvodpy API Levels: Mechanisms and Architecture

Date: 2026-03-08

This document describes the four API levels exposed by canvodpy, their internal
mechanisms, data flows, known bugs, and consistency gaps.

---

## 1. Overview Table

| Aspect | Level 1: Convenience Functions | Level 2: Fluent Workflow | Level 3: Site + Pipeline | Level 4: Functional |
|---|---|---|---|---|
| **Name** | One-liner functions | Chainable deferred workflow | Object-oriented pipeline | Pure functions |
| **Pattern** | `process_date("Rosalia", "2025001")` | `canvodpy.workflow("Rosalia").read(...).result()` | `Site("Rosalia").pipeline().process_date(...)` | `read_rinex("file.rnx")` |
| **Import** | `from canvodpy import process_date, calculate_vod` | `from canvodpy import workflow` or `FluentWorkflow` | `from canvodpy import Site, Pipeline` | `from canvodpy.functional import read_rinex, calculate_vod` |
| **Primary module** | `canvodpy.api` (functions at bottom) | `canvodpy.fluent` (`FluentWorkflow`) | `canvodpy.api` (`Site`, `Pipeline`) | `canvodpy.functional` |
| **Execution model** | Synchronous, delegates to Pipeline | Deferred: steps recorded, executed on terminal call | Synchronous, delegates to PipelineOrchestrator | Synchronous, single-process |
| **Internal engine** | Pipeline -> PipelineOrchestrator -> Dask | ReaderFactory -> reader.to_ds() directly | PipelineOrchestrator -> RinexDataProcessor -> Dask | ReaderFactory -> reader.to_ds() directly |
| **Aux data (SP3/CLK)** | Yes (via orchestrator) | No | Yes (via orchestrator) | No |
| **Theta/phi coords** | Yes (Hermite interpolation) | No | Yes (Hermite interpolation) | No |
| **Store writes** | Yes (Icechunk with metadata table) | Optional (`to_store()` terminal) | Yes (Icechunk with metadata table) | No (NetCDF via `*_to_file`) |
| **Deduplication** | 3-layer (hash, temporal, intra-batch) | None | 3-layer (hash, temporal, intra-batch) | None |
| **File discovery** | FilenameMapper (virtualiconvname) via orchestrator | Naive glob (`*.25o`) | FilenameMapper (virtualiconvname) via orchestrator | Caller provides path |
| **Dask parallelism** | Yes (spawn workers) | No (single-process) | Yes (spawn workers) | No (single-process) |
| **VOD computation** | Yes (reads back from store) | Yes (inline via VODFactory) | Yes (reads back from store) | Yes (inline via VODFactory) |
| **Airflow support** | No | No | No | Yes (`*_to_file` variants return path strings) |
| **Context manager** | Yes (`with Pipeline(...)`) | No | Yes (`with Pipeline(...)`) | No (stateless) |

---

## 2. Data Flow Per Level

### Level 1: Convenience Functions

**Entry point:** `process_date(site, date)` or `calculate_vod(site, canopy, reference, date)` in `canvodpy/api.py`.

```
process_date("Rosalia", "2025001")
  |
  +-> Pipeline("Rosalia")          # creates Site, loads config
  |     |
  |     +-> PipelineOrchestrator(site._site, ...)
  |           |
  |           +-> PairDataDirMatcher     # file discovery via virtualiconvname
  |           +-> DaskClusterManager     # lazy Dask cluster (spawn workers)
  |
  +-> pipeline.process_date("2025001")
        |
        +-> orchestrator.process_by_date(start_from=date, end_at=date)
              |
              +-> For each day:
              |     1. _discover_files()         # FilenameMapper patterns
              |     2. _check_existing_with_temporal_overlap()  # 3-layer dedup
              |     3. AuxDataPipeline           # download SP3/CLK from FTP
              |     4. Hermite spline interp     # preprocess to Zarr on disk
              |     5. Submit to Dask workers:
              |        preprocess_with_hermite_aux()
              |          -> ReaderFactory.create(reader_name, fpath=...)
              |          -> reader.to_ds_and_auxiliary()
              |          -> Open aux Zarr, select matching epochs
              |          -> Filter SIDs (keep_sids from config)
              |          -> compute_spherical_coordinates() -> theta, phi, r
              |     6. Collect results, write to Icechunk store
              |        -> append_to_group() with hash+temporal guardrail
              |        -> Update metadata table
              |     7. Yield {receiver_name: xr.Dataset}
              |
              +-> pipeline.close()  # shutdown Dask cluster
```

**Key:** The returned datasets contain theta/phi computed from SP3 ephemerides
via Hermite interpolation, and have been written to (and optionally read back
from) the Icechunk store.

### Level 2: Fluent Workflow (FluentWorkflow)

**Entry point:** `canvodpy.workflow("Rosalia").read("2025001").result()` in `canvodpy/fluent.py`.

```
canvodpy.workflow("Rosalia")
  |
  +-> FluentWorkflow(site="Rosalia")
        |
        .read("2025001")           # DEFERRED (appended to _plan)
        .preprocess(agency="COD")  # DEFERRED
        .grid("equal_area")        # DEFERRED
        .vod("canopy_01", "ref")   # DEFERRED
        .result()                  # TERMINAL -> executes all steps
              |
              +-> read() executes:
              |     1. Build path: data_root / recv_cfg.directory / doy_dir
              |     2. Glob *.25o files (hardcoded extension)
              |     3. For each file:
              |        ReaderFactory.create("rinex3", fpath=fpath)
              |        reader.to_ds()
              |        Drop vars not in keep_vars
              |     4. xr.concat(datasets, dim="epoch")
              |     5. Store in self._datasets[receiver_name]
              |
              +-> preprocess() executes:
              |     canvod.auxiliary.preprocess_aux_for_interpolation(ds)
              |     (operates on raw RINEX data - NO aux downloaded)
              |
              +-> grid() executes:
              |     GridFactory.create(...).build()
              |     canvod.grids.add_cell_ids_to_ds_fast(ds, grid)
              |
              +-> vod() executes:
              |     VODFactory.create("tau_omega", canopy_ds=..., sky_ds=...)
              |     calculator.calculate_vod()
              |
              +-> result() returns VOD dataset or dict of datasets
```

**Key:** No auxiliary data download, no Hermite interpolation, no theta/phi
computation, no store writes, no deduplication. The `preprocess()` step attempts
to run `preprocess_aux_for_interpolation()` on raw RINEX data, which will fail
because that function expects auxiliary orbit data, not observation data.

### Level 3: Site + Pipeline

**Entry point:** `Site("Rosalia").pipeline().process_date("2025001")` in `canvodpy/api.py`.

```
Site("Rosalia")
  |
  +-> GnssResearchSite("Rosalia")   # loads config, creates rinex_store + vod_store
  |
  .pipeline(n_workers=8, ...)
  |
  +-> Pipeline(site=self, ...)
        |
        +-> PipelineOrchestrator(site._site, ...)   # same as Level 1
        |
        .process_date("2025001")
        |
        +-> orchestrator.process_by_date(...)       # identical to Level 1
```

Level 3 is functionally identical to Level 1. The difference is ergonomic:
Level 1 creates a `Pipeline` internally per call (and destroys it), while
Level 3 lets the user hold a `Pipeline` object across multiple calls, reusing
the Dask cluster.

`Pipeline` implements `__enter__`/`__exit__` for context manager usage and
`close()` for explicit Dask cluster shutdown.

### Level 4: Functional API

**Entry point:** `read_rinex(path)`, `calculate_vod(canopy_ds, sky_ds)`, etc. in `canvodpy/functional.py`.

```
read_rinex("station.25o")
  |
  +-> ReaderFactory.create("rinex3", fpath=path)
  +-> reader.to_ds()
  +-> return xr.Dataset                # raw observations only

create_grid("equal_area", angular_resolution=5.0)
  |
  +-> GridFactory.create("equal_area", ...).build()
  +-> return GridData

assign_grid_cells(ds, grid)
  |
  +-> canvod.grids.add_cell_ids_to_ds_fast(ds, grid)
  +-> return xr.Dataset with cell coord

calculate_vod(canopy_ds, sky_ds)
  |
  +-> VODFactory.create("tau_omega", canopy_ds=..., sky_ds=...)
  +-> calculator.calculate_vod()
  +-> return xr.Dataset
```

Each function is stateless and pure. The `*_to_file` variants wrap each function
with `xr.Dataset.to_netcdf()` / `xr.open_dataset()` and return `str` paths for
Airflow XCom serialization.

**Key:** No site config, no file discovery, no aux data, no store, no dedup. The
caller is responsible for providing correct file paths and datasets. Grid
assignment requires theta/phi in the dataset, which are not present in raw RINEX
output (only the orchestrator adds them).

---

## 3. Key Differences

### File Discovery

| Level | Mechanism | Source of truth |
|---|---|---|
| 1, 3 | `FilenameMapper` from `canvod.virtualiconvname` via `PairDataDirMatcher` | `BUILTIN_PATTERNS` in `canvod.virtualiconvname.patterns` |
| 2 | `recv_dir.glob("*.25o")` hardcoded in `FluentWorkflow.read()` | None (naive glob) |
| 4 | Caller provides path | N/A |

Level 2's naive glob means:
- It only finds `.25o` files (misses `.rnx`, `.crx`, SBF `.sbf` files).
- It finds all files matching the glob, including files that should be excluded
  by the naming convention (e.g., duplicates, wrong receiver).
- No canonical name mapping, so daily file overlap detection is impossible.

### Deduplication

| Level | Hash check | Temporal overlap check | Intra-batch overlap check |
|---|---|---|---|
| 1, 3 | Yes (via `batch_check_existing`) | Yes (via `_check_existing_with_temporal_overlap`) | Yes (sort + gap detection in orchestrator) |
| 2 | None | None | None |
| 4 | None | None | None |

### Auxiliary Data and Coordinate Augmentation

| Level | SP3/CLK download | Hermite interpolation | theta/phi in output | clock corrections |
|---|---|---|---|---|
| 1, 3 | Yes (`AuxDataPipeline`) | Yes (to Zarr on disk) | Yes | Yes |
| 2 | No | No | No (raw RINEX only) | No |
| 4 | No | No | No (raw RINEX only) | No |

The orchestrator (Levels 1, 3) runs `compute_spherical_coordinates()` using
ECEF satellite positions from SP3 files and the receiver's ECEF position from
config, producing theta (elevation) and phi (azimuth) for each epoch x SID.
These are required for grid assignment and VOD computation.

Levels 2 and 4 return raw RINEX datasets that contain only the observation
variables (SNR, carrier phase, pseudorange, etc.) without any geometric
coordinates.

### Store Operations

| Level | Write target | Metadata table | Commit messages | Versioning |
|---|---|---|---|---|
| 1, 3 | Icechunk (Zarr-backed, version-controlled) | Yes (group/metadata/table) | Yes (with version, hash, timestamp) | Icechunk snapshots |
| 2 | Optional via `to_store()` terminal: direct `write_or_append_group` | Via store's internal logic | Basic | Icechunk snapshots |
| 4 | NetCDF files via `to_netcdf()` (in `*_to_file` variants) | None | N/A | None |

### Dask Parallelism

| Level | Execution model | Worker type | Serialization |
|---|---|---|---|
| 1, 3 | `dask.distributed.Client` with `LocalCluster` (spawn) | Separate processes | Module-level functions required |
| 2 | Single process, sequential loop over receivers | Main thread | N/A |
| 4 | Single process | Main thread | N/A |

The orchestrator uses `DaskClusterManager` to create a `LocalCluster` with
spawn-based workers. The `preprocess_with_hermite_aux()` function must be at
module level (not a method) because Dask serializes it for worker processes.

---

## 4. Bugs Found in API Level Testing (2026-03-08)

### 4.1 functional.py

**Bug:** The `ReaderFactory.create()` example in the module docstring uses
`path=` as the keyword argument. The actual `Rnxv3Obs` constructor and the
factory both expect `fpath=`. The implementation code in the functions
themselves correctly uses `fpath=`, but the docstring example at line 15 is
misleading:
```python
# Docstring says:
>>> ReaderFactory.create("rinex3", path="data.rnx")
# Should be:
>>> ReaderFactory.create("rinex3", fpath="data.rnx")
```

Similarly the factories.py module docstring (line 15) uses `path=`:
```python
>>> ReaderFactory.register("rinex3", Rnxv3Obs)
>>> reader = ReaderFactory.create("rinex3", path="data.rnx")
```

The actual function implementations in `functional.py` are correct (they pass
`fpath=path`).

### 4.2 fluent.py

**Bug 1: Hardcoded file extension.** `FluentWorkflow.read()` uses
`recv_dir.glob("*.25o")` (line 168), which only matches RINEX v3 files with
year-suffix `.25o`. This misses `.rnx`, `.crx`, compressed files, SBF files,
and files from other years.

**Bug 2: preprocess() calls aux preprocessing on raw data.** The `preprocess()`
step (line 193-210) imports and calls
`canvod.auxiliary.preprocess_aux_for_interpolation(ds)` on the raw RINEX
observation dataset. This function expects auxiliary orbit/clock data as input,
not GNSS observations. It will fail at runtime.

**Bug 3: Daily file duplication.** Without naming convention awareness
(`FilenameMapper`), the naive glob can pick up multiple files covering the same
time range (e.g., daily vs sub-daily files, reprocessed files). These are
concatenated without dedup, producing duplicated epochs.

### 4.3 workflow.py (VODWorkflow)

**Bug 1: `get_rinex_path()` does not exist.** `VODWorkflow._get_rinex_path()`
(line 462) calls `self.site._site.get_rinex_path(receiver, date)`, but
`GnssResearchSite` has no `get_rinex_path()` method. This will raise
`AttributeError` at runtime.

**Bug 2: `_augment_data()` is a no-op.** The method (line 367-400) contains
only a TODO comment and passes through. The docstring promises augmentation
(filtering, interpolation), but nothing is implemented. The subsequent
`_assign_grid_cells()` call will fail because theta/phi coordinates are absent
from the raw dataset.

### 4.4 Site.vod_analyses returns dicts, not Pydantic models

`Site.vod_analyses` (line 102-104) returns
`self._site.active_vod_analyses`, which comes from
`GnssResearchSite.active_vod_analyses`. That property calls `.model_dump()` on
each `VodAnalysisConfig`, returning plain dicts. Users expecting Pydantic
models with validation and attribute access will get untyped dictionaries
instead.

---

## 5. Consistency Gaps

### 5.1 File Discovery

Levels 2 and 4 do not use `canvod.virtualiconvname` for file discovery. Level 2
uses a hardcoded glob, while Level 4 relies entirely on the caller. For
consistent results:

- All levels should resolve files through `FilenameMapper` and `BUILTIN_PATTERNS`.
- `FluentWorkflow.read()` should accept a date and use the same discovery logic
  as the orchestrator, or at minimum use `BUILTIN_PATTERNS` for globbing.

### 5.2 Theta/Phi Augmentation

Only the orchestrator (Levels 1, 3) computes satellite elevation and azimuth
angles. Levels 2 and 4 return raw datasets without these coordinates, making
grid assignment and VOD computation fail or produce meaningless results.

To close this gap:
- Extract the coordinate augmentation logic from
  `preprocess_with_hermite_aux()` into a standalone function usable without
  the full orchestrator.
- Provide a `compute_geometry(ds, site_name)` function in the functional API
  that downloads aux data and computes theta/phi for a single dataset.

### 5.3 VOD Computation Path

Levels 1 and 3 compute VOD by:
1. Processing RINEX through the orchestrator (which adds theta/phi)
2. Writing to Icechunk
3. Reading back from Icechunk
4. Running `VODCalculator.from_datasets(canopy_ds, reference_ds)`

Levels 2 and 4 attempt to compute VOD inline on raw datasets that lack
theta/phi. This will produce incorrect results or errors because the
tau-omega model requires elevation angles.

### 5.4 Return Types

`Site.vod_analyses` and `GnssResearchSite.vod_analyses` return `dict[str, dict]`
(via `.model_dump()`), not Pydantic models. `Site.receivers` similarly returns
dicts. This prevents:
- Attribute access (e.g., `analysis.canopy_receiver` vs `analysis["canopy_receiver"]`)
- Pydantic validation on access
- IDE autocompletion

### 5.5 Reader Format Awareness

- Levels 1, 3: The orchestrator resolves reader format per-receiver from
  `sites.yaml` (`reader_format` field, default `"auto"`), supporting RINEX v2,
  v3, and SBF transparently.
- Level 2: Hardcoded `reader="rinex3"` default with hardcoded `*.25o` glob.
- Level 4: Caller must specify reader name; default is `"rinex3"`.

---

## 6. Recommendations

### 6.1 Fix Broken APIs (Priority: High)

1. **workflow.py:** Remove `_get_rinex_path()` and rewrite `_load_rinex()` to
   use the same path resolution as `FluentWorkflow.read()` (config-based
   directory + file discovery), or remove `VODWorkflow` entirely if Level 2
   (FluentWorkflow) is the preferred chainable API.

2. **fluent.py:** Replace `recv_dir.glob("*.25o")` with pattern-based discovery
   using `BUILTIN_PATTERNS` from `canvod.virtualiconvname.patterns`. Remove the
   `preprocess()` step or rewrite it to perform actual observation-level
   preprocessing (outlier filtering, cycle slip detection), not aux data
   preprocessing.

3. **factories.py / functional.py docstrings:** Change `path=` to `fpath=` in
   all examples.

### 6.2 Extract Geometry Computation (Priority: High)

Create a standalone function that computes theta/phi for a single
`xr.Dataset` without requiring the full orchestrator:

```python
def augment_geometry(
    ds: xr.Dataset,
    site_name: str,
    agency: str = "COD",
) -> xr.Dataset:
    """Download aux data and compute theta/phi for one dataset."""
```

This would allow Levels 2 and 4 to produce geometry-augmented datasets.

### 6.3 Unify File Discovery (Priority: Medium)

Add a `discover_files(site_name, receiver, date)` function to the functional
API that uses `FilenameMapper` internally. This gives all levels access to the
same file discovery logic without depending on the orchestrator.

### 6.4 Return Pydantic Models (Priority: Low)

Change `GnssResearchSite.receivers`, `.vod_analyses`, etc. to return the
original Pydantic config objects instead of calling `.model_dump()`. Users who
need dicts can call `.model_dump()` themselves.

### 6.5 Consolidate Workflow Classes (Priority: Medium)

There are currently three overlapping workflow patterns:
- `Pipeline` (Level 3) -- wraps `PipelineOrchestrator`, production-ready
- `FluentWorkflow` (Level 2) -- chainable/deferred, but broken
- `VODWorkflow` (workflow.py) -- similar to FluentWorkflow, also broken

Consider removing `VODWorkflow` and fixing `FluentWorkflow` as the single
lightweight alternative to the full `Pipeline`. Or, if `FluentWorkflow` is not
needed, remove both and keep only `Pipeline` (Levels 1/3) and the functional
API (Level 4).

---

## 7. Summary Matrix: What Each Level Provides

| Capability | L1 | L2 | L3 | L4 |
|---|---|---|---|---|
| Read RINEX | x | x | x | x |
| Read SBF | x | - | x | - |
| File discovery (virtualiconvname) | x | - | x | - |
| Aux data download | x | - | x | - |
| Hermite interpolation | x | - | x | - |
| Theta/phi computation | x | - | x | - |
| 3-layer dedup | x | - | x | - |
| Icechunk store write | x | partial | x | - |
| Metadata table | x | - | x | - |
| Dask parallelism | x | - | x | - |
| Grid assignment | x | x* | x | x* |
| VOD computation | x | x* | x | x* |
| Airflow XCom support | - | - | - | x |
| Deferred execution | - | x | - | - |
| Stateless / pure functions | - | - | - | x |

\* Requires theta/phi in input dataset, which Level 2/4 do not produce.
Without the orchestrator's geometry augmentation, grid assignment and VOD
will fail.
