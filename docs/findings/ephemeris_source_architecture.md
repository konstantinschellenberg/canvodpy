# Ephemeris Source Architecture: Design Document

**Date:** 2026-03-08
**Status:** Planning
**Author:** Nicolas Bader + Claude

---

## 1. Problem Statement

canvodpy currently supports only one ephemeris source: agency final products (SP3/CLK).
This requires internet access and 12-18 days of latency after data collection. For many
VOD applications, near-real-time (NRT) processing from broadcast ephemerides is sufficient,
since a 1-2 m orbit error at 20,000+ km altitude produces sub-0.00001 deg angular error
in theta/phi -- six orders of magnitude below the measurement noise floor.

Three distinct ephemeris sources exist, each tied to a receiver's output format:

| Source | Input files | Availability | Accuracy | Internet |
|--------|------------|-------------|----------|----------|
| **Agency final products** | SP3 + CLK | 12-18 day delay | ~2-3 cm orbit | Required |
| **SBF/UBX binary** | Proprietary binary | Immediate | ~1-2 m orbit | None |
| **RINEX NAV** | `.YYp` / `.YYn` / `.YYg` | Immediate | ~1-2 m orbit | None |

A fourth file type, **NMEA** (`.YY1`), contains GSV sentences with integer-degree
satellite elevation/azimuth. This precision (1 deg) is far too coarse for VOD and
NMEA lacks carrier-phase observables entirely. NMEA files are **not** an ephemeris
source for canvodpy.

---

## 2. Current Architecture

### 2.1 Pipeline overview

```
                    SP3/CLK files (downloaded)
                           |
                    AuxDataPipeline.load_all()
                           |
               Hermite interpolation to target epochs
                           |
                    Zarr cache (aux_{date}.zarr)
                           |
          preprocess_with_hermite_aux() per file
                           |
               open Zarr -> sel(epoch) -> compute SCS
                           |
               ds["theta"], ds["phi"], ds["r"]
```

### 2.2 Key components and locations

| Component | Package | File | Entry point |
|-----------|---------|------|-------------|
| SP3/CLK download & parse | canvod-auxiliary | `pipeline.py` | `AuxDataPipeline.create_standard()` |
| FTP with fallback | canvod-auxiliary | `core/downloader.py` | `FtpDownloader.download()` |
| SP3 Hermite interpolation | canvodpy | `orchestrator/interpolator.py` | `Sp3InterpolationStrategy.interpolate()` |
| Clock piecewise linear | canvodpy | `orchestrator/interpolator.py` | `ClockInterpolationStrategy.interpolate()` |
| ECEF -> theta/phi/r | canvod-auxiliary | `position/spherical_coords.py` | `compute_spherical_coordinates()` |
| SBF SatVisibility extract | canvod-readers | `sbf/reader.py` | `to_ds_and_auxiliary()` -> `sbf_obs` |
| Fast path (SBF geometry) | canvodpy | `orchestrator/processor.py` | `preprocess_with_hermite_aux()` L164 |
| File discovery | canvod-virtualiconvname | `patterns.py` | `BUILTIN_PATTERNS` |

### 2.3 Config model

```python
# canvod-utils/src/canvod/utils/config/models.py
class ProcessingParams(BaseModel):
    ephemeris_source: Literal["final", "broadcast"] = "final"
```

### 2.4 What works today

- **`final`**: Full pipeline. Downloads SP3+CLK, Hermite interpolation, ECEF->SCS.
  Works for any reader format (RINEX, SBF). Tested, production-quality.
- **`broadcast` (SBF only)**: Partial. Extracts theta/phi from SatVisibility blocks
  in SBF binary files. Skips SP3/CLK download entirely. Added 2026-03-08, partially
  working (reference receiver shared geometry still in progress).

### 2.5 What does NOT work today

- **RINEX NAV broadcast**: No parser, no Keplerian propagator. NAV files (`.25p`)
  are ignored entirely.
- **UBX binary**: No reader exists. Similar to SBF but Septentrio-only vs u-blox.
- **Broadcast + shared position for reference receivers**: The mechanism to pass
  canopy broadcast geometry to reference receivers is partially implemented but
  has performance issues (reading all canopy files sequentially).

---

## 3. Target Architecture

### 3.1 Design principle: EphemerisProvider abstraction

The core insight is that all three sources produce the same output:

```
EphemerisProvider.get_satellite_coordinates(target_epochs, sids)
    -> xr.Dataset with dims (epoch, sid) and vars (X, Y, Z) in ECEF meters
```

or, for SBF which already has theta/phi:

```
EphemerisProvider.get_geometry(target_epochs, sids)
    -> xr.Dataset with dims (epoch, sid) and vars (theta, phi) in radians/degrees
```

Both produce the same downstream result: `ds["theta"]`, `ds["phi"]` on the observation
dataset.

### 3.2 Proposed class hierarchy

```
EphemerisProvider (ABC)
    |
    +-- AgencyEphemerisProvider          # SP3/CLK final products
    |       Input: agency, product_type, date range
    |       Needs: internet, FTP credentials
    |       Output: interpolated XYZ at target epochs
    |       Then: ECEF -> SCS (theta/phi/r) using receiver position
    |
    +-- BroadcastEphemerisProvider (ABC)
    |       |
    |       +-- SbfBroadcastProvider     # SBF SatVisibility blocks
    |       |       Input: SBF file path
    |       |       Output: theta/phi directly (no XYZ intermediate)
    |       |       Note: theta/phi tied to receiver's own position
    |       |
    |       +-- UbxBroadcastProvider     # u-blox binary (future)
    |       |       Input: UBX file path
    |       |       Output: theta/phi or XYZ depending on UBX block type
    |       |
    |       +-- RinexNavProvider         # RINEX NAV broadcast ephemerides
    |               Input: .YYp / .YYn / .YYg file path
    |               Output: XYZ at target epochs (via Keplerian propagation)
    |               Then: ECEF -> SCS using receiver position
```

### 3.3 Provider selection logic

```python
# Automatic selection based on config + available files
def resolve_ephemeris_provider(
    ephemeris_source: str,          # "final" | "broadcast" | "auto"
    reader_format: str,             # "rinex3" | "sbf" | "ubx"
    data_dir: Path,                 # receiver data directory
    config: CanvodConfig,
) -> EphemerisProvider:

    if ephemeris_source == "final":
        return AgencyEphemerisProvider(config)

    if ephemeris_source == "broadcast":
        if reader_format == "sbf":
            return SbfBroadcastProvider()
        if reader_format == "ubx":
            return UbxBroadcastProvider()
        # RINEX: check for NAV files alongside OBS files
        nav_files = find_nav_files(data_dir)
        if nav_files:
            return RinexNavProvider(nav_files)
        raise ConfigError(
            "ephemeris_source=broadcast but no broadcast ephemerides found. "
            "RINEX receivers need .YYp/.YYn NAV files alongside OBS files."
        )

    # "auto": prefer broadcast if available, fall back to final
    if reader_format == "sbf":
        return SbfBroadcastProvider()
    nav_files = find_nav_files(data_dir)
    if nav_files:
        return RinexNavProvider(nav_files)
    return AgencyEphemerisProvider(config)
```

### 3.4 Config model changes

```python
class ProcessingParams(BaseModel):
    ephemeris_source: Literal["final", "broadcast", "auto"] = "final"
    # "final"    : SP3/CLK from agency (requires internet, 12-18d latency)
    # "broadcast": use broadcast ephemerides from receiver files (NRT)
    # "auto"     : broadcast if available, else final (future)
```

No per-receiver override needed: receiver_format already determines which broadcast
provider is used. A RINEX receiver with NAV files and `ephemeris_source: broadcast`
automatically uses `RinexNavProvider`.

---

## 4. Implementation Roadmap

### Phase 1: Clean up current broadcast path (SBF SatVisibility)

**Goal:** Make the existing SBF broadcast path production-ready.

**What exists:** `use_sbf_geometry` flag, SBF SatVisibility extraction, partial
shared-position support.

**What needs fixing:**
1. Reference receiver shared geometry: each reference Dask task needs the matching
   canopy file's theta/phi. Current approach (pre-reading all canopy files) is too
   slow. Solution: pass matching canopy file path per reference task, read one file
   per worker.
2. The `use_sbf_geometry` flag should be replaced by the cleaner
   `ephemeris_source` config option (already partially done).
3. Unit angle handling: SBF SatVisibility reports theta in degrees (zenith distance),
   the final SP3 path produces theta in radians. Both must use the same convention
   in the store.

**Files to modify:**
- `processor.py`: Clean up broadcast fast path, fix shared position
- `processor.py`: Remove `use_sbf_geometry` parameter, use `ephemeris_source` throughout

### Phase 2: EphemerisProvider abstraction

**Goal:** Extract the ephemeris logic from the orchestrator into a clean provider interface.

**What to extract from processor.py:**
- `_initialize_aux_pipeline()` -> `AgencyEphemerisProvider.__init__()`
- `_preprocess_aux_data_with_hermite()` -> `AgencyEphemerisProvider.preprocess()`
- `_ensure_aux_data_preprocessed()` -> `AgencyEphemerisProvider.ensure_preprocessed()`
- Broadcast fast path (L164-200) -> `SbfBroadcastProvider.augment_dataset()`

**New package or module?**
Option A: New module in canvod-auxiliary (`canvod.auxiliary.providers`)
  - Pro: keeps all ephemeris logic in one package
  - Con: canvod-auxiliary currently has no dependency on canvod-readers

Option B: New module in canvodpy orchestrator (`canvodpy.orchestrator.ephemeris`)
  - Pro: close to where it's used, can import from both auxiliary and readers
  - Con: not reusable outside canvodpy

**Recommendation:** Option A with a thin adapter. The provider ABC and
AgencyEphemerisProvider live in canvod-auxiliary. SbfBroadcastProvider lives in
canvod-readers (it depends on the SBF reader). The orchestrator imports both
and uses `resolve_ephemeris_provider()` to pick the right one.

**Interface:**

```python
class EphemerisProvider(ABC):
    """Provides satellite geometry (theta/phi) for observation datasets."""

    @abstractmethod
    def augment_dataset(
        self,
        ds: xr.Dataset,
        receiver_position: ECEFPosition,
        aux_datasets: dict[str, xr.Dataset] | None = None,
    ) -> xr.Dataset:
        """Add theta, phi (and optionally r) to the observation dataset.

        Parameters
        ----------
        ds : xr.Dataset
            Observation dataset with dims (epoch, sid).
        receiver_position : ECEFPosition
            Receiver ECEF position for coordinate transform.
        aux_datasets : dict, optional
            Auxiliary datasets from the reader (e.g. sbf_obs).

        Returns
        -------
        xr.Dataset
            Input dataset with theta, phi, r added as data variables.
        """
        ...

    @abstractmethod
    def preprocess_day(
        self,
        date_str: str,
        target_epochs: np.ndarray,
        output_path: Path,
    ) -> Path | None:
        """Preprocess ephemeris data for one day (optional).

        For SP3/CLK: interpolate to target epoch grid and write Zarr cache.
        For broadcast: no-op (geometry is extracted per-file).

        Returns
        -------
        Path | None
            Path to Zarr cache, or None if no preprocessing needed.
        """
        ...
```

### Phase 3: RINEX NAV broadcast provider

**Goal:** Compute satellite XYZ from RINEX navigation files using Keplerian propagation.

**Dependencies:**
- NAV parser: `georinex` (outputs xarray, mature, multi-GNSS)
- Keplerian propagator: custom implementation or `gnss_lib_py` (GPS-only) + custom for others

**Keplerian propagation algorithm (GPS/Galileo/BeiDou):**

All three use the same 16-parameter orbital model (IS-GPS-200, Galileo ICD, BeiDou ICD):

```python
def propagate_keplerian(nav_params: dict, t_target: float) -> tuple[float, float, float]:
    """Compute satellite ECEF XYZ from Keplerian elements at time t.

    Parameters (from NAV file):
        sqrtA    : sqrt of semi-major axis (m^0.5)
        e        : eccentricity
        i0       : inclination at reference time (rad)
        OMEGA0   : right ascension of ascending node at reference time (rad)
        omega    : argument of perigee (rad)
        M0       : mean anomaly at reference time (rad)
        dn       : mean motion difference (rad/s)
        IDOT     : rate of inclination angle (rad/s)
        OMEGA_DOT: rate of right ascension (rad/s)
        Cuc, Cus : argument of latitude corrections (rad)
        Crc, Crs : orbit radius corrections (m)
        Cic, Cis : inclination corrections (rad)
        toe      : time of ephemeris (s of GPS week)

    Steps:
        1. Compute mean motion: n = sqrt(mu / a^3) + dn
        2. Mean anomaly at t: M = M0 + n * (t - toe)
        3. Solve Kepler's equation: E - e*sin(E) = M  (iterate ~10x)
        4. True anomaly: v = atan2(sqrt(1-e^2)*sin(E), cos(E)-e)
        5. Argument of latitude: u = v + omega + Cuc*cos(2u) + Cus*sin(2u)
        6. Radius: r = a*(1 - e*cos(E)) + Crc*cos(2u) + Crs*sin(2u)
        7. Inclination: i = i0 + IDOT*(t-toe) + Cic*cos(2u) + Cis*sin(2u)
        8. Longitude of ascending node: OMEGA = OMEGA0 + (OMEGA_DOT - omega_e)*(t-toe) - omega_e*toe
        9. Satellite position in orbital plane -> ECEF rotation
    """
```

**GLONASS exception:** Uses PZ-90 state vectors (x, y, z, vx, vy, vz) with
4th-order Runge-Kutta numerical integration. Different algorithm, ~80 lines.

**Implementation plan:**
1. Add `georinex` as optional dependency to canvod-readers or canvod-auxiliary
2. Create `RinexNavReader` that parses `.YYp` files into a structured dataset
3. Create `KeplerianPropagator` with GPS/Galileo/BeiDou and GLONASS methods
4. Create `RinexNavProvider(EphemerisProvider)` that combines parser + propagator
5. Output: same `(epoch, sid) -> (X, Y, Z)` format as SP3 interpolation
6. Feed into existing `compute_spherical_coordinates()` for theta/phi/r

### Phase 4: UBX reader (future)

**Goal:** Read u-blox binary files for UBX-equipped receivers.

Not scoped in detail. Similar architecture to SBF reader. u-blox provides
`pyubx2` library for parsing. Relevant blocks: UBX-NAV-SAT (satellite info
with elevation/azimuth), UBX-RXM-RAWX (raw measurements).

---

## 5. File Discovery Changes

Current file discovery (`BUILTIN_PATTERNS`) already supports SBF globs (`*.[0-9][0-9]_`).
For the NAV provider, we need to discover NAV files alongside OBS files:

```python
# New patterns needed in BUILTIN_PATTERNS:
"rinex3_nav": SourcePattern(
    file_globs=["*.[0-9][0-9]p", "*.[0-9][0-9]n", "*.[0-9][0-9]g",
                "*_MN.rnx", "*_GN.rnx", "*_RN.rnx", "*_EN.rnx", "*_CN.rnx"],
    ...
)
```

NAV files live in the same directory as OBS files (confirmed for Rosalia site).
The provider needs a separate discovery step: "given this OBS data directory,
find associated NAV files."

---

## 6. Data Flow Comparison

### Agency final (SP3/CLK)

```
config.ephemeris_source = "final"
    |
    +-- AuxDataPipeline: download SP3 + CLK from FTP
    +-- Hermite interpolation: SP3 XYZ -> target epoch grid
    +-- Write Zarr cache: aux_{date}.zarr
    |
    Per file:
    +-- Open Zarr, sel(epoch) -> sat XYZ at obs times
    +-- compute_spherical_coordinates(sat_XYZ, rx_pos)
    +-- ds["theta"], ds["phi"], ds["r"]
```

### SBF broadcast (SatVisibility)

```
config.ephemeris_source = "broadcast"
config.reader_format = "sbf"
    |
    No download, no Zarr cache
    |
    Per file:
    +-- SBF reader extracts SatVisibility -> sbf_obs dataset
    +-- theta/phi already computed by receiver firmware
    +-- Align to obs epochs + SIDs
    +-- ds["theta"], ds["phi"]
    |
    Note: for reference receiver in shared position mode,
    use canopy receiver's sbf_obs (matched by timestamp)
```

### RINEX NAV broadcast (future)

```
config.ephemeris_source = "broadcast"
config.reader_format = "rinex3"
    |
    No download
    +-- Parse .YYp NAV files (georinex -> xarray)
    +-- Keplerian propagation: orbital elements -> XYZ at target epochs
    +-- Write Zarr cache: aux_{date}.zarr (same format as SP3 path)
    |
    Per file:
    +-- Open Zarr, sel(epoch) -> sat XYZ at obs times
    +-- compute_spherical_coordinates(sat_XYZ, rx_pos)
    +-- ds["theta"], ds["phi"], ds["r"]
```

The key insight: the RINEX NAV path reuses the entire downstream pipeline
(Zarr cache, per-file sel, SCS computation). Only the source of XYZ coordinates
changes. This is why the `EphemerisProvider` abstraction works: `preprocess_day()`
produces the Zarr cache, `augment_dataset()` consumes it.

---

## 7. Summary of Receiver File Types

| Extension | Format | Contains | Use in canvodpy |
|-----------|--------|----------|-----------------|
| `.YYo` | RINEX 3 OBS | Pseudorange, phase, SNR, Doppler | Primary observation data |
| `.YY_` | SBF binary | All of the above + SatVisibility, PVT, DOP, raw measurements | Primary observation data + broadcast geometry |
| `.YYp` | RINEX 3 NAV (mixed) | Broadcast ephemerides (Keplerian elements, all constellations) | Broadcast ephemeris source (Phase 3) |
| `.YYn` | RINEX 2 NAV (GPS) | GPS broadcast ephemerides only | Legacy broadcast source |
| `.YYg` | RINEX NAV (GLONASS) | GLONASS broadcast ephemerides (state vectors) | Legacy broadcast source |
| `.YY1` | NMEA | Position fixes, GSV (integer-deg elev/az), timestamps | **Not used** -- too coarse for VOD |
| `.ubx` | u-blox binary | Similar to SBF but u-blox protocol | Future (Phase 4) |

---

## 8. Dependencies to Add

| Phase | Package | Purpose | Optional? |
|-------|---------|---------|-----------|
| Phase 3 | `georinex` | Parse RINEX NAV files to xarray | Yes (only for NAV broadcast) |
| Phase 4 | `pyubx2` | Parse u-blox binary files | Yes (only for UBX receivers) |

No new dependencies needed for Phase 1 (SBF cleanup) or Phase 2 (abstraction).

---

## 9. Testing Strategy

| Test | Validates |
|------|-----------|
| Unit: `test_keplerian_propagator.py` | GPS/Galileo/BeiDou propagation against SP3 truth (expect <2m) |
| Unit: `test_glonass_propagator.py` | GLONASS RK4 integration against SP3 truth |
| Unit: `test_ephemeris_provider.py` | Provider ABC, factory, config resolution |
| Integration: `test_broadcast_vs_final.py` | Compare theta/phi from broadcast vs final for same day (expect <0.00001 deg) |
| Integration: `test_nav_round_trip.py` | NAV parse -> propagate -> theta/phi matches SBF SatVisibility |
| Regression: existing store comparison tests | Ensure final-product path unchanged |

---

## 10. Data Validation Gap: Pydantic Coverage Across Readers

The RINEX 3 reader has comprehensive Pydantic validation. The SBF reader has only
lightweight structural models. Any new reader (NAV, UBX) should match the RINEX
reader's validation depth.

### Current state

**RINEX 3 reader** (`canvod-readers/src/canvod/readers/rinex/` + `gnss_specs/models.py`):

Full Pydantic validation pipeline:

| Model | Validates |
|-------|-----------|
| `RnxObsFileModel` | File-level: path, hash, size, format version |
| `RnxVersion3Model` | Header: RINEX version, file type, satellite system |
| `Rnxv3ObsEpochRecordCompletenessModel` | Epoch record: flag, n_sats, timestamp, satellite list |
| `Rnxv3ObsEpochRecordLineModel` | Observation line: SID format, obs codes, value ranges |
| `RINEX304ComplianceValidator` | Full spec compliance: header completeness, obs code validity |
| `VodDataValidator` | Dataset-level: required vars present, dims correct, no all-NaN |

Every parsed record passes through Pydantic before entering the dataset. Malformed
lines are caught early with clear error messages.

**SBF reader** (`canvod-readers/src/canvod/readers/sbf/models.py`):

Minimal structural models only:

| Model | Validates |
|-------|-----------|
| `SbfHeader` | Block header: sync bytes, CRC, block number, length, TOW, WNc |
| `SbfSignalObs` | Signal observation: type, lock_time, CN0, values |
| `SbfEpoch` | Epoch container: TOW, signals list |

Missing validation:

- No value range checks on observations (SNR, pseudorange, phase)
- No satellite ID validation (SVID decoding is inline, not validated)
- No SatVisibility field validation (elevation 0-90, azimuth 0-360)
- No epoch timestamp sanity checks (future dates, negative TOW)
- No dataset-level validator (equivalent of `VodDataValidator`)
- No file-level integrity model (equivalent of `RnxObsFileModel`)

### What each new reader needs

**RINEX NAV reader (Phase 3):**

```python
# Proposed validation models
class NavEphemerisRecord(BaseModel):
    """Single broadcast ephemeris parameter set."""
    sv: str                          # e.g. "G01", "E05", "R24"
    toe: float                       # Time of ephemeris (s of week)
    # Keplerian elements (GPS/Galileo/BeiDou)
    sqrt_a: float = Field(ge=0)      # sqrt(semi-major axis), must be positive
    e: float = Field(ge=0, lt=1)     # eccentricity, [0, 1)
    i0: float                        # inclination (rad)
    omega0: float                    # RAAN (rad)
    omega: float                     # argument of perigee (rad)
    m0: float                        # mean anomaly (rad)
    dn: float                        # mean motion correction (rad/s)
    # ... correction terms, health, accuracy

    @field_validator("sv")
    def validate_sid_format(cls, v):
        """Ensure SV ID matches constellation pattern."""
        ...

class GlonassEphemerisRecord(BaseModel):
    """GLONASS state vector ephemeris (different from Keplerian)."""
    sv: str
    tb: float                        # Reference time
    x: float                         # ECEF X (km)
    y: float                         # ECEF Y (km)
    z: float                         # ECEF Z (km)
    vx: float                        # Velocity X (km/s)
    vy: float                        # Velocity Y (km/s)
    vz: float                        # Velocity Z (km/s)
    # ... frequency number, health, age

class NavFileModel(BaseModel):
    """File-level validation for RINEX NAV files."""
    path: Path
    rinex_version: float = Field(ge=3.0, lt=5.0)
    file_type: Literal["N"]
    satellite_system: str             # "M" (mixed), "G", "R", "E", "C"
    records: list[NavEphemerisRecord | GlonassEphemerisRecord]

    @model_validator(mode="after")
    def check_validity_intervals(self):
        """Warn if ephemeris records have gaps > 4 hours."""
        ...
```

**SBF reader (Phase 1 cleanup):**

The SBF reader should gain validation comparable to RINEX, but the challenge is
that SBF is a binary format with packed structs -- validation happens during
decoding rather than text parsing. Recommended additions:

```python
class SbfSatVisibilityObs(BaseModel):
    """Validated SatVisibility observation per satellite."""
    svid: int = Field(ge=1, le=255)
    elevation_cdeg: int = Field(ge=0, le=9000)    # 0-90 deg in centidegrees
    azimuth_cdeg: int = Field(ge=0, le=36000)     # 0-360 deg in centidegrees
    rise_set: Literal["rise", "set", "unknown"]

class SbfMeasEpochValidated(BaseModel):
    """Validated MeasEpoch block."""
    tow_ms: int = Field(ge=0, le=604800000)       # 0 to 1 week in ms
    wnc: int = Field(ge=0)
    n_obs: int = Field(ge=0, le=1000)
    signals: list[SbfSignalObs]

    @model_validator(mode="after")
    def check_signal_count(self):
        """Verify n_obs matches actual signal count."""
        ...

class SbfDatasetValidator(BaseModel):
    """Dataset-level validation (equivalent to VodDataValidator)."""
    model_config = ConfigDict(arbitrary_types_allowed=True)
    ds: xr.Dataset

    @model_validator(mode="after")
    def validate_dataset(self):
        """Check required vars, dims, no all-NaN slices."""
        ...
```

**UBX reader (Phase 4):**

Same pattern. `pyubx2` handles binary decoding; Pydantic validates the decoded
fields. Key blocks to validate: UBX-RXM-RAWX (raw measurements),
UBX-NAV-SAT (satellite info), UBX-NAV-PVT (position/velocity/time).

### Validation architecture principle

All readers should follow the same layered validation:

1. **Record-level** (Pydantic model per parsed record): value ranges, field types
2. **File-level** (Pydantic model per file): header integrity, record count consistency
3. **Dataset-level** (Pydantic validator on xr.Dataset): required variables, dimensions,
   no degenerate data (all-NaN epochs/SIDs)

This ensures that regardless of input format (RINEX text, SBF binary, UBX binary,
NAV text), the output xr.Dataset meets the same quality contract before entering
the store.

---

## 11. Open questions

1. **Clock corrections in broadcast mode:** The broadcast path currently skips
   clock corrections entirely. For VOD (which only uses theta/phi), this is fine.
   But if canvodpy ever needs precise pseudorange corrections, broadcast clock
   from NAV files would be needed. Include in Phase 3?

2. **Ionosphere/troposphere:** Currently not applied in any path. The NAV files
   contain ionosphere model coefficients (Klobuchar for GPS, NeQuick for Galileo).
   Not needed for VOD but useful metadata. Capture in schema?

3. **Multi-constellation NAV file handling:** Rosalia's `.25p` files are mixed
   GNSS (GPS + GLONASS + Galileo + BeiDou in one file). The Keplerian propagator
   must handle the GLONASS exception (state vectors vs Keplerian elements)
   within a single file parse.

4. **Validity interval:** Broadcast ephemerides are valid for ~2-4 hours around
   their reference epoch (toe). The propagator must select the closest valid
   ephemeris set for each target epoch, not simply the first one found.
   SP3 files don't have this issue (they provide positions at regular intervals).
