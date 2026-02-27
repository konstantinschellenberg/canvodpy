# canvod-readers

## Purpose

The `canvod-readers` package provides validated parsers for GNSS observation data. It transforms raw receiver files into analysis-ready xarray Datasets, serving as the data ingestion layer for GNSS Transmissometry (GNSS-T) analysis.

<div class="grid cards" markdown>

-   :fontawesome-solid-file-lines: &nbsp; **RINEX v3.04 — `Rnxv3Obs`**

    ---

    Text-based, all-GNSS standard format.
    Satellite geometry requires external SP3 + CLK precise ephemerides.

    [:octicons-arrow-right-24: RINEX format](rinex-format.md)

-   :fontawesome-solid-satellite-dish: &nbsp; **SBF Binary — `SbfReader`**

    ---

    Septentrio binary telemetry. Satellite geometry, PVT quality, DOP,
    and receiver health are **embedded** — no ephemeris download needed.

    [:octicons-arrow-right-24: SBF reader](sbf.md)

</div>

---

## Supported Formats at a Glance

| Feature | `Rnxv3Obs` | `SbfReader` |
| ------- | ---------- | ----------- |
| Format | Plain text | Binary |
| Extension | `.rnx`, `.XXo` | `.XX_`, `*.sbf` |
| Satellite geometry (θ, φ) | SP3 download | **Embedded** |
| Extra metadata | Header only | PVT · DOP · quality |
| `to_ds()` | ✓ | ✓ |
| `iter_epochs()` | ✓ | ✓ |
| `to_metadata_ds()` | — | ✓ |
| `to_ds_and_auxiliary()` | `{}` aux | `{"sbf_obs": meta_ds}` |

!!! tip "Drop-in replacement"

    Both readers produce identical `(epoch × sid)` xarray Datasets that pass
    `DatasetStructureValidator`. Downstream code is completely reader-agnostic.

---

## Design

### Data flow

```mermaid
graph TD
    A1["RINEX v3 File (.XXo)"] --> B1["Rnxv3Obs (+ SP3/CLK)"]
    A2["SBF File (.XX_)"] --> B2["SbfReader"]
    B1 --> C["DatasetStructureValidator"]
    B2 --> C
    C --> D["xarray.Dataset\n(epoch × sid)"]
    B2 --> E["Metadata Dataset\n(DOP · PVT · θ · φ)"]
    D --> F["Downstream Analysis"]
    E --> F
```

### Contract-Based Design

All readers implement the `GNSSDataReader` abstract base class:

```python
from abc import ABC, abstractmethod
import xarray as xr

class GNSSDataReader(ABC):
    """Base class for all GNSS data format readers."""

    @abstractmethod
    def to_ds(self, **kwargs) -> xr.Dataset:
        """Convert to xarray.Dataset (epoch × sid)."""

    @abstractmethod
    def iter_epochs(self):
        """Iterate through epochs."""

    @property
    @abstractmethod
    def file_hash(self) -> str:
        """SHA-256 hash for deduplication."""

    def to_ds_and_auxiliary(
        self, **kwargs
    ) -> tuple[xr.Dataset, dict[str, xr.Dataset]]:
        """Single-pass scan: obs dataset + any auxiliary datasets.

        Default returns empty aux dict.
        SbfReader overrides for one-pass binary decode.
        """
        return self.to_ds(**kwargs), {}

```

[:octicons-arrow-right-24: Full architecture](architecture.md)

---

## Usage Examples

=== "RINEX — VOD pipeline"

    ```python
    from canvod.readers import Rnxv3Obs

    reader = Rnxv3Obs(fpath="station.25o")
    ds = reader.to_ds(keep_rnx_data_vars=["SNR"])

    # Filter L-band signals
    l_band = ds.where(ds.band.isin(["L1", "L2", "L5"]), drop=True)
    ```

=== "SBF — quick-look (no downloads)"

    ```python
    from canvod.readers.sbf import SbfReader

    reader = SbfReader(fpath="rref001a00.25_")
    obs_ds, aux = reader.to_ds_and_auxiliary(keep_rnx_data_vars=["SNR"])
    meta_ds = aux["sbf_obs"]

    # Zenith angle filter: elevation ≥ 20°
    snr_filtered = obs_ds["SNR"].where(meta_ds["theta"] <= 70)
    ```

=== "Multi-constellation analysis"

    ```python
    ds = reader.to_ds()

    for system in ["G", "R", "E", "C"]:
        sys_ds = ds.where(ds.system == system, drop=True)
        mean_snr = sys_ds.SNR.mean(dim=["epoch", "sid"])
        print(f"{system}: {mean_snr:.2f} dB")
    ```

=== "Time-series concat"

    ```python
    import xarray as xr
    from pathlib import Path

    datasets = [
        Rnxv3Obs(fpath=f).to_ds(keep_rnx_data_vars=["SNR"])
        for f in sorted(Path("/data/").glob("*.25o"))
    ]

    time_series = xr.concat(datasets, dim="epoch")
    ```

---

## Key Components

<div class="grid cards" markdown>

-   :fontawesome-solid-earth-europe: &nbsp; **GNSS Specifications**

    ---

    `gnss_specs` provides constellation definitions for GPS, GALILEO,
    GLONASS, BeiDou, QZSS, and SBAS including band mappings and
    centre frequencies.

    ```python
    from canvod.readers.gnss_specs import GPS
    gps = GPS()
    gps.BANDS  # {'1': 'L1', '2': 'L2', '5': 'L5'}
    ```

-   :fontawesome-solid-id-badge: &nbsp; **Signal ID Mapper**

    ---

    `SignalIDMapper` converts raw observation codes to canonical
    `SV|Band|Code` signal IDs used across all datasets.

    ```python
    mapper = SignalIDMapper()
    sid = mapper.create_signal_id("G01", "G01|S1C")
    # → "G01|L1|C"
    ```

-   :fontawesome-solid-circle-check: &nbsp; **DatasetStructureValidator**

    ---

    Every dataset produced by any reader must pass structural validation
    before it is returned. Checks dimensions, coordinate dtypes, required
    variables, and global attributes.

    ```python
    validator = DatasetStructureValidator(dataset=ds)
    validator.validate_all()  # raises ValueError if invalid
    ```

</div>

---

## Performance

### Single-Pass Parser

`Rnxv3Obs` uses a single-pass parser that pre-computes the full Signal ID (SID) space from the RINEX header and fills pre-allocated NumPy arrays in one pass over the file. This avoids the overhead of:

- **Two-pass iteration** — epoch batches are cached, SIDs are derived from header metadata
- **Per-observation object allocation** — inline string parsing replaces Pydantic model instantiation
- **Repeated signal ID lookups** — a pre-built lookup table maps `(SV, obs_code)` → array index directly

The fast path is used by default. The original two-pass path is preserved for special features (conflict analysis, system analysis, time slicing).

### Tips

!!! tip "Memory"

    Use `keep_rnx_data_vars=["SNR"]` to load only what you need.
    Full RINEX with phase + Doppler uses ~4× more memory.

!!! tip "Batch processing"

    For many files, use `ProcessPoolExecutor`. Each reader is fully
    picklable and stateless after construction.

!!! tip "Storage"

    After processing, write to Icechunk via `canvod-store` for
    compressed, versioned storage with O(1) epoch lookups.
