# Extending Readers

Add support for a new GNSS data format by implementing the `GNSSDataReader` abstract base class. canvod-readers uses the ABC pattern to enforce a consistent contract — any reader that passes the checklist below can be used anywhere `GNSSDataReader` is accepted.

---

## Implementation Checklist

<div class="grid cards" markdown>

-   :fontawesome-solid-code: &nbsp; **1. Inherit correctly**

    ---

    `class MyReader(BaseModel, GNSSDataReader)` — both `BaseModel`
    (for Pydantic validation) and the ABC.

-   :fontawesome-solid-list-check: &nbsp; **2. Implement all abstract methods**

    ---

    `file_hash`, `to_ds()`, `iter_epochs()`, `start_time`, `end_time`,
    `systems`, `num_epochs`, `num_satellites`.

-   :fontawesome-solid-shield-halved: &nbsp; **3. Call `validate_output()`**

    ---

    Last line of every `to_ds()` must be
    `self.validate_output(ds, required_vars=...)`.
    Never skip it.

-   :fontawesome-solid-vial: &nbsp; **4. Write tests**

    ---

    Test structure, file hash, error paths, and the validation round-trip.
    Aim for >90 % coverage.

</div>

---

## Step-by-Step Implementation

### Step 1 — Reader Class

```python
from pathlib import Path
from pydantic import BaseModel, ConfigDict
from canvod.readers.base import GNSSDataReader

class MyFormatReader(BaseModel, GNSSDataReader):
    """Reader for My Custom Format.

    Implements GNSSDataReader ABC for custom GNSS data format.
    """

    model_config = ConfigDict(
        frozen=True,                  # Immutable after construction
        arbitrary_types_allowed=True, # Required for Path, numpy, etc.
    )

    fpath: Path
```

### Step 2 — File Hash

```python
from canvod.readers.gnss_specs.utils import rinex_file_hash

class MyFormatReader(BaseModel, GNSSDataReader):
    ...

    @property
    def file_hash(self) -> str:
        """16-character SHA-256 prefix of the file — used for deduplication."""
        return rinex_file_hash(self.fpath)
```

### Step 3 — Metadata Properties

```python
class MyFormatReader(BaseModel, GNSSDataReader):
    ...

    @property
    def start_time(self) -> datetime:
        return self._parse_start_time()

    @property
    def end_time(self) -> datetime:
        return self._parse_end_time()

    @property
    def systems(self) -> list[str]:
        return self._parse_systems()   # e.g. ["G", "E"]

    @property
    def num_epochs(self) -> int:
        return sum(1 for _ in self.iter_epochs())

    @property
    def num_satellites(self) -> int:
        return len({obs.sv for ep in self.iter_epochs() for obs in ep.observations})
```

### Step 4 — Epoch Iterator

```python
from typing import Generator

class MyFormatReader(BaseModel, GNSSDataReader):
    ...

    def iter_epochs(self) -> Generator:
        """Lazily yield one epoch at a time — keep memory bounded."""
        with open(self.fpath, "rb") as f:
            self._skip_header(f)
            for raw in self._raw_epoch_generator(f):
                yield self._decode_epoch(raw)
```

### Step 5 — Dataset Conversion

```python
import numpy as np
import xarray as xr
from canvod.readers.gnss_specs.signals import SignalIDMapper
from canvod.readers.gnss_specs.metadata import SNR_METADATA, COORDS_METADATA, get_global_attrs

class MyFormatReader(BaseModel, GNSSDataReader):
    ...

    def to_ds(
        self,
        keep_rnx_data_vars: list[str] | None = None,
        **kwargs,
    ) -> xr.Dataset:
        # 1. Collect
        all_epochs = list(self.iter_epochs())

        # 2. Build SID index
        mapper = SignalIDMapper()
        all_sids = sorted({
            mapper.create_signal_id(obs.sv, obs.code)
            for ep in all_epochs
            for obs in ep.observations
        })

        # 3. Coordinate arrays
        epochs     = [ep.timestamp for ep in all_epochs]
        sv_arr     = np.array([sid.split("|")[0] for sid in all_sids])
        system_arr = np.array([sid[0]            for sid in all_sids])
        band_arr   = np.array([sid.split("|")[1] for sid in all_sids])
        code_arr   = np.array([sid.split("|")[2] for sid in all_sids])
        fc         = np.array([mapper.get_band_frequency(sid.split("|")[1])
                               for sid in all_sids], dtype=np.float64)
        bw         = np.array([mapper.get_band_bandwidth(sid.split("|")[1])
                               for sid in all_sids], dtype=np.float64)

        # 4. Data arrays (SNR minimum; extend for Phase / PR / Doppler)
        sid_to_idx = {sid: i for i, sid in enumerate(all_sids)}
        snr = np.full((len(epochs), len(all_sids)), np.nan, dtype=np.float32)
        for i, ep in enumerate(all_epochs):
            for obs in ep.observations:
                sid = mapper.create_signal_id(obs.sv, obs.code)
                snr[i, sid_to_idx[sid]] = obs.snr

        # 5. Assemble Dataset
        ds = xr.Dataset(
            data_vars={"SNR": (("epoch", "sid"), snr, SNR_METADATA)},
            coords={
                "epoch":      ("epoch", epochs, COORDS_METADATA["epoch"]),
                "sid":        ("sid", all_sids, COORDS_METADATA["sid"]),
                "sv":         ("sid", sv_arr,     COORDS_METADATA["sv"]),
                "system":     ("sid", system_arr, COORDS_METADATA["system"]),
                "band":       ("sid", band_arr,   COORDS_METADATA["band"]),
                "code":       ("sid", code_arr,   COORDS_METADATA["code"]),
                "freq_center":("sid", fc,          COORDS_METADATA["freq_center"]),
                "freq_min":   ("sid", fc - bw / 2, COORDS_METADATA["freq_min"]),
                "freq_max":   ("sid", fc + bw / 2, COORDS_METADATA["freq_max"]),
            },
            attrs={
                **get_global_attrs(),
                "RINEX File Hash": self.file_hash,
                "Source Format":   "My Custom Format",
            },
        )

        # 6. MANDATORY — validate before returning
        self.validate_output(ds, required_vars=keep_rnx_data_vars)
        return ds
```

---

---

## Validation Requirements

=== "Dimensions"

    ```python
    assert "epoch" in ds.dims
    assert "sid"   in ds.dims
    ```

=== "Coordinates"

    ```python
    required_coords = {
        "epoch":       "datetime64[ns]",
        "sid":         "object",     # string
        "sv":          "object",
        "system":      "object",
        "band":        "object",
        "code":        "object",
        "freq_center": "float64",    # must be float64, NOT float32
        "freq_min":    "float64",
        "freq_max":    "float64",
    }
    ```

=== "Attributes"

    ```python
    required_attrs = {
        "Created",
        "Software",
        "Institution",
        "RINEX File Hash",   # for storage deduplication
    }
    ```

=== "Data Variables"

    ```python
    # SNR and Phase required by default
    assert "SNR"   in ds.data_vars
    assert "Phase" in ds.data_vars

    # All variables must be (epoch, sid)
    for var in ds.data_vars:
        assert ds[var].dims == ("epoch", "sid")
    ```

---

## Testing

=== "Unit Tests"

    ```python
    # tests/test_my_format_reader.py
    import pytest
    from pathlib import Path
    from my_package.readers import MyFormatReader

    class TestMyFormatReader:

        def test_file_hash_is_deterministic(self, tmp_path):
            f = tmp_path / "test.dat"
            f.write_bytes(b"content")
            reader = MyFormatReader(fpath=f)
            assert reader.file_hash == reader.file_hash
            assert len(reader.file_hash) == 16

        def test_dataset_dimensions(self, real_test_file):
            ds = MyFormatReader(fpath=real_test_file).to_ds()
            assert "epoch" in ds.dims
            assert "sid"   in ds.dims

        def test_dataset_variables(self, real_test_file):
            ds = MyFormatReader(fpath=real_test_file).to_ds()
            assert "SNR"   in ds.data_vars
            assert "Phase" in ds.data_vars

        def test_sid_dimensions(self, real_test_file):
            ds = MyFormatReader(fpath=real_test_file).to_ds()
            for var in ds.data_vars:
                assert ds[var].dims == ("epoch", "sid")

        def test_file_hash_in_attrs(self, real_test_file):
            ds = MyFormatReader(fpath=real_test_file).to_ds()
            assert "RINEX File Hash" in ds.attrs
    ```

=== "Integration Test"

    ```python
    @pytest.mark.integration
    def test_full_pipeline(real_test_file):
        reader = MyFormatReader(fpath=real_test_file)
        ds = reader.to_ds(keep_rnx_data_vars=["SNR"])

        # Filter GPS only
        gps = ds.where(ds.system == "G", drop=True)
        assert len(gps.sid) > 0

        # Sanity-check values
        assert float(gps.SNR.mean()) > 0
    ```

=== "Validation Round-Trip"

    ```python
    from canvod.readers.base import DatasetStructureValidator

    def test_validation_passes(real_test_file):
        ds = MyFormatReader(fpath=real_test_file).to_ds()
        # validate_output() is already called inside to_ds() —
        # this test verifies it didn't raise
        validator = DatasetStructureValidator(dataset=ds)
        validator.validate_all()   # should not raise
    ```

---

## Common Pitfalls

!!! failure "Wrong dtype for frequency coordinates"
    ```python
    # WRONG — float32 fails the dtype check
    freq_center = np.array([...], dtype=np.float32)

    # CORRECT
    freq_center = np.array([...], dtype=np.float64)
    ```

!!! failure "Skipping validation"
    ```python
    # WRONG — missing mandatory validation
    def to_ds(self, **kwargs) -> xr.Dataset:
        ds = self._build_dataset()
        return ds   # ← will silently produce invalid datasets downstream

    # CORRECT
    def to_ds(self, **kwargs) -> xr.Dataset:
        ds = self._build_dataset()
        self.validate_output(ds)   # mandatory
        return ds
    ```

!!! failure "Wrong dimension names"
    ```python
    # WRONG
    data_vars={"SNR": (("time", "signal"), data)}

    # CORRECT
    data_vars={"SNR": (("epoch", "sid"), data)}
    ```

---

## Registering with ReaderFactory

```python
from canvod.readers.base import ReaderFactory
from my_package.readers import MyFormatReader

# Register
ReaderFactory.register("my_format", MyFormatReader)

# Automatic detection
reader = ReaderFactory.create("file.dat")   # → MyFormatReader
```

Update the detection logic to recognise your format:

```python
# In ReaderFactory._detect_format()
if first_bytes.startswith(b"MY_FORMAT"):
    return "my_format"
```
