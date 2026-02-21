"""Integration tests for SbfReader — ABC compliance, to_ds(), to_metadata_ds()."""

from __future__ import annotations

from datetime import timezone
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from canvod.readers.base import DatasetStructureValidator, GNSSDataReader
from canvod.readers.sbf.reader import SbfReader

# ---------------------------------------------------------------------------
# Test data location
# ---------------------------------------------------------------------------

# SBF test file lives outside the submodule at .processing/sbf_rnx_data/
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
SBF_FILE = _PROJECT_ROOT / ".processing/sbf_rnx_data/rref213a00.25_"


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def sbf_file() -> Path:
    """Skip the test module if the SBF test file is absent."""
    if not SBF_FILE.exists():
        pytest.skip(f"SBF test file not found: {SBF_FILE}")
    return SBF_FILE


@pytest.fixture(scope="module")
def reader(sbf_file: Path) -> SbfReader:
    """One SbfReader for the whole module (file-scan results are cached)."""
    return SbfReader(fpath=sbf_file)


@pytest.fixture(scope="module")
def obs_ds(reader: SbfReader) -> xr.Dataset:
    """Full observation dataset (all six data vars, no padding)."""
    return reader.to_ds(pad_global_sid=False, strip_fillval=False)


@pytest.fixture(scope="module")
def meta_ds(reader: SbfReader) -> xr.Dataset:
    """Full metadata dataset (no padding)."""
    return reader.to_metadata_ds(pad_global_sid=False)


# ---------------------------------------------------------------------------
# Test classes
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestSbfReaderABC:
    """SbfReader satisfies the GNSSDataReader ABC."""

    def test_is_gnss_data_reader(self, reader: SbfReader) -> None:
        assert isinstance(reader, GNSSDataReader)

    def test_file_hash(self, reader: SbfReader) -> None:
        fh = reader.file_hash
        assert isinstance(fh, str)
        assert len(fh) == 16
        assert all(c in "0123456789abcdef" for c in fh)

    def test_start_end_time(self, reader: SbfReader) -> None:
        st = reader.start_time
        et = reader.end_time
        assert st < et
        assert st.tzinfo is not None  # tz-aware UTC
        assert st.tzinfo == timezone.utc

    def test_systems(self, reader: SbfReader) -> None:
        valid = {"G", "R", "E", "C", "J", "I", "S"}
        systems = reader.systems
        assert isinstance(systems, list)
        assert len(systems) > 0
        assert set(systems).issubset(valid), f"Unexpected systems: {set(systems) - valid}"

    def test_num_epochs(self, reader: SbfReader) -> None:
        assert reader.num_epochs == 180

    def test_num_satellites(self, reader: SbfReader) -> None:
        assert reader.num_satellites > 0


@pytest.mark.integration
class TestToDs:
    """to_ds() produces a valid (epoch, sid) observation dataset."""

    def test_to_ds_validates(self, obs_ds: xr.Dataset) -> None:
        validator = DatasetStructureValidator(dataset=obs_ds)
        validator.validate_all()  # raises ValueError on failure

    def test_to_ds_dims(self, obs_ds: xr.Dataset) -> None:
        assert "epoch" in obs_ds.dims
        assert "sid" in obs_ds.dims

    def test_to_ds_coords(self, obs_ds: xr.Dataset) -> None:
        required = {"epoch", "sid", "sv", "system", "band", "code",
                    "freq_center", "freq_min", "freq_max"}
        missing = required - set(obs_ds.coords)
        assert not missing, f"Missing coords: {missing}"

    def test_to_ds_required_vars(self, obs_ds: xr.Dataset) -> None:
        for var in ("SNR", "Pseudorange", "Phase", "Doppler"):
            assert var in obs_ds.data_vars, f"Missing data var: {var}"
        # SBF has no loss-of-lock indicator — LLI must not be present
        assert "LLI" not in obs_ds.data_vars, "LLI should be absent from SBF datasets"

    def test_to_ds_file_hash_attr(self, obs_ds: xr.Dataset) -> None:
        assert "File Hash" in obs_ds.attrs

    def test_to_ds_snr_values(self, obs_ds: xr.Dataset) -> None:
        snr = obs_ds["SNR"].values
        valid = snr[~np.isnan(snr)]
        assert len(valid) > 0, "SNR array is all-NaN"
        assert np.any(valid > 20), "Expected some SNR values > 20 dB-Hz"

    def test_to_ds_sid_format(self, obs_ds: xr.Dataset) -> None:
        for sid in obs_ds.sid.values:
            parts = str(sid).split("|")
            assert len(parts) == 3, f"Bad SID format: {sid}"
            sv, band, code = parts
            assert len(sv) >= 3, f"Bad SV length: {sv}"
            assert sv[0] in "GRECJSI?", f"Unknown system in SV: {sv}"
            assert len(band) >= 2, f"Empty band in {sid}"
            assert len(code) >= 1, f"Empty code in {sid}"

    def test_to_ds_freq_coords_positive(self, obs_ds: xr.Dataset) -> None:
        for coord in ("freq_center", "freq_min", "freq_max"):
            vals = obs_ds[coord].values
            valid = vals[~np.isnan(vals)]
            assert np.all(valid > 0), f"{coord} has non-positive values"

    def test_to_ds_epoch_count(self, obs_ds: xr.Dataset) -> None:
        assert obs_ds.sizes["epoch"] == 180


@pytest.mark.integration
class TestToMetadataDs:
    """to_metadata_ds() produces a valid (epoch, sid) metadata dataset."""

    def test_to_metadata_ds_dims(self, meta_ds: xr.Dataset, obs_ds: xr.Dataset) -> None:
        assert "epoch" in meta_ds.dims
        assert "sid" in meta_ds.dims
        # Must span at least as many epochs as observations
        assert meta_ds.sizes["epoch"] == obs_ds.sizes["epoch"]

    def test_to_metadata_ds_sid_coverage(
        self, meta_ds: xr.Dataset, obs_ds: xr.Dataset
    ) -> None:
        obs_sids = set(obs_ds.sid.values)
        meta_sids = set(meta_ds.sid.values)
        missing = obs_sids - meta_sids
        assert not missing, f"Metadata dataset missing sids from observations: {missing}"

    def test_to_metadata_ds_theta_phi(self, meta_ds: xr.Dataset) -> None:
        theta = meta_ds["theta"].values
        phi = meta_ds["phi"].values
        valid_theta = theta[~np.isnan(theta)]
        valid_phi = phi[~np.isnan(phi)]
        if len(valid_theta) > 0:
            assert np.all(valid_theta >= 0), "theta values must be >= 0"
            assert np.all(valid_theta <= 90), "theta (zenith angle) must be <= 90"
        if len(valid_phi) > 0:
            assert np.all(valid_phi >= 0), "phi values must be >= 0"
            assert np.all(valid_phi < 360), "phi (azimuth) must be < 360"
        # Provenance attrs must be present
        assert "source" in meta_ds["theta"].attrs, "theta must have 'source' provenance attr"
        assert "source" in meta_ds["phi"].attrs, "phi must have 'source' provenance attr"

    def test_to_metadata_ds_epoch_coords(self, meta_ds: xr.Dataset) -> None:
        for coord in ("pdop", "hdop", "n_sv"):
            assert coord in meta_ds.coords, f"Missing epoch coord: {coord}"

    def test_to_metadata_ds_data_vars(self, meta_ds: xr.Dataset) -> None:
        for var in ("theta", "phi", "rise_set", "mp_correction_m"):
            assert var in meta_ds.data_vars, f"Missing metadata data var: {var}"

    def test_to_metadata_ds_epoch_coord_dims(self, meta_ds: xr.Dataset) -> None:
        for coord in ("pdop", "hdop", "vdop", "n_sv", "cpu_load"):
            if coord in meta_ds.coords:
                assert meta_ds[coord].dims == ("epoch",), (
                    f"Coord {coord} should be 1-D over epoch"
                )

    def test_to_metadata_ds_file_hash_attr(self, meta_ds: xr.Dataset) -> None:
        assert "File Hash" in meta_ds.attrs
