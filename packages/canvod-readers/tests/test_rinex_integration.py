"""Integration tests for RINEX reading with full signal mapping."""

from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from canvod.readers.rinex.v3_04 import Rnxv3Header, Rnxv3Obs

# Test data paths
TEST_DATA_DIR = Path(__file__).parent / "test_data"
RINEX_FILE = (
    TEST_DATA_DIR
    / "valid/rinex_v3_04/01_Rosalia/02_canopy/01_GNSS/01_raw/25001/ract001a00.25o"
)


@pytest.fixture
def rinex_file():
    """Fixture providing path to test RINEX file."""
    if not RINEX_FILE.exists():
        pytest.skip(f"Test file not found: {RINEX_FILE}")
    return RINEX_FILE


class TestRINEXIntegration:
    """Integration tests for RINEX reading with signal mapping."""

    def test_rinex_header_with_signal_mapping(self, rinex_file):
        """Test RINEX header parsing works with signal mapping system."""
        header = Rnxv3Header.from_file(rinex_file)

        assert header is not None
        assert header.version >= 3.0
        assert header.obs_codes_per_system is not None

        # Verify observation codes are parsed
        for system, obs_codes in header.obs_codes_per_system.items():
            assert isinstance(obs_codes, list)
            assert len(obs_codes) > 0

    def test_rinex_obs_initialization_with_signal_mapping(self, rinex_file):
        """Test Rnxv3Obs initializes with signal mapper."""
        obs = Rnxv3Obs(fpath=rinex_file, completeness_mode="off")

        assert obs is not None
        assert hasattr(obs, "_signal_mapper")
        assert obs._signal_mapper is not None

    def test_signal_id_creation_from_rinex(self, rinex_file):
        """Test signal IDs are created correctly from RINEX data."""
        obs = Rnxv3Obs(fpath=rinex_file, completeness_mode="off")

        # Read some epochs
        epochs = list(obs.iter_epochs())
        assert len(epochs) > 0

        # Get first epoch
        first_epoch = epochs[0]
        assert len(first_epoch.data) > 0

        # Check signal ID format in first observation
        first_sat = first_epoch.data[0]
        if hasattr(first_sat, "sv"):
            # Signal IDs should be created during processing
            # This will be verified in dataset creation
            pass

    def test_dataset_creation_with_signal_mapping(self, rinex_file):
        """Test xarray Dataset is created with proper signal IDs."""
        obs = Rnxv3Obs(fpath=rinex_file, completeness_mode="off")

        # Convert to dataset
        ds = obs.to_ds(keep_rnx_data_vars=["SNR"])

        assert isinstance(ds, xr.Dataset)

        # Check required dimensions
        assert "epoch" in ds.dims
        assert "sid" in ds.dims

        # Check signal IDs have correct format: "SV|BAND|CODE"
        for sid in ds.sid.values:
            sid_str = str(sid)
            parts = sid_str.split("|")
            assert len(parts) == 3, f"Invalid signal ID format: {sid_str}"

            sv, band, code = parts

            # Check SV format: System letter + 2 digits
            assert len(sv) == 3, f"Invalid SV format: {sv}"
            assert sv[0] in "GRECJSI", f"Unknown GNSS system: {sv[0]}"
            assert sv[1:].isdigit(), f"Invalid PRN number: {sv[1:]}"

            # Check band is not empty
            assert len(band) > 0, f"Empty band in signal ID: {sid_str}"

            # Check code is not empty
            assert len(code) > 0, f"Empty code in signal ID: {sid_str}"

    def test_band_frequencies_in_dataset(self, rinex_file):
        """Test band frequencies are correctly assigned in dataset."""
        obs = Rnxv3Obs(fpath=rinex_file, completeness_mode="off")
        ds = obs.to_ds(keep_rnx_data_vars=["SNR"])

        # Check frequency coordinates exist
        assert "freq_center" in ds.coords
        assert "freq_min" in ds.coords
        assert "freq_max" in ds.coords

        # Check frequencies are valid (excluding NaN values from padded sids)
        freq_center = ds.freq_center.values
        valid_freqs = freq_center[~np.isnan(freq_center)]
        assert len(valid_freqs) > 0, "Should have some valid frequencies"
        assert np.all(valid_freqs > 0), "All valid frequencies should be positive"
        assert np.all(valid_freqs < 3000), "Frequencies should be < 3000 MHz"

        # Check frequency ranges (excluding NaN values)
        freq_min = ds.freq_min.values
        freq_max = ds.freq_max.values
        valid_mask = ~(np.isnan(freq_min) | np.isnan(freq_max))
        assert np.all(freq_min[valid_mask] < freq_max[valid_mask]), (
            "freq_min should be < freq_max"
        )

    def test_band_coordinate_in_dataset(self, rinex_file):
        """Test band coordinate is correctly set."""
        obs = Rnxv3Obs(fpath=rinex_file, completeness_mode="off")
        ds = obs.to_ds(keep_rnx_data_vars=["SNR"])

        assert "band" in ds.coords

        # Check band names are valid
        for band in ds.band.values:
            band_str = str(band)
            # Common band names: L1, L2, L5, E1, E5a, E5b, G1, G2, B1I, etc.
            assert len(band_str) >= 2, f"Invalid band name: {band_str}"

    def test_system_coordinate_matches_signal_ids(self, rinex_file):
        """Test system coordinate matches signal ID system letters."""
        obs = Rnxv3Obs(fpath=rinex_file, completeness_mode="off")
        ds = obs.to_ds(keep_rnx_data_vars=["SNR"])

        assert "system" in ds.coords

        # Check each system matches the signal ID (skip padded sids with no data)
        for i, sid in enumerate(ds.sid.values):
            sv_system = str(sid).split("|")[0][0]
            dataset_system = str(ds.system.values[i])

            # Skip NaN values from padded sids
            if dataset_system == "nan":
                continue

            assert sv_system == dataset_system, (
                f"System mismatch: SID has {sv_system}, dataset has {dataset_system}"
            )

    def test_overlapping_group_filtering(self, rinex_file):
        """Test overlapping signal group filtering works."""
        obs = Rnxv3Obs(fpath=rinex_file, completeness_mode="off")

        # Get signal mapper
        mapper = obs._signal_mapper

        # Check overlapping groups exist
        assert len(mapper.OVERLAPPING_GROUPS) > 0

        # Test group membership
        l1_group = mapper.get_overlapping_group("L1")
        assert l1_group is not None

        # L1 and E1 should be in same group
        e1_group = mapper.get_overlapping_group("E1")
        if e1_group is not None:
            assert l1_group == e1_group

    def test_multiple_data_variables(self, rinex_file):
        """Test multiple observation types can be read."""
        obs = Rnxv3Obs(fpath=rinex_file, completeness_mode="off")

        # Try to read multiple data variables
        ds = obs.to_ds(keep_rnx_data_vars=["SNR", "Pseudorange", "Phase"])

        # Check which ones are actually present
        available_vars = []
        for var in ["SNR", "Pseudorange", "Phase"]:
            if var in ds.data_vars:
                available_vars.append(var)

        assert len(available_vars) > 0, "At least one data variable should be present"

        # Check each available variable has correct dimensions
        for var in available_vars:
            assert ds[var].dims == ("epoch", "sid")

    def test_epoch_iteration_with_signal_mapping(self, rinex_file):
        """Test epoch iteration works with signal mapping."""
        obs = Rnxv3Obs(fpath=rinex_file, completeness_mode="off")

        epoch_count = 0
        for epoch in obs.iter_epochs():
            epoch_count += 1

            # Check epoch structure
            assert hasattr(epoch, "info")
            assert hasattr(epoch, "data")
            assert len(epoch.data) > 0

            # Check first few epochs only (for speed)
            if epoch_count >= 5:
                break

        assert epoch_count > 0, "Should have at least one epoch"

    def test_file_hash_generation(self, rinex_file):
        """Test file hash is generated correctly."""
        obs = Rnxv3Obs(fpath=rinex_file, completeness_mode="off")

        file_hash = obs.file_hash
        assert file_hash is not None
        assert len(file_hash) == 16  # Short hash
        assert all(c in "0123456789abcdef" for c in file_hash)

    def test_dataset_global_attributes(self, rinex_file):
        """Test dataset has proper global attributes."""
        obs = Rnxv3Obs(fpath=rinex_file, completeness_mode="off")
        ds = obs.to_ds(keep_rnx_data_vars=["SNR"])

        # Check required global attributes
        required_attrs = ["Created", "Software", "Institution"]
        for attr in required_attrs:
            assert attr in ds.attrs, f"Missing attribute: {attr}"

    @pytest.mark.slow
    def test_full_file_processing(self, rinex_file):
        """Test full RINEX file processing (slow test)."""
        obs = Rnxv3Obs(fpath=rinex_file, completeness_mode="off")

        # Process entire file
        ds = obs.to_ds(keep_rnx_data_vars=["SNR"])

        # Check dataset is not empty
        assert ds.sizes["epoch"] > 0
        assert ds.sizes["sid"] > 0

        # Check data is not all NaN
        if "SNR" in ds.data_vars:
            snr_data = ds["SNR"].values
            assert not np.all(np.isnan(snr_data)), "SNR data should not be all NaN"


class TestSignalMappingEdgeCases:
    """Test edge cases in signal mapping."""

    def test_unknown_system(self):
        """Test handling of unknown GNSS systems."""
        from canvod.readers.gnss_specs.signals import SignalIDMapper

        mapper = SignalIDMapper()

        # Unknown system should still produce a signal ID
        sid = mapper.create_signal_id("X01", "X01|S1C")
        assert sid is not None
        assert "Unknown" in sid or "X01" in sid

    def test_malformed_obs_code(self):
        """Test handling of malformed observation codes."""
        from canvod.readers.gnss_specs.signals import SignalIDMapper

        mapper = SignalIDMapper()

        # Malformed codes should not crash
        sid = mapper.create_signal_id("G01", "invalid")
        assert sid is not None

    def test_empty_signal_id(self):
        """Test handling of empty signal IDs."""
        from canvod.readers.gnss_specs.signals import SignalIDMapper

        mapper = SignalIDMapper()

        # Empty signal ID
        sv, band, code = mapper.parse_signal_id("")
        assert sv == "nan"
        assert band == "nan"
        assert code == "nan"

    def test_nonexistent_band_frequency(self):
        """Test getting frequency for nonexistent band."""
        from canvod.readers.gnss_specs.signals import SignalIDMapper

        mapper = SignalIDMapper()

        # Nonexistent band
        freq = mapper.get_band_frequency("NONEXISTENT")
        assert freq is None

    def test_nonexistent_band_bandwidth(self):
        """Test getting bandwidth for nonexistent band."""
        from canvod.readers.gnss_specs.signals import SignalIDMapper

        mapper = SignalIDMapper()

        # Nonexistent band
        bw = mapper.get_band_bandwidth("NONEXISTENT")
        assert bw is None

    def test_nonexistent_overlapping_group(self):
        """Test getting overlapping group for isolated band."""
        from canvod.readers.gnss_specs.signals import SignalIDMapper

        mapper = SignalIDMapper()

        # Band might not be in any overlapping group
        group = mapper.get_overlapping_group("NONEXISTENT")
        assert group is None
