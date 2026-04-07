"""Test NMEA v4.00 reader functionality."""

from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from canvod.readers.nmea.exceptions import (
    NmeaChecksumError,
    NmeaInvalidSentenceError,
    NmeaMissingSentenceError,
)
from canvod.readers.nmea.v4_00 import (
    NmeaObs,
    _parse_gsv_satellites,
    _parse_rmc_datetime,
    _sv_to_sid,
    compute_nmea_checksum,
    prn_to_sv,
    validate_nmea_checksum,
)

# Test data paths
TEST_DATA_DIR = Path(__file__).parent / "test_data"
NMEA_FILE = (
    TEST_DATA_DIR / "valid/nmea/01_reference/ROSR01TUW_R_20250010000_15M_05S_AA.nmea"
)


@pytest.fixture
def nmea_file():
    """Fixture providing path to test NMEA file."""
    if not NMEA_FILE.exists():
        pytest.skip(f"Test file not found: {NMEA_FILE}")
    return NMEA_FILE


class TestNmeaChecksum:
    """Tests for NMEA checksum computation and validation."""

    def test_compute_checksum(self):
        """Test checksum computation for a known sentence."""
        sentence = (
            "$GPRMC,001442.00,A,4742.1601213,N,01618.1004213,E,0.0,,010125,5.1,E,D*16"
        )
        assert compute_nmea_checksum(sentence) == "16"

    def test_validate_checksum_pass(self):
        """Test checksum validation passes for valid sentence."""
        sentence = (
            "$GPRMC,001442.00,A,4742.1601213,N,01618.1004213,E,0.0,,010125,5.1,E,D*16"
        )
        validate_nmea_checksum(sentence)  # Should not raise

    def test_validate_checksum_fail(self):
        """Test checksum validation raises on bad checksum."""
        sentence = (
            "$GPRMC,001442.00,A,4742.1601213,N,01618.1004213,E,0.0,,010125,5.1,E,D*FF"
        )
        with pytest.raises(NmeaChecksumError):
            validate_nmea_checksum(sentence)

    def test_invalid_sentence_format(self):
        """Test error on malformed sentence."""
        with pytest.raises(NmeaInvalidSentenceError):
            compute_nmea_checksum("not a valid sentence")


class TestPrnToSv:
    """Tests for PRN-to-SV mapping."""

    def test_gps_prn(self):
        assert prn_to_sv("GP", 1) == "G01"
        assert prn_to_sv("GP", 32) == "G32"

    def test_sbas_in_gp(self):
        assert prn_to_sv("GP", 33) == "S01"
        assert prn_to_sv("GP", 48) == "S16"

    def test_glonass(self):
        assert prn_to_sv("GL", 1) == "R01"
        assert prn_to_sv("GL", 24) == "R24"
        assert prn_to_sv("GL", 65) == "R01"  # Global ID format

    def test_galileo(self):
        assert prn_to_sv("GA", 1) == "E01"
        assert prn_to_sv("GA", 36) == "E36"

    def test_beidou(self):
        assert prn_to_sv("GB", 1) == "C01"
        assert prn_to_sv("GB", 63) == "C63"

    def test_invalid_prn(self):
        assert prn_to_sv("GP", 0) is None
        assert prn_to_sv("GP", 100) is None
        assert prn_to_sv("GA", 37) is None
        assert prn_to_sv("XX", 1) is None

    def test_gn_multi_constellation(self):
        assert prn_to_sv("GN", 1) == "G01"
        assert prn_to_sv("GN", 65) == "R01"
        assert prn_to_sv("GN", 301) == "E01"
        assert prn_to_sv("GN", 401) == "C01"


class TestSvToSid:
    """Tests for SV-to-SID mapping."""

    def test_gps_sid(self):
        assert _sv_to_sid("G01") == "G01|L1|C"

    def test_glonass_sid(self):
        assert _sv_to_sid("R05") == "R05|G1|C"

    def test_galileo_sid(self):
        assert _sv_to_sid("E12") == "E12|E1|C"

    def test_beidou_sid(self):
        assert _sv_to_sid("C30") == "C30|B1I|I"

    def test_sbas_sid(self):
        assert _sv_to_sid("S01") == "S01|L1|C"


class TestRmcParsing:
    """Tests for RMC datetime parsing."""

    def test_valid_rmc(self):
        fields = [
            "001442.00",
            "A",
            "4742.16",
            "N",
            "01618.10",
            "E",
            "0.0",
            "",
            "010125",
            "5.1",
            "E",
            "D",
        ]
        dt = _parse_rmc_datetime(fields)
        assert dt is not None
        assert dt.year == 2025
        assert dt.month == 1
        assert dt.day == 1
        assert dt.hour == 0
        assert dt.minute == 14
        assert dt.second == 42

    def test_empty_fields(self):
        assert _parse_rmc_datetime([]) is None

    def test_missing_date(self):
        fields = ["001442.00", "A", "", "", "", "", "", "", ""]
        assert _parse_rmc_datetime(fields) is None


class TestGsvParsing:
    """Tests for GSV satellite parsing."""

    def test_gps_gsv(self):
        # $GPGSV,6,1,21,02,85,302,50,08,22,184,42,28,16,099,40,32,36,053,46*7C
        fields = [
            "6",
            "1",
            "21",
            "02",
            "85",
            "302",
            "50",
            "08",
            "22",
            "184",
            "42",
            "28",
            "16",
            "099",
            "40",
            "32",
            "36",
            "053",
            "46",
        ]
        sats = _parse_gsv_satellites("GP", fields)
        assert len(sats) == 4
        assert sats[0] == ("G02", 50.0)
        assert sats[1] == ("G08", 42.0)
        assert sats[2] == ("G28", 40.0)
        assert sats[3] == ("G32", 46.0)

    def test_glonass_gsv(self):
        fields = [
            "2",
            "1",
            "08",
            "84",
            "81",
            "349",
            "41",
            "70",
            "12",
            "216",
            "35",
            "83",
            "29",
            "134",
            "38",
            "85",
            "26",
            "321",
            "47",
        ]
        sats = _parse_gsv_satellites("GL", fields)
        assert len(sats) == 4
        assert sats[0][0] == "R20"  # GL PRN 84 → 84-64=20
        assert sats[0][1] == 41.0

    def test_galileo_gsv(self):
        fields = [
            "3",
            "1",
            "11",
            "11",
            "83",
            "055",
            "44",
            "02",
            "13",
            "285",
            "39",
            "09",
            "27",
            "185",
            "43",
            "12",
            "31",
            "111",
            "40",
        ]
        sats = _parse_gsv_satellites("GA", fields)
        assert len(sats) == 4
        assert sats[0] == ("E11", 44.0)

    def test_beidou_gsv(self):
        fields = [
            "5",
            "1",
            "20",
            "29",
            "64",
            "281",
            "51",
            "19",
            "12",
            "226",
            "42",
            "35",
            "11",
            "315",
            "41",
            "39",
            "24",
            "061",
            "42",
        ]
        sats = _parse_gsv_satellites("GB", fields)
        assert len(sats) == 4
        assert sats[0] == ("C29", 51.0)

    def test_missing_snr(self):
        # SNR field empty → None
        fields = ["5", "1", "18", "22", "15", "219", "", "35", "16", "315", "43"]
        sats = _parse_gsv_satellites("GB", fields)
        assert len(sats) == 2
        assert sats[0] == ("C22", None)
        assert sats[1] == ("C35", 43.0)

    def test_empty_gsv(self):
        # Last message with no satellites: $GPGSV,6,6,21*7A
        fields = ["6", "6", "21"]
        sats = _parse_gsv_satellites("GP", fields)
        assert len(sats) == 0


class TestNmeaObs:
    """Tests for the NmeaObs reader."""

    def test_initialization(self, nmea_file):
        """Test NmeaObs can be initialized."""
        obs = NmeaObs(fpath=nmea_file)
        assert obs is not None
        assert obs.fpath == nmea_file

    def test_file_hash(self, nmea_file):
        """Test file hash is generated."""
        obs = NmeaObs(fpath=nmea_file)
        assert obs.file_hash
        assert len(obs.file_hash) == 16
        assert all(c in "0123456789abcdef" for c in obs.file_hash)

    def test_start_end_time(self, nmea_file):
        """Test start and end time are parsed."""
        obs = NmeaObs(fpath=nmea_file)
        assert obs.start_time < obs.end_time

    def test_systems(self, nmea_file):
        """Test multiple GNSS systems are detected."""
        obs = NmeaObs(fpath=nmea_file)
        systems = obs.systems
        assert len(systems) > 1
        # Should have at least GPS and one other
        assert "G" in systems

    def test_num_epochs(self, nmea_file):
        """Test epochs are parsed."""
        obs = NmeaObs(fpath=nmea_file)
        assert obs.num_epochs > 0

    def test_num_satellites(self, nmea_file):
        """Test satellites are detected."""
        obs = NmeaObs(fpath=nmea_file)
        assert obs.num_satellites > 0

    def test_source_format(self, nmea_file):
        """Test source format is nmea."""
        obs = NmeaObs(fpath=nmea_file)
        assert obs.source_format == "nmea"

    def test_iter_epochs(self, nmea_file):
        """Test epoch iteration works."""
        obs = NmeaObs(fpath=nmea_file)
        epochs = list(obs.iter_epochs())
        assert len(epochs) > 0
        assert epochs[0].timestamp is not None
        assert len(epochs[0].satellites) > 0

    def test_to_ds_basic(self, nmea_file):
        """Test conversion to xarray Dataset."""
        obs = NmeaObs(fpath=nmea_file)
        ds = obs.to_ds()

        assert isinstance(ds, xr.Dataset)
        assert "epoch" in ds.dims
        assert "sid" in ds.dims
        assert "SNR" in ds.data_vars

    def test_to_ds_coordinates(self, nmea_file):
        """Test Dataset has required coordinates."""
        obs = NmeaObs(fpath=nmea_file)
        ds = obs.to_ds()

        required_coords = ["epoch", "sid", "sv", "system", "band", "code"]
        for coord in required_coords:
            assert coord in ds.coords, f"Missing coordinate: {coord}"

    def test_to_ds_frequency_info(self, nmea_file):
        """Test Dataset has frequency information."""
        obs = NmeaObs(fpath=nmea_file)
        ds = obs.to_ds()

        assert "freq_center" in ds.coords
        assert "freq_min" in ds.coords
        assert "freq_max" in ds.coords

    def test_to_ds_metadata(self, nmea_file):
        """Test Dataset has required global attributes."""
        obs = NmeaObs(fpath=nmea_file)
        ds = obs.to_ds()

        assert "Created" in ds.attrs
        assert "Software" in ds.attrs
        assert "Institution" in ds.attrs
        assert "File Hash" in ds.attrs

    def test_to_ds_snr_has_valid_values(self, nmea_file):
        """Test SNR data contains valid values."""
        obs = NmeaObs(fpath=nmea_file)
        ds = obs.to_ds()

        snr = ds["SNR"].values
        valid = snr[~np.isnan(snr)]
        assert len(valid) > 0
        assert valid.min() >= 0
        assert valid.max() <= 99  # NMEA SNR range

    def test_to_ds_snr_dims(self, nmea_file):
        """Test SNR has correct dimensions."""
        obs = NmeaObs(fpath=nmea_file)
        ds = obs.to_ds()
        assert ds["SNR"].dims == ("epoch", "sid")


class TestNmeaSignalMapping:
    """Tests for signal ID mapping in NMEA."""

    def test_signal_ids_format(self, nmea_file):
        """Test signal IDs have correct SV|BAND|CODE format."""
        obs = NmeaObs(fpath=nmea_file)
        ds = obs.to_ds()

        for sid in ds.sid.values:
            parts = str(sid).split("|")
            assert len(parts) == 3, f"Invalid signal ID format: {sid}"

            sv, band, code = parts
            assert len(sv) == 3, f"SV should be 3 chars: {sv}"
            assert sv[0] in "GRECSI", f"Invalid system prefix: {sv[0]}"
            assert sv[1:3].isdigit(), f"PRN should be digits: {sv[1:3]}"

    def test_system_coordinate_matches_sid(self, nmea_file):
        """Test system coordinate matches signal IDs."""
        obs = NmeaObs(fpath=nmea_file)
        ds = obs.to_ds()

        for i, sid in enumerate(ds.sid.values):
            sv_system = str(sid).split("|")[0][0]
            dataset_system = str(ds.system.values[i])
            assert dataset_system == sv_system

    def test_band_names_are_valid(self, nmea_file):
        """Test band names match SignalIDMapper expectations."""
        obs = NmeaObs(fpath=nmea_file)
        ds = obs.to_ds()

        valid_bands = {
            "L1",
            "L2",
            "L5",
            "G1",
            "G2",
            "E1",
            "E5a",
            "E5b",
            "E6",
            "B1I",
            "B1C",
            "B2a",
            "B2b",
            "B3I",
        }
        for band in ds.band.values:
            assert str(band) in valid_bands, f"Unexpected band name: {band}"

    def test_multiple_constellations(self, nmea_file):
        """Test that multiple GNSS systems are present."""
        obs = NmeaObs(fpath=nmea_file)
        ds = obs.to_ds()

        systems_found = set()
        for sid in ds.sid.values:
            systems_found.add(str(sid).split("|")[0][0])

        assert len(systems_found) > 1


class TestNmeaErrorHandling:
    """Tests for error handling."""

    def test_nonexistent_file(self):
        """Test error on nonexistent file."""
        with pytest.raises((ValueError, FileNotFoundError)):
            NmeaObs(fpath=Path("/nonexistent/file.nmea"))

    def test_empty_file(self, tmp_path):
        """Test error on empty file."""
        empty_file = tmp_path / "empty.nmea"
        empty_file.write_text("")
        with pytest.raises(NmeaMissingSentenceError):
            obs = NmeaObs(fpath=empty_file)
            obs.to_ds()

    def test_no_gsv_sentences(self, tmp_path):
        """Test error when file has no GSV sentences."""
        f = tmp_path / "no_gsv.nmea"
        f.write_text(
            "$GPRMC,001442.00,A,4742.1601213,N,01618.1004213,E,0.0,,010125,5.1,E,D*16\n"
        )
        with pytest.raises(NmeaMissingSentenceError):
            obs = NmeaObs(fpath=f)
            obs.to_ds()


class TestNmeaMultipleFiles:
    """Tests reading multiple NMEA files."""

    REFERENCE_DIR = TEST_DATA_DIR / "valid/nmea/01_reference"
    CANOPY_DIR = TEST_DATA_DIR / "valid/nmea/02_canopy"

    def test_read_second_file(self):
        """Test reading a different time segment."""
        f = self.REFERENCE_DIR / "ROSR01TUW_R_20250010015_15M_05S_AA.nmea"
        if not f.exists():
            pytest.skip(f"Test file not found: {f}")

        obs = NmeaObs(fpath=f)
        ds = obs.to_ds()

        assert isinstance(ds, xr.Dataset)
        assert ds.sizes["epoch"] > 0
        assert ds.sizes["sid"] > 0

    def test_file_hashes_differ(self):
        """Test that different files produce different hashes."""
        f1 = self.REFERENCE_DIR / "ROSR01TUW_R_20250010000_15M_05S_AA.nmea"
        f2 = self.REFERENCE_DIR / "ROSR01TUW_R_20250010015_15M_05S_AA.nmea"
        if not f1.exists() or not f2.exists():
            pytest.skip("Test files not found")

        obs1 = NmeaObs(fpath=f1)
        obs2 = NmeaObs(fpath=f2)
        assert obs1.file_hash != obs2.file_hash

    def test_read_canopy_file(self):
        """Test reading a canopy receiver NMEA file."""
        f = self.CANOPY_DIR / "ROSA01TUW_R_20250010000_15M_05S_AA.nmea"
        if not f.exists():
            pytest.skip(f"Test file not found: {f}")

        obs = NmeaObs(fpath=f)
        ds = obs.to_ds()

        assert isinstance(ds, xr.Dataset)
        assert ds.sizes["epoch"] > 0

    def test_reference_and_canopy_share_sids(self):
        """Test that reference and canopy share common signal IDs."""
        ref_file = self.REFERENCE_DIR / "ROSR01TUW_R_20250010000_15M_05S_AA.nmea"
        canopy_file = self.CANOPY_DIR / "ROSA01TUW_R_20250010000_15M_05S_AA.nmea"

        if not ref_file.exists() or not canopy_file.exists():
            pytest.skip("Test files not found")

        ref_ds = NmeaObs(fpath=ref_file).to_ds()
        canopy_ds = NmeaObs(fpath=canopy_file).to_ds()

        ref_sids = set(str(s) for s in ref_ds.sid.values)
        canopy_sids = set(str(s) for s in canopy_ds.sid.values)

        common_sids = ref_sids & canopy_sids
        assert len(common_sids) > 0, "Reference and canopy should share signal IDs"
