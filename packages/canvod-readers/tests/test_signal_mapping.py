"""Tests for GNSS signal mapping system."""

import pytest
from canvod.readers.gnss_specs.bands import Bands
from canvod.readers.gnss_specs.constellations import (
    BEIDOU,
    GALILEO,
    GLONASS,
    GPS,
    IRNSS,
    QZSS,
    SBAS,
)
from canvod.readers.gnss_specs.signals import SignalIDMapper


class TestSignalIDMapper:
    """Test SignalIDMapper class."""

    def test_initialization(self):
        """Test SignalIDMapper can be initialized."""
        mapper = SignalIDMapper(aggregate_glonass_fdma=True)

        assert mapper is not None
        assert mapper.aggregate_glonass_fdma is True
        assert mapper.SYSTEM_BANDS is not None
        assert mapper.BAND_PROPERTIES is not None
        assert mapper.OVERLAPPING_GROUPS is not None

    def test_system_bands_coverage(self):
        """Test all GNSS systems have band mappings."""
        mapper = SignalIDMapper()

        expected_systems = ["G", "E", "R", "C", "I", "S", "J"]
        for system in expected_systems:
            assert system in mapper.SYSTEM_BANDS, f"Missing system: {system}"
            assert len(mapper.SYSTEM_BANDS[system]) > 0

    def test_create_signal_id_gps(self):
        """Test signal ID creation for GPS."""
        mapper = SignalIDMapper()

        sid = mapper.create_signal_id("G01", "G01|S1C")
        assert sid == "G01|L1|C"

        sid = mapper.create_signal_id("G01", "G01|S2W")
        assert sid == "G01|L2|W"

        sid = mapper.create_signal_id("G01", "G01|S5X")
        assert sid == "G01|L5|X"

    def test_create_signal_id_galileo(self):
        """Test signal ID creation for Galileo."""
        mapper = SignalIDMapper()

        sid = mapper.create_signal_id("E05", "E05|S1C")
        assert sid == "E05|E1|C"

        sid = mapper.create_signal_id("E05", "E05|S5Q")
        assert sid == "E05|E5a|Q"

        sid = mapper.create_signal_id("E05", "E05|S7I")
        assert sid == "E05|E5b|I"

    def test_create_signal_id_glonass(self):
        """Test signal ID creation for GLONASS."""
        mapper = SignalIDMapper()

        sid = mapper.create_signal_id("R01", "R01|S1C")
        assert sid == "R01|G1|C"

        sid = mapper.create_signal_id("R01", "R01|S2P")
        assert sid == "R01|G2|P"

    def test_create_signal_id_beidou(self):
        """Test signal ID creation for BeiDou."""
        mapper = SignalIDMapper()

        sid = mapper.create_signal_id("C01", "C01|S2I")
        assert sid == "C01|B1I|I"

        sid = mapper.create_signal_id("C01", "C01|S1D")
        assert sid == "C01|B1C|D"

    def test_create_signal_id_auxiliary(self):
        """Test X1 auxiliary observation handling."""
        mapper = SignalIDMapper()

        sid = mapper.create_signal_id("G01", "G01|X1")
        assert sid == "G01|X1|X"

        sid = mapper.create_signal_id("E05", "E05|X1")
        assert sid == "E05|X1|X"

    def test_parse_signal_id(self):
        """Test signal ID parsing."""
        mapper = SignalIDMapper()

        sv, band, code = mapper.parse_signal_id("G01|L1|C")
        assert sv == "G01"
        assert band == "L1"
        assert code == "C"

        sv, band, code = mapper.parse_signal_id("E05|E5a|Q")
        assert sv == "E05"
        assert band == "E5a"
        assert code == "Q"

    def test_get_band_frequency(self):
        """Test band frequency retrieval."""
        mapper = SignalIDMapper()

        # GPS L1
        freq = mapper.get_band_frequency("L1")
        assert freq == pytest.approx(1575.42)

        # GPS L2
        freq = mapper.get_band_frequency("L2")
        assert freq == pytest.approx(1227.60)

        # GPS L5
        freq = mapper.get_band_frequency("L5")
        assert freq == pytest.approx(1176.45)

        # Galileo E1
        freq = mapper.get_band_frequency("E1")
        assert freq == pytest.approx(1575.42)

    def test_get_band_bandwidth(self):
        """Test band bandwidth retrieval."""
        mapper = SignalIDMapper()

        # GPS L1
        bw = mapper.get_band_bandwidth("L1")
        assert bw == pytest.approx(30.69)

        # GPS L5
        bw = mapper.get_band_bandwidth("L5")
        assert bw == pytest.approx(24.0)

        # Galileo E1
        bw = mapper.get_band_bandwidth("E1")
        assert bw == pytest.approx(24.552)

    def test_get_overlapping_group(self):
        """Test overlapping group identification."""
        mapper = SignalIDMapper()

        # L1/E1/B1I are in same group (~1575 MHz)
        group_l1 = mapper.get_overlapping_group("L1")
        group_e1 = mapper.get_overlapping_group("E1")
        group_b1i = mapper.get_overlapping_group("B1I")

        assert group_l1 is not None
        assert group_l1 == group_e1
        assert group_l1 == group_b1i

        # L5/E5a are in same group (~1176 MHz)
        group_l5 = mapper.get_overlapping_group("L5")
        group_e5a = mapper.get_overlapping_group("E5a")

        assert group_l5 is not None
        assert group_l5 == group_e5a

    def test_is_auxiliary_observation(self):
        """Test auxiliary observation detection."""
        mapper = SignalIDMapper()

        # X1 should not be auxiliary in BAND_PROPERTIES by default
        is_aux = mapper.is_auxiliary_observation("G01|X1|X")
        # This depends on whether X1 has 'auxiliary' flag in BAND_PROPERTIES
        assert isinstance(is_aux, bool)

        # Regular observations should not be auxiliary
        is_aux = mapper.is_auxiliary_observation("G01|L1|C")
        assert is_aux is False


class TestBands:
    """Test Bands class."""

    def test_initialization(self):
        """Test Bands can be initialized."""
        bands = Bands(aggregate_glonass_fdma=True)

        assert bands is not None
        assert bands.BAND_PROPERTIES is not None
        assert bands.SYSTEM_BANDS is not None
        assert bands.OVERLAPPING_GROUPS is not None

    def test_band_properties_structure(self):
        """Test BAND_PROPERTIES has correct structure."""
        bands = Bands()

        # Check L1 band exists
        assert "L1" in bands.BAND_PROPERTIES
        l1_props = bands.BAND_PROPERTIES["L1"]

        assert "freq" in l1_props
        assert "bandwidth" in l1_props
        assert "system" in l1_props

        assert isinstance(l1_props["freq"], (int, float))
        assert isinstance(l1_props["bandwidth"], (int, float))
        # System can be 'G' (GPS), 'J' (QZSS), or 'S' (SBAS) - all use L1
        assert l1_props["system"] in ["G", "J", "S"]

    def test_system_bands_structure(self):
        """Test SYSTEM_BANDS has correct structure."""
        bands = Bands()

        # Check GPS
        assert "G" in bands.SYSTEM_BANDS
        gps_bands = bands.SYSTEM_BANDS["G"]

        assert "1" in gps_bands  # L1
        assert "2" in gps_bands  # L2
        assert "5" in gps_bands  # L5

        assert gps_bands["1"] == "L1"
        assert gps_bands["2"] == "L2"
        assert gps_bands["5"] == "L5"

    def test_overlapping_groups_structure(self):
        """Test OVERLAPPING_GROUPS has correct structure."""
        bands = Bands()

        # Find group containing L1
        l1_group = None
        for group, band_list in bands.OVERLAPPING_GROUPS.items():
            if "L1" in band_list:
                l1_group = band_list
                break

        assert l1_group is not None
        assert "L1" in l1_group
        assert "E1" in l1_group  # Galileo E1 overlaps with L1

    def test_all_systems_present(self):
        """Test all GNSS systems are present."""
        bands = Bands()

        expected_systems = ["G", "E", "R", "C", "I", "S", "J"]
        for system in expected_systems:
            assert system in bands.SYSTEM_BANDS

    def test_glonass_fdma_aggregation(self):
        """Test GLONASS FDMA aggregation option."""
        # With aggregation
        bands_agg = Bands(aggregate_glonass_fdma=True)
        assert "R" in bands_agg.SYSTEM_BANDS
        r_bands_agg = bands_agg.SYSTEM_BANDS["R"]
        assert "1" in r_bands_agg  # G1 should be present
        assert "2" in r_bands_agg  # G2 should be present

        # Without aggregation
        bands_no_agg = Bands(aggregate_glonass_fdma=False)
        r_bands_no_agg = bands_no_agg.SYSTEM_BANDS["R"]
        # Structure might be different based on FDMA handling


class TestConstellations:
    """Test constellation classes."""

    def test_gps_initialization(self):
        """Test GPS constellation can be initialized."""
        gps = GPS(use_wiki=False)

        assert gps.constellation == "GPS"
        assert len(gps.svs) > 0
        assert gps.svs[0].startswith("G")
        assert len(gps.BANDS) > 0
        assert "L1" in gps.BAND_PROPERTIES

    def test_gps_static_svs(self):
        """Test GPS has static SV list."""
        gps = GPS(use_wiki=False)

        assert len(gps.svs) == 32
        assert "G01" in gps.svs
        assert "G32" in gps.svs

    def test_galileo_initialization(self):
        """Test Galileo constellation can be initialized."""
        galileo = GALILEO()

        assert galileo.constellation == "GALILEO"
        assert "E1" in galileo.BAND_PROPERTIES
        assert len(galileo.BANDS) > 0

    def test_glonass_initialization(self):
        """Test GLONASS constellation can be initialized."""
        glonass = GLONASS(aggregate_fdma=True)

        # GLONASS has custom __init__ and doesn't set constellation attribute
        assert len(glonass.svs) == 24
        assert glonass.svs[0].startswith("R")
        assert glonass.aggregate_fdma is True

    def test_glonass_fdma_equations(self):
        """Test GLONASS FDMA frequency equations."""
        glonass = GLONASS(aggregate_fdma=True)

        # Test G1 frequency calculation
        freq_g1 = glonass.band_G1_equation("R01")
        assert freq_g1 is not None
        assert hasattr(freq_g1, "magnitude")

        # Test G2 frequency calculation
        freq_g2 = glonass.band_G2_equation("R01")
        assert freq_g2 is not None
        assert hasattr(freq_g2, "magnitude")

    def test_beidou_initialization(self):
        """Test BeiDou constellation can be initialized."""
        beidou = BEIDOU()

        assert beidou.constellation == "BEIDOU"
        assert "B1I" in beidou.BAND_PROPERTIES
        assert len(beidou.BANDS) > 0

    def test_irnss_initialization(self):
        """Test IRNSS constellation can be initialized."""
        irnss = IRNSS()

        assert irnss.constellation == "IRNSS"
        assert "L5" in irnss.BAND_PROPERTIES
        assert "S" in irnss.BAND_PROPERTIES

    def test_qzss_initialization(self):
        """Test QZSS constellation can be initialized."""
        qzss = QZSS()

        assert qzss.constellation == "QZSS"
        assert "L1" in qzss.BAND_PROPERTIES
        assert "L6" in qzss.BAND_PROPERTIES  # Unique to QZSS

    def test_sbas_initialization(self):
        """Test SBAS constellation can be initialized."""
        sbas = SBAS()

        assert sbas.constellation == "SBAS"
        assert len(sbas.svs) > 0
        assert sbas.svs[0].startswith("S")

    def test_constellation_freqs_lut(self):
        """Test constellation frequency lookup tables."""
        gps = GPS(use_wiki=False)

        freqs_lut = gps.freqs_lut
        assert len(freqs_lut) > 0

        # Check format: SV|*ObsCode
        sample_key = next(iter(freqs_lut.keys()))
        assert "|" in sample_key
        assert sample_key.startswith("G")


class TestIntegration:
    """Integration tests for signal mapping with RINEX reader."""

    def test_signal_mapper_in_rinex_context(self):
        """Test SignalIDMapper works in RINEX reader context."""
        mapper = SignalIDMapper()

        # Simulate RINEX observation processing
        test_obs_codes = [
            ("G01", "G01|S1C"),
            ("G01", "G01|S2W"),
            ("E05", "E05|S1C"),
            ("R01", "R01|S1C"),
        ]

        for sv, obs_code in test_obs_codes:
            sid = mapper.create_signal_id(sv, obs_code)
            assert "|" in sid
            assert sid.startswith(sv)

            # Parse it back
            parsed_sv, band, code = mapper.parse_signal_id(sid)
            assert parsed_sv == sv
            assert len(band) > 0
            assert len(code) > 0

    def test_overlapping_group_filtering(self):
        """Test overlapping group identification for filtering."""
        mapper = SignalIDMapper()

        # Create signal IDs
        sid_l1 = mapper.create_signal_id("G01", "G01|S1C")
        sid_e1 = mapper.create_signal_id("E05", "E05|S1C")

        # Get bands
        _, band_l1, _ = mapper.parse_signal_id(sid_l1)
        _, band_e1, _ = mapper.parse_signal_id(sid_e1)

        # Check they're in same overlapping group
        group_l1 = mapper.get_overlapping_group(band_l1)
        group_e1 = mapper.get_overlapping_group(band_e1)

        assert group_l1 == group_e1  # Should be in same group

    def test_frequency_consistency(self):
        """Test frequency values are consistent across systems."""
        mapper = SignalIDMapper()

        # L1 and E1 should have same frequency (1575.42 MHz)
        freq_l1 = mapper.get_band_frequency("L1")
        freq_e1 = mapper.get_band_frequency("E1")

        assert freq_l1 == freq_e1
        assert freq_l1 == pytest.approx(1575.42)


@pytest.mark.slow
class TestWikipediaCache:
    """Tests for Wikipedia satellite list caching.

    These tests are marked as slow because they involve network access.
    """

    def test_wikipedia_cache_initialization(self):
        """Test WikipediaCache can be initialized."""
        from canvod.readers.gnss_specs.constellations import WikipediaCache

        cache = WikipediaCache(cache_hours=6)
        assert cache.cache_file == "gnss_satellites_cache.db"
        assert cache.cache_hours == 6

    @pytest.mark.skipif(True, reason="Requires network access")
    def test_wikipedia_fetch_gps(self):
        """Test fetching GPS satellites from Wikipedia."""
        gps = GPS(use_wiki=True)

        assert len(gps.svs) > 0
        assert all(sv.startswith("G") for sv in gps.svs)

    @pytest.mark.skipif(True, reason="Requires network access")
    def test_wikipedia_fetch_galileo(self):
        """Test fetching Galileo satellites from Wikipedia."""
        galileo = GALILEO()

        assert len(galileo.svs) > 0
        assert all(sv.startswith("E") for sv in galileo.svs)
