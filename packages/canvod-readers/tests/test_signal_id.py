"""Tests for SignalID Pydantic model."""

import pytest
from pydantic import ValidationError

from canvod.readers.base import SignalID


class TestSignalIDValidation:
    """Test SV validation."""

    @pytest.mark.parametrize(
        "sv",
        ["G01", "G32", "R01", "R24", "E01", "E36", "C01", "C63", "J01", "S01", "I01"],
    )
    def test_valid_svs(self, sv: str):
        sid = SignalID(sv=sv, band="L1", code="C")
        assert sid.sv == sv

    @pytest.mark.parametrize(
        "sv",
        ["X01", "G1", "G001", "01G", "", "g01", "G0A"],
    )
    def test_invalid_svs(self, sv: str):
        with pytest.raises(ValidationError, match="Invalid SV"):
            SignalID(sv=sv, band="L1", code="C")


class TestSignalIDProperties:
    """Test computed properties."""

    def test_system_property(self):
        assert SignalID(sv="G01", band="L1", code="C").system == "G"
        assert SignalID(sv="E25", band="E5a", code="I").system == "E"
        assert SignalID(sv="R12", band="G1", code="P").system == "R"

    def test_sid_property(self):
        sig = SignalID(sv="G01", band="L1", code="C")
        assert sig.sid == "G01|L1|C"

    def test_str(self):
        sig = SignalID(sv="G01", band="L1", code="C")
        assert str(sig) == "G01|L1|C"

    def test_hash(self):
        sig1 = SignalID(sv="G01", band="L1", code="C")
        sig2 = SignalID(sv="G01", band="L1", code="C")
        assert hash(sig1) == hash(sig2)
        assert sig1 == sig2

    def test_hash_different(self):
        sig1 = SignalID(sv="G01", band="L1", code="C")
        sig2 = SignalID(sv="G01", band="L2", code="C")
        assert hash(sig1) != hash(sig2)
        assert sig1 != sig2

    def test_usable_in_set(self):
        sig1 = SignalID(sv="G01", band="L1", code="C")
        sig2 = SignalID(sv="G01", band="L1", code="C")
        sig3 = SignalID(sv="G02", band="L1", code="C")
        s = {sig1, sig2, sig3}
        assert len(s) == 2

    def test_frozen(self):
        sig = SignalID(sv="G01", band="L1", code="C")
        with pytest.raises(ValidationError):
            sig.sv = "G02"


class TestSignalIDFromString:
    """Test from_string factory."""

    def test_valid(self):
        sig = SignalID.from_string("G01|L1|C")
        assert sig.sv == "G01"
        assert sig.band == "L1"
        assert sig.code == "C"

    def test_round_trip(self):
        original = SignalID(sv="E25", band="E5a", code="I")
        restored = SignalID.from_string(str(original))
        assert original == restored

    @pytest.mark.parametrize(
        "bad_str",
        ["G01|L1", "G01", "G01|L1|C|extra", ""],
    )
    def test_invalid_format(self, bad_str: str):
        with pytest.raises(ValueError, match="Invalid SID format"):
            SignalID.from_string(bad_str)

    @pytest.mark.parametrize("bad_str", ["X01|L1|C", "|L1|C"])
    def test_invalid_sv_in_string(self, bad_str: str):
        with pytest.raises(ValidationError, match="Invalid SV"):
            SignalID.from_string(bad_str)
