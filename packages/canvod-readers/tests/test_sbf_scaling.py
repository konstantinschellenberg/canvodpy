"""Unit tests for SBF physical-unit scaling functions.

All expected values are derived from the Septentrio AsteRx SB3 ProBase
Firmware v4.14.0 Reference Guide (RefGuide-4.14.0).

These are pure-function tests — no test data files required.
"""

from __future__ import annotations

import pytest

from canvod.readers.gnss_specs.constants import SPEEDOFLIGHT, UREG
from canvod.readers.sbf._scaling import (
    _DOPPLER_DNU,
    _signed_n_bit,
    cn0_dbhz,
    decode_offsets_msb,
    decode_signal_num,
    doppler2_hz,
    doppler_hz,
    glonass_freq_hz,
    phase_cycles,
    pr2_m,
    pseudorange_m,
)

# Speed of light for hand-calculations
_C = float(SPEEDOFLIGHT.to(UREG.meter / UREG.second).magnitude)


# ===================================================================
# decode_signal_num — RefGuide-4.14.0, p.261
# Type byte bits 0-4 = SigIdxLo
# If SigIdxLo == 31: signal_num = (ObsInfo >> 3) & 0x1F + 32
# Otherwise: signal_num = SigIdxLo
# ===================================================================


class TestDecodeSignalNum:
    """Signal number extraction from Type/ObsInfo byte pair."""

    @pytest.mark.parametrize(
        "type_byte,obs_info,expected",
        [
            # Normal signals: SigIdxLo < 31 — obs_info ignored
            (0, 0, 0),  # GPS L1CA
            (1, 0, 1),  # GPS L1P
            (2, 0, 2),  # GPS L2P
            (17, 0, 17),  # Galileo E1
            (30, 0, 30),  # BeiDou B3I
            # Antenna bits 5-7 set — must be masked out
            (0b11100000 | 4, 0, 4),  # antenna=7, SigIdxLo=4 → GPS L5
            (0b01100000 | 20, 0, 20),  # antenna=3, SigIdxLo=20 → Galileo E5a
            # Extended signal: SigIdxLo == 31, actual in ObsInfo bits 3-7
            # ObsInfo bits 3-7 = 0 → signal_num = 0 + 32 = 32 (QZSS L1C)
            (31, 0b00000_000, 32),
            # ObsInfo bits 3-7 = 1 → 1 + 32 = 33 (QZSS L1S)
            (31, 0b00001_000, 33),
            # ObsInfo bits 3-7 = 2 → 2 + 32 = 34 (BeiDou B2b)
            (31, 0b00010_000, 34),
            # ObsInfo bits 3-7 = 6 → 6 + 32 = 38 (QZSS L1CB tentative)
            (31, 0b00110_000, 38),
            # ObsInfo bits 3-7 = 7 → 7 + 32 = 39 (QZSS L5S tentative)
            (31, 0b00111_000, 39),
            # Extended + antenna bits + low ObsInfo bits set (must isolate correctly)
            (0b11100000 | 31, 0b00010_101, 34),  # only bits 3-7 matter
        ],
    )
    def test_decode_signal_num(self, type_byte, obs_info, expected):
        assert decode_signal_num(type_byte, obs_info) == expected


# ===================================================================
# cn0_dbhz — RefGuide-4.14.0, p.261
# C/N0 [dB-Hz] = raw * 0.25 + 10  (normal)
# C/N0 [dB-Hz] = raw * 0.25       (signals 1 and 2 only)
# DNU: raw == 255
# ===================================================================


class TestCn0Dbhz:
    """CN0 byte → C/N0 in dB-Hz."""

    def test_dnu_returns_none(self):
        """Do-Not-Use sentinel: raw == 255 (u1 max, field not available)."""
        assert cn0_dbhz(255, 0) is None
        assert cn0_dbhz(255, 17) is None

    @pytest.mark.parametrize(
        "raw,sig_num,expected_dbhz",
        [
            # Normal signals: raw * 0.25 + 10
            (0, 0, 0.0 + 10.0),  # GPS L1CA, raw == 0 is valid (not DNU)
            (1, 0, 0.25 + 10.0),  # GPS L1CA, min non-DNU
            (100, 0, 25.0 + 10.0),  # GPS L1CA
            (200, 17, 50.0 + 10.0),  # Galileo E1
            (160, 20, 40.0 + 10.0),  # Galileo E5a, typical strong signal
            (254, 0, 63.5 + 10.0),  # Max valid raw for GPS L1CA (255 is DNU)
            # Signals 1 and 2: raw * 0.25 (no +10 offset)
            (100, 1, 25.0),  # GPS L1P
            (100, 2, 25.0),  # GPS L2P
            (200, 1, 50.0),  # GPS L1P
            (1, 2, 0.25),  # GPS L2P, min non-DNU
        ],
    )
    def test_cn0_scaling(self, raw, sig_num, expected_dbhz):
        result = cn0_dbhz(raw, sig_num)
        assert result is not None
        assert abs(result.magnitude - expected_dbhz) < 1e-10
        assert str(result.units) == "dBHz"

    def test_cn0_returns_pint_quantity(self):
        result = cn0_dbhz(100, 0)
        assert hasattr(result, "magnitude")
        assert hasattr(result, "units")

    def test_cn0_typical_gnss_range(self):
        """A realistic raw value of 120 on GPS L1CA should give ~40 dB-Hz."""
        result = cn0_dbhz(120, 0)
        assert 30 < result.magnitude < 50


# ===================================================================
# pseudorange_m — RefGuide-4.14.0, p.261
# PR [m] = (CodeMSB * 4294967296 + CodeLSB) * 0.001
# CodeMSB = Misc & 0x0F
# DNU: CodeMSB == 0 AND CodeLSB == 0
# ===================================================================


class TestPseudorangeM:
    """Type1 pseudorange scaling."""

    def test_dnu_returns_none(self):
        """Do-Not-Use: CodeMSB==0 and CodeLSB==0."""
        assert pseudorange_m(misc=0, code_lsb=0) is None

    def test_dnu_misc_has_upper_bits(self):
        """Upper nibble of Misc doesn't affect CodeMSB; DNU still triggers."""
        assert pseudorange_m(misc=0xF0, code_lsb=0) is None

    def test_basic_scaling(self):
        """CodeMSB=0, CodeLSB=20_000_000 → 20_000 m."""
        result = pseudorange_m(misc=0, code_lsb=20_000_000)
        assert result is not None
        assert abs(result.magnitude - 20_000.0) < 1e-6

    def test_code_msb_contribution(self):
        """CodeMSB=1, CodeLSB=0 → 1 * 4294967296 * 0.001 = 4_294_967.296 m."""
        result = pseudorange_m(misc=1, code_lsb=0)
        expected = 4_294_967.296
        assert abs(result.magnitude - expected) < 1e-3

    def test_gps_typical_pseudorange(self):
        """GPS MEO pseudorange ~20,000–26,000 km.

        CodeMSB=0, CodeLSB=22_000_000_000 → 22_000_000 m = 22,000 km.
        """
        result = pseudorange_m(misc=0, code_lsb=22_000_000_000)
        pr_km = result.magnitude / 1000
        assert 19_000 < pr_km < 30_000

    def test_misc_upper_nibble_ignored(self):
        """Bits 4-7 of Misc are reserved; only bits 0-3 (CodeMSB) matter."""
        result_clean = pseudorange_m(misc=0x01, code_lsb=1_000_000)
        result_dirty = pseudorange_m(misc=0xF1, code_lsb=1_000_000)
        assert result_clean.magnitude == result_dirty.magnitude

    def test_returns_meters(self):
        result = pseudorange_m(misc=0, code_lsb=1)
        assert str(result.units) == "meter"


# ===================================================================
# doppler_hz — RefGuide-4.14.0, p.261
# D [Hz] = raw * 0.0001
# DNU: raw == -2147483648 (i4 minimum)
# ===================================================================


class TestDopplerHz:
    """Type1 Doppler scaling."""

    def test_dnu_returns_none(self):
        assert doppler_hz(_DOPPLER_DNU) is None
        assert doppler_hz(-(1 << 31)) is None

    def test_zero_doppler(self):
        result = doppler_hz(0)
        assert result is not None
        assert result.magnitude == 0.0

    @pytest.mark.parametrize(
        "raw,expected_hz",
        [
            (10_000, 1.0),  # 10000 * 0.0001 = 1.0 Hz
            (-10_000, -1.0),
            (1, 0.0001),
            (100_000_000, 10_000.0),  # Strong Doppler
        ],
    )
    def test_doppler_scaling(self, raw, expected_hz):
        result = doppler_hz(raw)
        assert abs(result.magnitude - expected_hz) < 1e-10

    def test_returns_hz_unit(self):
        result = doppler_hz(1)
        assert str(result.units) == "hertz"


# ===================================================================
# phase_cycles — RefGuide-4.14.0, p.261
# L [cycles] = PR [m] / λ + (CarrierMSB * 65536 + CarrierLSB) * 0.001
# λ = c / freq_hz
# DNU: CarrierMSB == -128 AND CarrierLSB == 0
# ===================================================================


class TestPhaseCycles:
    """Type1 carrier phase scaling."""

    def test_dnu_returns_none(self):
        """Do-Not-Use: CarrierMSB==-128 and CarrierLSB==0."""
        pr = 22_000_000.0 * UREG.meter
        freq = 1575.42 * UREG.MHz
        result = phase_cycles(pr, carrier_msb=-128, carrier_lsb=0, freq=freq)
        assert result is None

    def test_not_dnu_when_carrier_msb_minus128_lsb_nonzero(self):
        """If CarrierLSB != 0, this is NOT DNU even with CarrierMSB == -128."""
        pr = 22_000_000.0 * UREG.meter
        freq = 1575.42 * UREG.MHz
        result = phase_cycles(pr, carrier_msb=-128, carrier_lsb=1, freq=freq)
        assert result is not None

    def test_hand_calculation_gps_l1(self):
        """Manual calculation for GPS L1 (1575.42 MHz).

        λ = c / f = 299792458 / 1575420000 = 0.190293673 m
        PR = 22_000_000 m
        CarrierMSB = 0, CarrierLSB = 0
        L = PR / λ + 0 = 22_000_000 / 0.190293673 ≈ 115,610,321.5 cycles
        """
        pr = 22_000_000.0 * UREG.meter
        freq = 1575.42 * UREG.MHz
        result = phase_cycles(pr, carrier_msb=0, carrier_lsb=0, freq=freq)

        lambda_m = _C / 1575.42e6
        expected = 22_000_000.0 / lambda_m
        assert abs(result - expected) < 1.0  # within 1 cycle

    def test_carrier_offset_contribution(self):
        """Verify the (CarrierMSB * 65536 + CarrierLSB) * 0.001 term."""
        pr = 22_000_000.0 * UREG.meter
        freq = 1575.42 * UREG.MHz

        base = phase_cycles(pr, carrier_msb=0, carrier_lsb=0, freq=freq)
        with_offset = phase_cycles(pr, carrier_msb=1, carrier_lsb=1000, freq=freq)

        offset_cycles = (1 * 65536 + 1000) * 0.001
        assert abs((with_offset - base) - offset_cycles) < 1e-6

    def test_negative_carrier_msb(self):
        """Negative CarrierMSB produces a negative offset."""
        pr = 22_000_000.0 * UREG.meter
        freq = 1575.42 * UREG.MHz

        base = phase_cycles(pr, carrier_msb=0, carrier_lsb=0, freq=freq)
        neg = phase_cycles(pr, carrier_msb=-1, carrier_lsb=0, freq=freq)

        expected_offset = -1 * 65536 * 0.001
        assert abs((neg - base) - expected_offset) < 1e-6


# ===================================================================
# _signed_n_bit and decode_offsets_msb — RefGuide-4.14.0, p.263
# OffsetMSB bits 0-2: CodeOffsetMSB (3-bit two's complement, -4..+3)
# OffsetMSB bits 3-7: DopplerOffsetMSB (5-bit two's complement, -16..+15)
# ===================================================================


class TestSignedNBit:
    """Two's complement reinterpretation helper."""

    @pytest.mark.parametrize(
        "value,bits,expected",
        [
            (0, 3, 0),
            (3, 3, 3),  # max positive 3-bit
            (4, 3, -4),  # min negative 3-bit (100₂ = -4)
            (7, 3, -1),  # 111₂ = -1
            (5, 3, -3),  # 101₂ = -3
            (0, 5, 0),
            (15, 5, 15),  # max positive 5-bit
            (16, 5, -16),  # min negative 5-bit
            (31, 5, -1),
        ],
    )
    def test_signed_n_bit(self, value, bits, expected):
        assert _signed_n_bit(value, bits) == expected


class TestDecodeOffsetsMsb:
    """OffsetMSB byte → (CodeOffsetMSB, DopplerOffsetMSB)."""

    def test_zero_byte(self):
        code, doppler = decode_offsets_msb(0)
        assert code == 0
        assert doppler == 0

    def test_max_positive_code_only(self):
        """Bits 0-2 = 011 = 3 (max positive), bits 3-7 = 0."""
        code, doppler = decode_offsets_msb(0b00000_011)
        assert code == 3
        assert doppler == 0

    def test_min_negative_code_only(self):
        """Bits 0-2 = 100 = -4 (min negative 3-bit), bits 3-7 = 0."""
        code, doppler = decode_offsets_msb(0b00000_100)
        assert code == -4
        assert doppler == 0

    def test_max_positive_doppler_only(self):
        """Bits 0-2 = 0, bits 3-7 = 01111 = 15."""
        code, doppler = decode_offsets_msb(0b01111_000)
        assert code == 0
        assert doppler == 15

    def test_min_negative_doppler_only(self):
        """Bits 0-2 = 0, bits 3-7 = 10000 = -16."""
        code, doppler = decode_offsets_msb(0b10000_000)
        assert code == 0
        assert doppler == -16

    def test_both_extreme_negative(self):
        """Code = -4 (100₂), Doppler = -16 (10000₂)."""
        # byte = 10000_100 = 0x84
        code, doppler = decode_offsets_msb(0b10000_100)
        assert code == -4
        assert doppler == -16

    def test_both_max_positive(self):
        """Code = +3 (011₂), Doppler = +15 (01111₂)."""
        code, doppler = decode_offsets_msb(0b01111_011)
        assert code == 3
        assert doppler == 15

    def test_dnu_code_pattern(self):
        """CodeOffsetMSB==-4 is the DNU marker for Type2 pseudorange."""
        code, _ = decode_offsets_msb(0b00000_100)
        assert code == -4

    def test_dnu_doppler_pattern(self):
        """DopplerOffsetMSB==-16 is the DNU marker for Type2 Doppler."""
        _, doppler = decode_offsets_msb(0b10000_000)
        assert doppler == -16


# ===================================================================
# pr2_m — RefGuide-4.14.0, p.263
# PR_type2 = PR_type1 + (CodeOffsetMSB * 65536 + CodeOffsetLSB) * 0.001
# DNU: CodeOffsetMSB == -4 AND CodeOffsetLSB == 0
# ===================================================================


class TestPr2M:
    """Type2 pseudorange from Type1 base + offset."""

    def test_dnu_returns_none(self):
        pr1 = 22_000_000.0 * UREG.meter
        assert pr2_m(pr1, code_offset_msb=-4, code_offset_lsb=0) is None

    def test_not_dnu_when_lsb_nonzero(self):
        pr1 = 22_000_000.0 * UREG.meter
        result = pr2_m(pr1, code_offset_msb=-4, code_offset_lsb=1)
        assert result is not None

    def test_zero_offset(self):
        """Zero offset means Type2 == Type1."""
        pr1 = 22_000_000.0 * UREG.meter
        result = pr2_m(pr1, code_offset_msb=0, code_offset_lsb=0)
        assert abs(result.magnitude - 22_000_000.0) < 1e-6

    def test_positive_offset(self):
        """CodeOffsetMSB=1, CodeOffsetLSB=0 → offset = 65536 * 0.001 = 65.536 m."""
        pr1 = 22_000_000.0 * UREG.meter
        result = pr2_m(pr1, code_offset_msb=1, code_offset_lsb=0)
        expected = 22_000_000.0 + 65.536
        assert abs(result.magnitude - expected) < 1e-3

    def test_negative_offset(self):
        """CodeOffsetMSB=-1, CodeOffsetLSB=0 → offset = -65.536 m."""
        pr1 = 22_000_000.0 * UREG.meter
        result = pr2_m(pr1, code_offset_msb=-1, code_offset_lsb=0)
        expected = 22_000_000.0 - 65.536
        assert abs(result.magnitude - expected) < 1e-3


# ===================================================================
# doppler2_hz — RefGuide-4.14.0, p.263
# D_type2 = D_type1 * (freq_type2 / freq_type1)
#          + (DopplerOffsetMSB * 65536 + DopplerOffsetLSB) * 1e-4
# DNU: DopplerOffsetMSB == -16 AND DopplerOffsetLSB == 0
# ===================================================================


class TestDoppler2Hz:
    """Type2 Doppler from Type1 base and differential offset."""

    def test_dnu_returns_none(self):
        d1 = 1000.0 * UREG.Hz
        f1 = 1575.42 * UREG.MHz
        f2 = 1227.60 * UREG.MHz
        assert doppler2_hz(d1, -16, 0, f2, f1) is None

    def test_not_dnu_when_lsb_nonzero(self):
        d1 = 1000.0 * UREG.Hz
        f1 = 1575.42 * UREG.MHz
        f2 = 1227.60 * UREG.MHz
        result = doppler2_hz(d1, -16, 1, f2, f1)
        assert result is not None

    def test_same_frequency_zero_offset(self):
        """Same freq, zero offset → D_type2 == D_type1."""
        d1 = 1500.0 * UREG.Hz
        f = 1575.42 * UREG.MHz
        result = doppler2_hz(d1, 0, 0, f, f)
        assert abs(result.magnitude - 1500.0) < 1e-6

    def test_frequency_ratio_scaling(self):
        """L2/L1 frequency ratio should scale the Doppler.

        alpha = 1227.60 / 1575.42 ≈ 0.7792
        D_type2 = 1000 * 0.7792 + 0 = 779.2 Hz (approx)
        """
        d1 = 1000.0 * UREG.Hz
        f1 = 1575.42 * UREG.MHz
        f2 = 1227.60 * UREG.MHz
        result = doppler2_hz(d1, 0, 0, f2, f1)
        expected = 1000.0 * (1227.60 / 1575.42)
        assert abs(result.magnitude - expected) < 0.01


# ===================================================================
# glonass_freq_hz — RefGuide-4.14.0, Table 4.1.10, p.256
# G1 [MHz] = 1602.000 + (FreqNr - 8) * 9/16
# G2 [MHz] = 1246.000 + (FreqNr - 8) * 7/16
# FreqNr = GLONASS slot + 8; valid range 1..21 (slot -7..+13)
# ===================================================================


class TestGlonassFreqHz:
    """GLONASS FDMA carrier frequency computation."""

    def test_g1_slot_zero(self):
        """Slot 0 (FreqNr=8): G1 = 1602.000 + 0 = 1602.000 MHz."""
        result = glonass_freq_hz(signal_num=8, freq_nr=8)
        assert abs(result.magnitude - 1602.000) < 1e-6

    def test_g1_slot_minus7(self):
        """Slot -7 (FreqNr=1): G1 = 1602.000 + (-7) * 9/16 = 1598.0625 MHz."""
        result = glonass_freq_hz(signal_num=8, freq_nr=1)
        expected = 1602.000 + (-7) * (9 / 16)
        assert abs(result.magnitude - expected) < 1e-6

    def test_g1_slot_plus13(self):
        """Slot +13 (FreqNr=21): G1 = 1602.000 + 13 * 9/16 = 1609.3125 MHz."""
        result = glonass_freq_hz(signal_num=8, freq_nr=21)
        expected = 1602.000 + 13 * (9 / 16)
        assert abs(result.magnitude - expected) < 1e-6

    def test_g2_slot_zero(self):
        """Slot 0 (FreqNr=8): G2 = 1246.000 + 0 = 1246.000 MHz."""
        result = glonass_freq_hz(signal_num=10, freq_nr=8)
        assert abs(result.magnitude - 1246.000) < 1e-6

    def test_g2_slot_minus7(self):
        """Slot -7 (FreqNr=1): G2 = 1246.000 + (-7) * 7/16 = 1242.9375 MHz."""
        result = glonass_freq_hz(signal_num=10, freq_nr=1)
        expected = 1246.000 + (-7) * (7 / 16)
        assert abs(result.magnitude - expected) < 1e-6

    def test_g2_slot_plus13(self):
        """Slot +13 (FreqNr=21): G2 = 1246.000 + 13 * 7/16 = 1251.6875 MHz."""
        result = glonass_freq_hz(signal_num=10, freq_nr=21)
        expected = 1246.000 + 13 * (7 / 16)
        assert abs(result.magnitude - expected) < 1e-6

    @pytest.mark.parametrize("sig_num", [8, 9])
    def test_g1_band_signals(self, sig_num):
        """Signal numbers 8 (L1CA) and 9 (L1P) both use G1 band formula."""
        result = glonass_freq_hz(sig_num, freq_nr=8)
        assert abs(result.magnitude - 1602.000) < 1e-6

    @pytest.mark.parametrize("sig_num", [10, 11])
    def test_g2_band_signals(self, sig_num):
        """Signal numbers 10 (L2P) and 11 (L2CA) both use G2 band formula."""
        result = glonass_freq_hz(sig_num, freq_nr=8)
        assert abs(result.magnitude - 1246.000) < 1e-6

    def test_non_fdma_raises(self):
        """Non-FDMA signal numbers (0-7, 12+) must raise ValueError."""
        with pytest.raises(ValueError, match="not a GLONASS FDMA signal"):
            glonass_freq_hz(signal_num=0, freq_nr=8)

    def test_non_fdma_glonass_l3_raises(self):
        """GLONASS L3 CDMA (signal 12) is not FDMA — must raise."""
        with pytest.raises(ValueError, match="not a GLONASS FDMA signal"):
            glonass_freq_hz(signal_num=12, freq_nr=8)

    def test_returns_mhz_unit(self):
        result = glonass_freq_hz(8, freq_nr=8)
        assert str(result.units) == "megahertz"
