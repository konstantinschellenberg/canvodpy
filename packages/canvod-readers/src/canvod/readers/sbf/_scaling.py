"""Physical-unit scaling functions for SBF measurement fields.

All formulas are taken verbatim from the official reference guide.
Do-Not-Use (DNU) conditions return ``None`` rather than sentinel floats.

Physical constants are imported from
:mod:`canvod.readers.gnss_specs.constants` to avoid duplication
(``SPEEDOFLIGHT``, ``UREG``).

Primary source
--------------
AsteRx SB3 ProBase Firmware v4.14.0 Reference Guide (Septentrio).
Abbreviated below as **RefGuide-4.14.0**.
"""

from __future__ import annotations

import pint

from canvod.readers.gnss_specs.constants import SPEEDOFLIGHT, UREG

# Speed of light as a plain float [m/s] for fast per-observation arithmetic.
# Derived from the shared SPEEDOFLIGHT constant; not hardcoded independently.
_C_M_S: float = float(SPEEDOFLIGHT.to(UREG.meter / UREG.second).magnitude)

# GPS reference epoch: 1980-01-06 00:00:00 UTC
# Source: IS-GPS-200, §20.3.3.5.2.4
_SECONDS_PER_GPS_WEEK: int = 604_800


# ---------------------------------------------------------------------------
# Signal number extraction from Type + ObsInfo bytes (extended signal table)
# Source: RefGuide-4.14.0, MeasEpochChannelType1.Type, p.261
#   bits 0-4 of Type = SigIdxLo
#   If SigIdxLo == 31: actual signal num = (ObsInfo bits 3-7) + 32
# ---------------------------------------------------------------------------


def decode_signal_num(type_byte: int, obs_info: int) -> int:
    """Extract the extended signal type number from a Type/ObsInfo byte pair.

    Parameters
    ----------
    type_byte : int
        Raw ``u1`` Type field from a Type1 or Type2 sub-block.
    obs_info : int
        Raw ``u1`` ObsInfo field from the same sub-block.

    Returns
    -------
    int
        Signal type number in the range 0-39.

    Notes
    -----
    Source: RefGuide-4.14.0, MeasEpochChannelType1.Type, p.261.
    Bits 5-7 of Type encode the antenna descriptor; they are ignored here.
    """
    sig_idx_lo = type_byte & 0x1F  # bits 0-4
    match sig_idx_lo:
        case 31:
            # Extended signal number encoded in ObsInfo bits 3-7
            return ((obs_info >> 3) & 0x1F) + 32
        case _:
            return sig_idx_lo


# ---------------------------------------------------------------------------
# CN0 scaling
# Source: RefGuide-4.14.0, MeasEpochChannelType1.CN0, p.261
#   C/N0 [dB-Hz] = raw * 0.25 + 10  for all signals except 1 (L1P) and 2 (L2P)
#   C/N0 [dB-Hz] = raw * 0.25       for signals 1 and 2 (no +10 dB offset)
# Do-Not-Use: raw == 0
# ---------------------------------------------------------------------------


def cn0_dbhz(raw: int, sig_num: int) -> pint.Quantity | None:
    """Scale a raw CN0 byte to C/N0 in dB-Hz.

    Parameters
    ----------
    raw : int
        Raw ``u1`` CN0 field from a Type1 or Type2 sub-block.
    sig_num : int
        Signal type number (from :func:`decode_signal_num`).

    Returns
    -------
    pint.Quantity or None
        C/N0 as a ``Quantity`` in ``dBHz``, or ``None`` if Do-Not-Use
        (``raw == 0``).

    Notes
    -----
    Source: RefGuide-4.14.0, MeasEpochChannelType1.CN0, p.261.
    Only GPS L1P (sig 1) and GPS L2P (sig 2) omit the +10 dB offset.
    """
    if raw == 0:
        return None
    match sig_num:
        case 1 | 2:
            value = raw * 0.25
        case _:
            value = raw * 0.25 + 10.0
    return value * UREG.dBHz


# ---------------------------------------------------------------------------
# Type1 pseudorange
# Source: RefGuide-4.14.0, MeasEpochChannelType1, p.261
#   CodeMSB = Misc & 0x0F  (bits 0-3)
#   PR [m]  = (CodeMSB * 4_294_967_296 + CodeLSB) * 0.001
# Do-Not-Use: CodeMSB == 0 AND CodeLSB == 0
# ---------------------------------------------------------------------------


def pseudorange_m(misc: int, code_lsb: int) -> pint.Quantity | None:
    """Scale Type1 code fields to pseudorange.

    Parameters
    ----------
    misc : int
        Raw ``u1`` Misc byte; bits 0-3 carry CodeMSB.
    code_lsb : int
        Raw ``u4`` CodeLSB field.

    Returns
    -------
    pint.Quantity or None
        Pseudorange as a ``Quantity`` in metres, or ``None`` if Do-Not-Use.

    Notes
    -----
    Source: RefGuide-4.14.0, MeasEpochChannelType1, p.261.
    """
    code_msb = misc & 0x0F
    if code_msb == 0 and code_lsb == 0:
        return None
    return (code_msb * 4_294_967_296 + code_lsb) * 1e-3 * UREG.meter


# ---------------------------------------------------------------------------
# Type1 Doppler
# Source: RefGuide-4.14.0, MeasEpochChannelType1.Doppler, p.261
#   D [Hz] = raw * 0.0001
# Do-Not-Use: raw == -2_147_483_648  (i4 minimum)
# ---------------------------------------------------------------------------

_DOPPLER_DNU: int = -(1 << 31)  # -2_147_483_648


def doppler_hz(raw: int) -> pint.Quantity | None:
    """Scale a Type1 Doppler raw ``i4`` to Hz.

    Parameters
    ----------
    raw : int
        Raw ``i4`` Doppler field.

    Returns
    -------
    pint.Quantity or None
        Doppler shift as a ``Quantity`` in Hz (positive = approaching),
        or ``None`` if Do-Not-Use.

    Notes
    -----
    Source: RefGuide-4.14.0, MeasEpochChannelType1.Doppler, p.261.
    """
    if raw == _DOPPLER_DNU:
        return None
    return raw * 1e-4 * UREG.Hz


# ---------------------------------------------------------------------------
# Type1 carrier phase
# Source: RefGuide-4.14.0, MeasEpochChannelType1, p.261
#   λ          = c / freq_hz
#   L [cycles] = PR [m] / λ  +  (CarrierMSB * 65536 + CarrierLSB) * 0.001
# Do-Not-Use: CarrierMSB == -128 AND CarrierLSB == 0
# ---------------------------------------------------------------------------


def phase_cycles(
    pr_m: pint.Quantity,
    carrier_msb: int,
    carrier_lsb: int,
    freq: pint.Quantity,
) -> float | None:
    """Scale Type1 carrier fields to carrier phase in cycles.

    Parameters
    ----------
    pr_m : pint.Quantity
        Pseudorange (from :func:`pseudorange_m`); must be in metres.
    carrier_msb : int
        Raw ``i1`` CarrierMSB field.
    carrier_lsb : int
        Raw ``u2`` CarrierLSB field.
    freq : pint.Quantity
        Carrier frequency (e.g. from :data:`~canvod.readers.sbf._registry.SignalDef.freq`
        or :func:`glonass_freq_hz`); any frequency unit accepted.

    Returns
    -------
    float or None
        Carrier phase in cycles (dimensionless), or ``None`` if Do-Not-Use.

    Notes
    -----
    Source: RefGuide-4.14.0, MeasEpochChannelType1, p.261.
    Uses :data:`~canvod.readers.gnss_specs.constants.SPEEDOFLIGHT`.
    """
    if carrier_msb == -128 and carrier_lsb == 0:
        return None
    freq_hz = float(freq.to(UREG.Hz).magnitude)
    lambda_m = _C_M_S / freq_hz
    pr_val = float(pr_m.to(UREG.meter).magnitude)
    return pr_val / lambda_m + (carrier_msb * 65_536 + carrier_lsb) * 1e-3


# ---------------------------------------------------------------------------
# OffsetMSB decoding for Type2 sub-blocks
# Source: RefGuide-4.14.0, MeasEpochChannelType2.OffsetMSB, p.263
#   bits 0-2: CodeOffsetMSB    (3-bit two's-complement, range -4 to +3)
#   bits 3-7: DopplerOffsetMSB (5-bit two's-complement, range -16 to +15)
# ---------------------------------------------------------------------------


def _signed_n_bit(value: int, bits: int) -> int:
    """Reinterpret an unsigned integer as a signed n-bit two's-complement."""
    sign_bit = 1 << (bits - 1)
    return (value & (sign_bit - 1)) - (value & sign_bit)


def decode_offsets_msb(offset_msb: int) -> tuple[int, int]:
    """Decode the Type2 OffsetMSB byte into (CodeOffsetMSB, DopplerOffsetMSB).

    Parameters
    ----------
    offset_msb : int
        Raw ``u1`` OffsetMSB byte.

    Returns
    -------
    code_offset_msb : int
        3-bit signed value in range [-4, +3].
    doppler_offset_msb : int
        5-bit signed value in range [-16, +15].

    Notes
    -----
    Source: RefGuide-4.14.0, MeasEpochChannelType2.OffsetMSB, p.263.
    """
    code_raw = offset_msb & 0x07             # bits 0-2
    doppler_raw = (offset_msb >> 3) & 0x1F   # bits 3-7
    return _signed_n_bit(code_raw, 3), _signed_n_bit(doppler_raw, 5)


# ---------------------------------------------------------------------------
# Type2 pseudorange
# Source: RefGuide-4.14.0, MeasEpochChannelType2, p.263
#   PR_type2 [m] = PR_type1 [m]  +  (CodeOffsetMSB * 65536 + CodeOffsetLSB) * 0.001
# Do-Not-Use: CodeOffsetMSB == -4 AND CodeOffsetLSB == 0
# ---------------------------------------------------------------------------


def pr2_m(
    pr1: pint.Quantity,
    code_offset_msb: int,
    code_offset_lsb: int,
) -> pint.Quantity | None:
    """Compute Type2 pseudorange from Type1 base and offset fields.

    Parameters
    ----------
    pr1 : pint.Quantity
        Type1 pseudorange (from :func:`pseudorange_m`); in metres.
    code_offset_msb : int
        3-bit signed CodeOffsetMSB (from :func:`decode_offsets_msb`).
    code_offset_lsb : int
        Raw ``u2`` CodeOffsetLSB field.

    Returns
    -------
    pint.Quantity or None
        Type2 pseudorange in metres, or ``None`` if Do-Not-Use.

    Notes
    -----
    Source: RefGuide-4.14.0, MeasEpochChannelType2, p.263.
    """
    if code_offset_msb == -4 and code_offset_lsb == 0:
        return None
    offset_m = (code_offset_msb * 65_536 + code_offset_lsb) * 1e-3 * UREG.meter
    return pr1 + offset_m


# ---------------------------------------------------------------------------
# Type2 Doppler
# Source: RefGuide-4.14.0, MeasEpochChannelType2, p.263
#   D_type2 [Hz] = D_type1 * (freq_type2 / freq_type1)
#                + (DopplerOffsetMSB * 65536 + DopplerOffsetLSB) * 1e-4
# Do-Not-Use: DopplerOffsetMSB == -16 AND DopplerOffsetLSB == 0
# ---------------------------------------------------------------------------


def doppler2_hz(
    d1: pint.Quantity,
    doppler_offset_msb: int,
    doppler_offset_lsb: int,
    freq_type2: pint.Quantity,
    freq_type1: pint.Quantity,
) -> pint.Quantity | None:
    """Compute Type2 Doppler from Type1 base and differential offset.

    Parameters
    ----------
    d1 : pint.Quantity
        Type1 Doppler (from :func:`doppler_hz`); in Hz.
    doppler_offset_msb : int
        5-bit signed DopplerOffsetMSB (from :func:`decode_offsets_msb`).
    doppler_offset_lsb : int
        Raw ``u2`` DopplerOffsetLSB field.
    freq_type2 : pint.Quantity
        Carrier frequency of the Type2 signal.
    freq_type1 : pint.Quantity
        Carrier frequency of the Type1 (master) signal.

    Returns
    -------
    pint.Quantity or None
        Type2 Doppler in Hz, or ``None`` if Do-Not-Use.

    Notes
    -----
    Source: RefGuide-4.14.0, MeasEpochChannelType2, p.263.
    """
    if doppler_offset_msb == -16 and doppler_offset_lsb == 0:
        return None
    alpha = float((freq_type2 / freq_type1).to(UREG.dimensionless).magnitude)
    offset_hz = (doppler_offset_msb * 65_536 + doppler_offset_lsb) * 1e-4 * UREG.Hz
    return d1 * alpha + offset_hz


# ---------------------------------------------------------------------------
# GLONASS FDMA carrier frequencies
# Source: RefGuide-4.14.0, Table 4.1.10, p. 256 (frequency column footnote)
#   G1 [MHz] = 1602.000 + (FreqNr - 8) * 9/16
#   G2 [MHz] = 1246.000 + (FreqNr - 8) * 7/16
# FreqNr: ChannelStatus_ChannelSatInfo.FreqNr
#   FreqNr = GLONASS slot number + 8;  valid range 1..21 (slot -7..+13)
# ---------------------------------------------------------------------------

_G1_BASE: pint.Quantity = 1602.000 * UREG.MHz
_G1_STEP: pint.Quantity = (9 / 16) * UREG.MHz   # per slot
_G2_BASE: pint.Quantity = 1246.000 * UREG.MHz
_G2_STEP: pint.Quantity = (7 / 16) * UREG.MHz   # per slot
_FREQ_NR_OFFSET: int = 8                          # slot = FreqNr - 8


def glonass_freq_hz(signal_num: int, freq_nr: int) -> pint.Quantity:
    """Return the GLONASS FDMA carrier frequency for a given channel.

    Parameters
    ----------
    signal_num : int
        SBF signal type number; must be 8 (L1CA), 9 (L1P), 10 (L2P),
        or 11 (L2CA).
    freq_nr : int
        ``FreqNr`` value from the ChannelStatus block; encodes the GLONASS
        frequency slot as ``slot = freq_nr - 8``.

    Returns
    -------
    pint.Quantity
        Carrier frequency in MHz.

    Raises
    ------
    ValueError
        If ``signal_num`` is not a GLONASS FDMA signal (8-11).

    Notes
    -----
    Source: RefGuide-4.14.0, Table 4.1.10, p. 256 frequency column footnote.
    GLONASS L3 CDMA (signal 12) has a fixed frequency (1202.025 MHz) stored
    directly in :data:`~canvod.readers.sbf._registry.SIGNAL_TABLE`.
    """
    slot = freq_nr - _FREQ_NR_OFFSET
    match signal_num:
        case 8 | 9:    # G1 band: L1CA, L1P
            return _G1_BASE + slot * _G1_STEP
        case 10 | 11:  # G2 band: L2P, L2CA
            return _G2_BASE + slot * _G2_STEP
        case _:
            raise ValueError(
                f"signal_num {signal_num} is not a GLONASS FDMA signal (8-11)"
            )
