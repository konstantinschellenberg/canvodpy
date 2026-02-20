"""SBF file reader.

Wraps the ``sbf-parser`` library and converts raw SBF fields to physical
units using :mod:`_scaling`.  Physical quantities are expressed as
:class:`pint.Quantity` objects via the shared
:data:`~canvod.readers.gnss_specs.constants.UREG` registry.

GLONASS FDMA frequencies are resolved via a live ``FreqNr`` cache updated
from ChannelStatus blocks as they appear in the stream.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from functools import cached_property
from pathlib import Path
from typing import Any

import pint
import structlog
from pydantic import BaseModel, ConfigDict, field_validator

from canvod.readers.gnss_specs.constants import UREG
from canvod.readers.sbf._registry import FDMA_SIGNAL_NUMS, SIGNAL_TABLE, decode_svid
from canvod.readers.sbf._scaling import (
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
from canvod.readers.sbf.models import SbfEpoch, SbfHeader, SbfSignalObs

try:
    import sbf_parser
except ImportError as _err:
    raise ImportError(
        "sbf-parser is required for SbfReader. "
        "Install it with: uv add sbf-parser"
    ) from _err

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# GPS ↔ UTC time conversion
# Source: IS-GPS-200, §20.3.3.5.2.4
# GPS epoch: 1980-01-06 00:00:00 UTC (no leap seconds at that date)
# ---------------------------------------------------------------------------

_GPS_EPOCH = datetime(1980, 1, 6, tzinfo=timezone.utc)
_SECONDS_PER_GPS_WEEK: int = 604_800

# Leap second offset GPS - UTC.  Valid from 2017-01-01; next scheduled: TBD.
# Updated dynamically when a ReceiverTime block is available in the stream.
_DEFAULT_DELTA_LS: int = 18


def _tow_wn_to_utc(tow_ms: int, wn: int, delta_ls: int) -> datetime:
    """Convert GPS TOW + WN to a UTC datetime.

    Parameters
    ----------
    tow_ms : int
        GPS Time of Week in milliseconds.
    wn : int
        GPS Week Number (continuous, post-rollover correction applied by
        the receiver).
    delta_ls : int
        Leap second count: GPS - UTC (seconds).

    Returns
    -------
    datetime
        Timezone-aware UTC timestamp.

    Notes
    -----
    Source: IS-GPS-200, §20.3.3.5.2.4.
    """
    gps_seconds = wn * _SECONDS_PER_GPS_WEEK + tow_ms / 1000.0
    utc_seconds = gps_seconds - delta_ls
    return _GPS_EPOCH + timedelta(seconds=utc_seconds)


# ---------------------------------------------------------------------------
# String helpers for ReceiverSetup binary character arrays
# ---------------------------------------------------------------------------


def _decode_bytes(raw: bytes) -> str:
    """Decode a NUL-padded SBF character array to a clean Python string."""
    return raw.decode("ascii", errors="replace").rstrip("\x00").strip()


# ---------------------------------------------------------------------------
# SbfReader
# ---------------------------------------------------------------------------


class SbfReader(BaseModel):
    """Read and decode a Septentrio Binary Format (SBF) observation file.

    Parameters
    ----------
    fpath : Path
        Path to the ``*.sbf`` (or ``*.SBF``, or receiver-named) binary file.

    Examples
    --------
    >>> reader = SbfReader(fpath=Path("rref213a00.25_"))
    >>> print(reader.header.rx_version)
    4.14.4
    >>> for epoch in reader.iter_epochs():
    ...     for obs in epoch.observations:
    ...         print(obs.system, obs.prn, obs.cn0)

    Notes
    -----
    - All physical-unit conversions follow RefGuide-4.14.0.
    - Physical quantities are expressed as :class:`pint.Quantity` objects
      using the shared :data:`~canvod.readers.gnss_specs.constants.UREG`.
    - GLONASS FDMA frequencies are resolved from the most recently seen
      ChannelStatus block; observations before the first ChannelStatus for a
      given SVID have ``phase_cycles=None``.
    - The file is scanned once per :meth:`iter_epochs` call; use
      :attr:`num_epochs` for a pre-computed count (scans once on first access).
    """

    model_config = ConfigDict(frozen=False, arbitrary_types_allowed=True)

    fpath: Path

    @field_validator("fpath")
    @classmethod
    def validate_fpath(cls, v: Path) -> Path:
        """Validate that the file exists and is readable.

        Parameters
        ----------
        v : Path
            Path to validate.

        Returns
        -------
        Path
            Validated path.

        Raises
        ------
        FileNotFoundError
            If the file does not exist.
        """
        if not v.is_file():
            raise FileNotFoundError(f"SBF file not found: {v}")
        return v

    @cached_property
    def file_hash(self) -> str:
        """SHA-256 hex digest of the file (first 16 characters).

        Returns
        -------
        str
            16-character hexadecimal prefix of the SHA-256 hash.
        """
        h = hashlib.sha256(self.fpath.read_bytes())
        return h.hexdigest()[:16]

    @cached_property
    def header(self) -> SbfHeader:
        """Parse the first ReceiverSetup block in the file.

        Returns
        -------
        SbfHeader
            Receiver metadata.

        Raises
        ------
        LookupError
            If no ReceiverSetup block is found.
        """
        parser = sbf_parser.SbfParser()
        for name, data in parser.read(str(self.fpath)):
            if name == "ReceiverSetup":
                return SbfHeader(
                    marker_name=_decode_bytes(data["MarkerName"]),
                    marker_number=_decode_bytes(data["MarkerNumber"]),
                    observer=_decode_bytes(data["Observer"]),
                    agency=_decode_bytes(data["Agency"]),
                    rx_serial=_decode_bytes(data["RxSerialNumber"]),
                    rx_name=_decode_bytes(data["RxName"]),
                    rx_version=_decode_bytes(data["RxVersion"]),
                    ant_serial=_decode_bytes(data["AntSerialNbr"]),
                    ant_type=_decode_bytes(data["AntType"]),
                    delta_h=float(data["deltaH"]) * UREG.meter,
                    delta_e=float(data["deltaE"]) * UREG.meter,
                    delta_n=float(data["deltaN"]) * UREG.meter,
                    latitude_rad=float(data["Latitude"]),
                    longitude_rad=float(data["Longitude"]),
                    height_m=float(data["Height"]) * UREG.meter,
                    gnss_fw_version=_decode_bytes(data["GNSSFWVersion"]),
                    product_name=_decode_bytes(data["ProductName"]),
                )
        raise LookupError(f"No ReceiverSetup block found in {self.fpath}")

    @cached_property
    def num_epochs(self) -> int:
        """Count the number of MeasEpoch blocks in the file.

        Returns
        -------
        int
            Total MeasEpoch block count (one per observation epoch).

        Notes
        -----
        Scans the entire file once; result is cached.
        """
        parser = sbf_parser.SbfParser()
        count = sum(
            1 for name, _ in parser.read(str(self.fpath)) if name == "MeasEpoch"
        )
        log.debug("sbf_epoch_count", fpath=str(self.fpath), num_epochs=count)
        return count

    def iter_epochs(self) -> Iterator[SbfEpoch]:
        """Iterate over decoded MeasEpoch blocks.

        Yields decoded :class:`SbfEpoch` objects with all signal observations
        converted to physical units as :class:`pint.Quantity`.

        Yields
        ------
        SbfEpoch
            One decoded observation epoch.

        Notes
        -----
        - The file is scanned from start to finish on each call.
        - A ``FreqNr`` cache (SVID → FreqNr) is built from ChannelStatus
          blocks encountered *before* each MeasEpoch.  Epochs that precede
          the first ChannelStatus will have ``phase_cycles=None`` for all
          GLONASS FDMA signals.
        - ``delta_ls`` (leap seconds) is taken from the most recent
          ReceiverTime block; defaults to 18 if none has been seen yet.
        """
        parser = sbf_parser.SbfParser()
        freq_nr_cache: dict[int, int] = {}  # SVID → FreqNr
        delta_ls: int = _DEFAULT_DELTA_LS

        for name, data in parser.read(str(self.fpath)):
            match name:
                case "ReceiverTime":
                    delta_ls = int(data["DeltaLS"])

                case "ChannelStatus":
                    for sat in data.get("ChannelSatInfo", []):
                        svid = int(sat["SVID"])
                        if svid != 0:
                            freq_nr_cache[svid] = int(sat["FreqNr"])

                case "MeasEpoch":
                    epoch = self._decode_epoch(data, freq_nr_cache, delta_ls)
                    if epoch is not None:
                        yield epoch

    # ------------------------------------------------------------------
    # Private decoding helpers
    # ------------------------------------------------------------------

    def _decode_epoch(  # pylint: disable=too-many-locals
        self,
        data: dict[str, Any],
        freq_nr_cache: dict[int, int],
        delta_ls: int,
    ) -> SbfEpoch | None:
        """Decode one raw MeasEpoch dict into an :class:`SbfEpoch`.

        Parameters
        ----------
        data : dict
            Raw block dict from ``sbf_parser``.
        freq_nr_cache : dict of {int: int}
            Current SVID → FreqNr mapping for GLONASS FDMA frequency lookup.
        delta_ls : int
            GPS - UTC leap second offset.

        Returns
        -------
        SbfEpoch or None
            Decoded epoch, or ``None`` if decoding fails (logged as warning).
        """
        tow_ms = int(data["TOW"])
        wn = int(data["WNc"])
        timestamp = _tow_wn_to_utc(tow_ms, wn, delta_ls)
        common_flags = int(data["CommonFlags"])
        cum_clk_jumps = int(data["CumClkJumps"])

        observations: list[SbfSignalObs] = []

        for t1 in data.get("Type_1", []):
            t1_obs, t1_freq = self._decode_type1(t1, freq_nr_cache)
            if t1_obs is not None:
                observations.append(t1_obs)
                # Decode linked Type2 slave observations
                pr1 = t1_obs.pseudorange
                d1 = t1_obs.doppler
                if pr1 is not None and d1 is not None and t1_freq is not None:
                    for t2 in t1.get("Type_2", []):
                        t2_obs = self._decode_type2(
                            t2, int(t1["SVID"]), pr1, d1, t1_freq, freq_nr_cache
                        )
                        if t2_obs is not None:
                            observations.append(t2_obs)

        return SbfEpoch(
            tow_ms=tow_ms,
            wn=wn,
            timestamp=timestamp,
            common_flags=common_flags,
            cum_clk_jumps=cum_clk_jumps,
            observations=tuple(observations),
        )

    def _resolve_freq(
        self,
        sig_num: int,
        svid: int,
        freq_nr_cache: dict[int, int],
    ) -> pint.Quantity | None:
        """Return carrier frequency as a pint Quantity, or None if unavailable.

        Parameters
        ----------
        sig_num : int
            Signal type number (0-39).
        svid : int
            Septentrio internal SVID.
        freq_nr_cache : dict of {int: int}
            Current SVID → FreqNr map.

        Returns
        -------
        pint.Quantity or None
            Carrier frequency (in MHz), or ``None`` if GLONASS and FreqNr
            not yet known, or signal not in table (e.g. L-Band MSS).
        """
        if sig_num in FDMA_SIGNAL_NUMS:
            freq_nr = freq_nr_cache.get(svid)
            if freq_nr is None:
                return None
            return glonass_freq_hz(sig_num, freq_nr)

        sig_def = SIGNAL_TABLE.get(sig_num)
        if sig_def is None:
            return None
        return sig_def.freq  # None for L-Band MSS (sig 23)

    def _decode_type1(  # pylint: disable=too-many-locals
        self,
        t1: dict[str, Any],
        freq_nr_cache: dict[int, int],
    ) -> tuple[SbfSignalObs | None, pint.Quantity | None]:
        """Decode a Type1 sub-block dict to an SbfSignalObs.

        Parameters
        ----------
        t1 : dict
            Raw Type1 sub-block dict.
        freq_nr_cache : dict of {int: int}
            Current SVID → FreqNr map.

        Returns
        -------
        obs : SbfSignalObs or None
            Decoded observation, or ``None`` for unknown signals.
        freq : pint.Quantity or None
            Carrier frequency used (needed for Type2 Doppler scaling).
        """
        svid = int(t1["SVID"])
        type_byte = int(t1["Type"])
        obs_info = int(t1["ObsInfo"])
        sig_num = decode_signal_num(type_byte, obs_info)

        sig_def = SIGNAL_TABLE.get(sig_num)
        if sig_def is None:
            log.debug("sbf_unknown_signal", svid=svid, sig_num=sig_num)
            return None, None

        system, prn = decode_svid(svid)
        freq = self._resolve_freq(sig_num, svid, freq_nr_cache)

        misc = int(t1["Misc"])
        code_lsb = int(t1["CodeLSB"])
        pr = pseudorange_m(misc, code_lsb)
        dop = doppler_hz(int(t1["Doppler"]))
        carrier_msb = int(t1["CarrierMSB"])
        carrier_lsb = int(t1["CarrierLSB"])

        ph: float | None = None
        if pr is not None and freq is not None:
            ph = phase_cycles(pr, carrier_msb, carrier_lsb, freq)

        obs = SbfSignalObs(
            svid=svid,
            system=system,
            prn=prn,
            signal_num=sig_num,
            signal_type=sig_def.signal_type,
            rx_channel=int(t1["RxChannel"]),
            lock_time_ms=int(t1["LockTime"]),
            cn0=cn0_dbhz(int(t1["CN0"]), sig_num),
            pseudorange=pr,
            doppler=dop,
            phase_cycles=ph,
            obs_info=obs_info,
            is_type2=False,
        )
        return obs, freq

    def _decode_type2(  # pylint: disable=too-many-arguments,too-many-locals,too-many-positional-arguments
        self,
        t2: dict[str, Any],
        svid: int,
        pr1: pint.Quantity,
        d1: pint.Quantity,
        freq1: pint.Quantity,
        freq_nr_cache: dict[int, int],
    ) -> SbfSignalObs | None:
        """Decode a Type2 sub-block dict to an SbfSignalObs.

        Parameters
        ----------
        t2 : dict
            Raw Type2 sub-block dict.
        svid : int
            SVID of the parent Type1 sub-block.
        pr1 : pint.Quantity
            Type1 pseudorange in metres.
        d1 : pint.Quantity
            Type1 Doppler in Hz.
        freq1 : pint.Quantity
            Type1 carrier frequency.
        freq_nr_cache : dict of {int: int}
            Current SVID → FreqNr map.

        Returns
        -------
        SbfSignalObs or None
            Decoded observation, or ``None`` for unknown signals.
        """
        type_byte = int(t2["Type"])
        obs_info = int(t2["ObsInfo"])
        sig_num = decode_signal_num(type_byte, obs_info)

        sig_def = SIGNAL_TABLE.get(sig_num)
        if sig_def is None:
            log.debug("sbf_unknown_type2_signal", svid=svid, sig_num=sig_num)
            return None

        system, prn = decode_svid(svid)
        freq2 = self._resolve_freq(sig_num, svid, freq_nr_cache)

        code_msb_signed, doppler_msb_signed = decode_offsets_msb(int(t2["OffsetMSB"]))
        code_offset_lsb = int(t2["CodeOffsetLSB"])
        doppler_offset_lsb = int(t2["DopplerOffsetLSB"])
        carrier_msb = int(t2["CarrierMSB"])
        carrier_lsb = int(t2["CarrierLSB"])

        pr2 = pr2_m(pr1, code_msb_signed, code_offset_lsb)

        d2: pint.Quantity | None = None
        if freq2 is not None:
            d2 = doppler2_hz(d1, doppler_msb_signed, doppler_offset_lsb, freq2, freq1)

        ph: float | None = None
        if pr2 is not None and freq2 is not None:
            ph = phase_cycles(pr2, carrier_msb, carrier_lsb, freq2)

        return SbfSignalObs(
            svid=svid,
            system=system,
            prn=prn,
            signal_num=sig_num,
            signal_type=sig_def.signal_type,
            rx_channel=int(t2.get("RxChannel", 0)),
            lock_time_ms=int(t2["LockTime"]),
            cn0=cn0_dbhz(int(t2["CN0"]), sig_num),
            pseudorange=pr2,
            doppler=d2,
            phase_cycles=ph,
            obs_info=obs_info,
            is_type2=True,
        )

    def __repr__(self) -> str:
        """Return a short string representation."""
        return f"SbfReader(file='{self.fpath.name}', epochs={self.num_epochs})"
