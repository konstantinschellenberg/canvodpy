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

import numpy as np
import pint
import structlog
import xarray as xr
from pydantic import BaseModel, ConfigDict, field_validator

from canvod.readers.base import GNSSDataReader
from canvod.readers.gnss_specs.constants import UREG
from canvod.readers.gnss_specs.constellations import (
    BEIDOU,
    GALILEO,
    GLONASS,
    GPS,
    IRNSS,
    QZSS,
    SBAS,
)
from canvod.readers.gnss_specs.metadata import (
    CN0_METADATA,
    COORDS_METADATA,
    DTYPES,
    OBSERVABLES_METADATA,
    get_global_attrs,
)
from canvod.readers.gnss_specs.utils import get_version_from_pyproject
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
# Bandwidth / frequency helpers for to_ds() and to_metadata_ds()
# ---------------------------------------------------------------------------

_CONSTELLATION_MAP: dict[str, Any] = {
    "G": GPS,
    "R": GLONASS,
    "E": GALILEO,
    "C": BEIDOU,
    "J": QZSS,
    "I": IRNSS,
    "S": SBAS,
}


def _get_bandwidth_mhz(system: str, band: str) -> float:
    """Return signal bandwidth in MHz, or NaN if unknown.

    Parameters
    ----------
    system : str
        RINEX single-letter system code.
    band : str
        Band label (e.g. "L1", "G1", "E5a").

    Returns
    -------
    float
        Bandwidth in MHz, or NaN if the band is not found.
    """
    if system == "R" and band in ("G1", "G2"):
        # FDMA G1/G2: use aggregated bandwidth table (ClassVar on GLONASS)
        bw = GLONASS.AGGR_G1_G2_BAND_PROPERTIES[band]["bandwidth"]
        return float(bw.to(UREG.MHz).magnitude)
    const = _CONSTELLATION_MAP.get(system)
    if const is None:
        return float("nan")
    try:
        bw = const.BAND_PROPERTIES[band]["bandwidth"]
        return float(bw.to(UREG.MHz).magnitude)
    except (KeyError, AttributeError):
        return float("nan")


# ---------------------------------------------------------------------------
# Theta / phi provenance attributes for to_metadata_ds()
# ---------------------------------------------------------------------------

_THETA_ATTRS: dict[str, str | float] = {
    "long_name": "Polar angle (zenith angle)",
    "standard_name": "polar_angle",
    "units": "degrees",
    "description": "Angle from zenith (90 deg - elevation). SCS convention.",
    "source": "SBF SatVisibility block — reported by receiver firmware",
    "comment": (
        "Derived from the receiver's internal navigation solution, NOT from "
        "independently-computed satellite ephemerides. Values match the "
        "receiver's tracking geometry and may differ from ephemeris-derived angles."
    ),
    "_FillValue": -9999.0,
}
_PHI_ATTRS: dict[str, str | float] = {
    "long_name": "Azimuthal angle from North",
    "standard_name": "azimuth",
    "units": "degrees",
    "description": "Clockwise azimuth from North. SCS convention.",
    "source": "SBF SatVisibility block — reported by receiver firmware",
    "comment": (
        "Derived from the receiver's internal navigation solution, NOT from "
        "independently-computed satellite ephemerides. Values match the "
        "receiver's tracking geometry and may differ from ephemeris-derived angles."
    ),
    "_FillValue": -9999.0,
}


def _build_obs_map(meas_epoch_data: dict[str, Any]) -> dict[tuple[int, int], int]:
    """Build ``(rx_channel, sig_num) → svid`` mapping from a MeasEpoch dict.

    Parameters
    ----------
    meas_epoch_data : dict
        Raw MeasEpoch block dict from ``sbf_parser``.

    Returns
    -------
    dict
        Mapping ``(rx_channel, signal_num) → svid`` for all Type1 and
        Type2 sub-blocks in the epoch.
    """
    obs_map: dict[tuple[int, int], int] = {}
    for t1 in meas_epoch_data.get("Type_1", []):
        svid = int(t1["SVID"])
        type_byte = int(t1["Type"])
        obs_info = int(t1["ObsInfo"])
        sig_num = decode_signal_num(type_byte, obs_info)
        rx_ch = int(t1["RxChannel"])
        obs_map[(rx_ch, sig_num)] = svid
        for t2 in t1.get("Type_2", []):
            type_byte2 = int(t2["Type"])
            obs_info2 = int(t2["ObsInfo"])
            sig_num2 = decode_signal_num(type_byte2, obs_info2)
            rx_ch2 = int(t2.get("RxChannel", 0))
            obs_map[(rx_ch2, sig_num2)] = svid
    return obs_map


def _sid_props_from_obs(
    svid: int,
    sig_num: int,
    freq_nr_cache: dict[int, int],
) -> dict[str, Any] | None:
    """Compute sid string and properties for one (svid, signal_num) pair.

    Returns None if the signal is not in SIGNAL_TABLE.
    """
    sig_def = SIGNAL_TABLE.get(sig_num)
    if sig_def is None:
        return None
    system, prn = decode_svid(svid)
    sv = f"{system}{prn:02d}"
    sid = f"{sv}|{sig_def.band}|{sig_def.code}"

    band = sig_def.band
    if sig_num in FDMA_SIGNAL_NUMS:
        freq_nr = freq_nr_cache.get(svid)
        if freq_nr is not None:
            freq_qty = glonass_freq_hz(sig_num, freq_nr)
            freq_center_mhz = float(freq_qty.to(UREG.MHz).magnitude)
        else:
            freq_qty = GLONASS.AGGR_G1_G2_BAND_PROPERTIES[band]["freq"]
            freq_center_mhz = float(freq_qty.to(UREG.MHz).magnitude)
    elif sig_def.freq is not None:
        freq_center_mhz = float(sig_def.freq.to(UREG.MHz).magnitude)
    else:
        freq_center_mhz = float("nan")

    bw_mhz = _get_bandwidth_mhz(system, band)
    if np.isnan(freq_center_mhz) or np.isnan(bw_mhz):
        freq_min_mhz = float("nan")
        freq_max_mhz = float("nan")
    else:
        freq_min_mhz = freq_center_mhz - bw_mhz / 2.0
        freq_max_mhz = freq_center_mhz + bw_mhz / 2.0

    return {
        "sid": sid,
        "sv": sv,
        "system": system,
        "band": band,
        "code": sig_def.code,
        "freq_center": freq_center_mhz,
        "freq_min": freq_min_mhz,
        "freq_max": freq_max_mhz,
    }


# ---------------------------------------------------------------------------
# SbfReader
# ---------------------------------------------------------------------------


class SbfReader(GNSSDataReader, BaseModel):
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

    # ------------------------------------------------------------------
    # Pre-scan caches
    # ------------------------------------------------------------------

    @cached_property
    def _freq_nr_cache(self) -> dict[int, int]:
        """Pre-scan ALL ChannelStatus blocks to build a complete SVID → FreqNr map.

        Scanning the entire file once means early GLONASS epochs also have
        accurate FDMA frequency assignments in :meth:`iter_epochs`.

        Returns
        -------
        dict of {int: int}
            Mapping from Septentrio SVID to GLONASS frequency slot number.
        """
        parser = sbf_parser.SbfParser()
        cache: dict[int, int] = {}
        for name, data in parser.read(str(self.fpath)):
            if name == "ChannelStatus":
                for sat in data.get("ChannelSatInfo", []):
                    svid = int(sat["SVID"])
                    if svid != 0:
                        cache[svid] = int(sat["FreqNr"])
        return cache

    # ------------------------------------------------------------------
    # GNSSDataReader abstract property implementations
    # ------------------------------------------------------------------

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
    def start_time(self) -> datetime:
        """Return the timestamp of the first decoded epoch.

        Returns
        -------
        datetime
            Timezone-aware UTC datetime of the first observation epoch.

        Raises
        ------
        LookupError
            If the file contains no decodable epochs.
        """
        for epoch in self.iter_epochs():
            return epoch.timestamp
        raise LookupError(f"No epochs in {self.fpath}")

    @cached_property
    def end_time(self) -> datetime:
        """Return the timestamp of the last decoded epoch.

        Returns
        -------
        datetime
            Timezone-aware UTC datetime of the last observation epoch.

        Raises
        ------
        LookupError
            If the file contains no decodable epochs.
        """
        last: datetime | None = None
        for epoch in self.iter_epochs():
            last = epoch.timestamp
        if last is None:
            raise LookupError(f"No epochs in {self.fpath}")
        return last

    @cached_property
    def systems(self) -> list[str]:
        """Return sorted list of GNSS system codes present in the file.

        Returns
        -------
        list of str
            Sorted list of RINEX system letters (e.g. ``["E", "G", "R"]``).
        """
        return sorted({
            obs.system
            for ep in self.iter_epochs()
            for obs in ep.observations
        })

    @cached_property
    def num_satellites(self) -> int:
        """Return the number of unique satellites observed in the file.

        Returns
        -------
        int
            Count of unique ``system + PRN`` pairs across all epochs.
        """
        return len({
            f"{obs.system}{obs.prn:02d}"
            for ep in self.iter_epochs()
            for obs in ep.observations
        })

    # ------------------------------------------------------------------
    # Epoch count (existing cached property — kept for backward compat)
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Header
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Epoch iterator
    # ------------------------------------------------------------------

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
        - The :attr:`_freq_nr_cache` is pre-populated from ALL ChannelStatus
          blocks before the first call, so all GLONASS FDMA epochs have
          accurate carrier frequencies.
        - ``delta_ls`` (leap seconds) is taken from the most recent
          ReceiverTime block; defaults to 18 if none has been seen yet.
        """
        parser = sbf_parser.SbfParser()
        freq_nr_cache: dict[int, int] = self._freq_nr_cache.copy()
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
    # Dataset construction — observations
    # ------------------------------------------------------------------

    def to_ds(
        self,
        keep_rnx_data_vars: list[str] | None = None,
        pad_global_sid: bool = True,
        strip_fillval: bool = True,
        **kwargs: object,
    ) -> xr.Dataset:
        """Convert SBF observations to an ``(epoch, sid)`` xarray Dataset.

        Produces the same structure as :class:`~canvod.readers.rinex.v3_04.Rnxv3Obs`
        and passes :class:`~canvod.readers.base.DatasetStructureValidator`.

        Parameters
        ----------
        keep_rnx_data_vars : list of str, optional
            Data variables to retain.  If ``None``, all six variables are
            kept: ``SNR``, ``Pseudorange``, ``Phase``, ``Doppler``,
            ``LLI``, ``SSI``.
        pad_global_sid : bool, default True
            If ``True``, pads the dataset to the global SID space via
            :func:`canvod.auxiliary.preprocessing.pad_to_global_sid`.
        strip_fillval : bool, default True
            If ``True``, removes fill values via
            :func:`canvod.auxiliary.preprocessing.strip_fillvalue`.
        **kwargs
            Ignored (for ABC compatibility).

        Returns
        -------
        xr.Dataset
            Dataset with dimensions ``(epoch, sid)`` that passes
            :class:`~canvod.readers.base.DatasetStructureValidator`.
        """
        freq_nr_cache = self._freq_nr_cache.copy()

        # --- Pass 1: collect all (epoch, sid) pairs and signal properties ---
        sid_props: dict[str, dict[str, Any]] = {}
        timestamps: list[np.datetime64] = []

        for epoch in self.iter_epochs():
            ts_np = np.datetime64(epoch.timestamp.replace(tzinfo=None), "ns")
            timestamps.append(ts_np)

            for obs in epoch.observations:
                props = _sid_props_from_obs(obs.svid, obs.signal_num, freq_nr_cache)
                if props is None:
                    continue
                sid = props["sid"]
                if sid not in sid_props:
                    sid_props[sid] = props

        sorted_sids = sorted(sid_props)
        sid_to_idx = {sid: i for i, sid in enumerate(sorted_sids)}
        n_epochs = len(timestamps)
        n_sids = len(sorted_sids)

        # Allocate arrays (LLI is dropped — SBF has no loss-of-lock indicator)
        snr_arr = np.full((n_epochs, n_sids), np.nan, dtype=DTYPES["SNR"])
        pr_arr  = np.full((n_epochs, n_sids), np.nan, dtype=DTYPES["Pseudorange"])
        ph_arr  = np.full((n_epochs, n_sids), np.nan, dtype=DTYPES["Phase"])
        dop_arr = np.full((n_epochs, n_sids), np.nan, dtype=DTYPES["Doppler"])
        ssi_arr = np.full((n_epochs, n_sids), -1,     dtype=DTYPES["SSI"])

        # --- Pass 2: fill arrays ---
        for t_idx, epoch in enumerate(self.iter_epochs()):
            for obs in epoch.observations:
                sig_def = SIGNAL_TABLE.get(obs.signal_num)
                if sig_def is None:
                    continue
                sv = f"{obs.system}{obs.prn:02d}"
                sid = f"{sv}|{sig_def.band}|{sig_def.code}"
                s_idx = sid_to_idx.get(sid)
                if s_idx is None:
                    continue

                if obs.cn0 is not None:
                    snr_arr[t_idx, s_idx] = float(obs.cn0.to(UREG.dBHz).magnitude)
                if obs.pseudorange is not None:
                    pr_arr[t_idx, s_idx] = float(
                        obs.pseudorange.to(UREG.meter).magnitude
                    )
                if obs.phase_cycles is not None:
                    ph_arr[t_idx, s_idx] = obs.phase_cycles
                if obs.doppler is not None:
                    dop_arr[t_idx, s_idx] = float(obs.doppler.to(UREG.Hz).magnitude)

        # Build coordinate arrays
        freq_center = np.asarray(
            [sid_props[s]["freq_center"] for s in sorted_sids], dtype=DTYPES["freq_center"]
        )
        freq_min = np.asarray(
            [sid_props[s]["freq_min"] for s in sorted_sids], dtype=DTYPES["freq_min"]
        )
        freq_max = np.asarray(
            [sid_props[s]["freq_max"] for s in sorted_sids], dtype=DTYPES["freq_max"]
        )

        coords: dict[str, Any] = {
            "epoch": ("epoch", timestamps, COORDS_METADATA["epoch"]),
            "sid": xr.DataArray(sorted_sids, dims=["sid"], attrs=COORDS_METADATA["sid"]),
            "sv":     ("sid", [sid_props[s]["sv"]     for s in sorted_sids], COORDS_METADATA["sv"]),
            "system": ("sid", [sid_props[s]["system"] for s in sorted_sids], COORDS_METADATA["system"]),
            "band":   ("sid", [sid_props[s]["band"]   for s in sorted_sids], COORDS_METADATA["band"]),
            "code":   ("sid", [sid_props[s]["code"]   for s in sorted_sids], COORDS_METADATA["code"]),
            "freq_center": ("sid", freq_center, COORDS_METADATA["freq_center"]),
            "freq_min":    ("sid", freq_min,    COORDS_METADATA["freq_min"]),
            "freq_max":    ("sid", freq_max,    COORDS_METADATA["freq_max"]),
        }

        attrs = get_global_attrs()
        attrs["Created"] = datetime.now(timezone.utc).isoformat()
        attrs["Software"] = f"{attrs['Software']}, Version: {get_version_from_pyproject()}"

        ds = xr.Dataset(
            data_vars={
                "SNR":         (["epoch", "sid"], snr_arr, CN0_METADATA),
                "Pseudorange": (["epoch", "sid"], pr_arr,  OBSERVABLES_METADATA["Pseudorange"]),
                "Phase":       (["epoch", "sid"], ph_arr,  OBSERVABLES_METADATA["Phase"]),
                "Doppler":     (["epoch", "sid"], dop_arr, OBSERVABLES_METADATA["Doppler"]),
                "SSI":         (["epoch", "sid"], ssi_arr, OBSERVABLES_METADATA["SSI"]),
            },
            coords=coords,
            attrs=attrs,
        )

        # Post-process
        if keep_rnx_data_vars is not None:
            for var in list(ds.data_vars):
                if var not in keep_rnx_data_vars:
                    ds = ds.drop_vars([var])

        if pad_global_sid:
            from canvod.auxiliary.preprocessing import pad_to_global_sid
            ds = pad_to_global_sid(ds)

        if strip_fillval:
            from canvod.auxiliary.preprocessing import strip_fillvalue
            ds = strip_fillvalue(ds)

        ds.attrs["File Hash"] = self.file_hash
        self.validate_output(ds, required_vars=keep_rnx_data_vars)
        return ds

    # ------------------------------------------------------------------
    # Dataset construction — metadata
    # ------------------------------------------------------------------

    def to_metadata_ds(self, pad_global_sid: bool = True) -> xr.Dataset:
        """Decode SBF metadata blocks to an ``(epoch, sid)`` xarray Dataset.

        Decodes PVTGeodetic, DOP, ReceiverStatus, SatVisibility, and
        MeasExtra blocks in a single file scan.

        Parameters
        ----------
        pad_global_sid : bool, default True
            If ``True``, pads to the global SID space via
            :func:`canvod.auxiliary.preprocessing.pad_to_global_sid`.

        Returns
        -------
        xr.Dataset
            Dataset with dimensions ``(epoch, sid)``.  Epoch-level scalars
            (PDOP, NrSV, …) are 1-D ``(epoch,)`` coordinates.  Satellite
            geometry (theta, phi) and signal quality (MPCorrection, …) are
            ``(epoch, sid)`` data variables.
        """
        parser = sbf_parser.SbfParser()
        freq_nr_cache = self._freq_nr_cache.copy()

        pending: dict[str, Any] = {
            "pvt": None, "dop": None, "status": None,
            "satvis": [], "extra": [],
        }

        # Each record: (ts, pvt, dop, status, satvis, extra, obs_map)
        records: list[tuple[Any, ...]] = []

        # sid discovery — same logic as to_ds() pass 1
        sid_props: dict[str, dict[str, Any]] = {}

        delta_ls: int = _DEFAULT_DELTA_LS

        for name, data in parser.read(str(self.fpath)):
            match name:
                case "ReceiverTime":
                    delta_ls = int(data["DeltaLS"])

                case "ChannelStatus":
                    for sat in data.get("ChannelSatInfo", []):
                        svid_cs = int(sat["SVID"])
                        if svid_cs != 0:
                            freq_nr_cache[svid_cs] = int(sat["FreqNr"])

                case "PVTGeodetic":
                    pending["pvt"] = data

                case "DOP":
                    pending["dop"] = data

                case "ReceiverStatus":
                    pending["status"] = data

                case "SatVisibility":
                    pending["satvis"] = list(data.get("SatInfo", []))

                case "MeasExtra":
                    pending["extra"] = list(data.get("MeasExtraChannel", []))

                case "MeasEpoch":
                    tow_ms = int(data["TOW"])
                    wn = int(data["WNc"])
                    ts = _tow_wn_to_utc(tow_ms, wn, delta_ls)
                    obs_map = _build_obs_map(data)

                    # Discover sids from Type1 and Type2 sub-blocks
                    for t1 in data.get("Type_1", []):
                        svid1 = int(t1["SVID"])
                        props1 = _sid_props_from_obs(
                            svid1,
                            decode_signal_num(int(t1["Type"]), int(t1["ObsInfo"])),
                            freq_nr_cache,
                        )
                        if props1 is not None and props1["sid"] not in sid_props:
                            sid_props[props1["sid"]] = props1

                        for t2 in t1.get("Type_2", []):
                            props2 = _sid_props_from_obs(
                                svid1,
                                decode_signal_num(int(t2["Type"]), int(t2["ObsInfo"])),
                                freq_nr_cache,
                            )
                            if props2 is not None and props2["sid"] not in sid_props:
                                sid_props[props2["sid"]] = props2

                    records.append((
                        ts,
                        pending["pvt"],
                        pending["dop"],
                        pending["status"],
                        list(pending["satvis"]),
                        list(pending["extra"]),
                        obs_map,
                    ))
                    pending = {
                        "pvt": None, "dop": None, "status": None,
                        "satvis": [], "extra": [],
                    }

        # Build index structures
        sorted_sids = sorted(sid_props)
        sid_to_idx = {sid: i for i, sid in enumerate(sorted_sids)}
        n_epochs = len(records)
        n_sids = len(sorted_sids)

        # sv → list of sid indices (for SatVisibility broadcasting)
        sids_for_sv: dict[str, list[int]] = {}
        for sid in sorted_sids:
            sv = sid_props[sid]["sv"]
            sids_for_sv.setdefault(sv, []).append(sid_to_idx[sid])

        # (epoch, sid) data variable arrays
        theta_arr    = np.full((n_epochs, n_sids), np.nan, dtype=np.float32)
        phi_arr      = np.full((n_epochs, n_sids), np.nan, dtype=np.float32)
        rise_set_arr = np.full((n_epochs, n_sids), -1,    dtype=np.int8)
        mp_corr_arr  = np.full((n_epochs, n_sids), np.nan, dtype=np.float32)
        code_var_arr = np.full((n_epochs, n_sids), np.nan, dtype=np.float32)
        carr_var_arr = np.full((n_epochs, n_sids), np.nan, dtype=np.float32)

        # (epoch,) scalar coordinate arrays
        pdop_arr      = np.full(n_epochs, np.nan, dtype=np.float32)
        hdop_arr      = np.full(n_epochs, np.nan, dtype=np.float32)
        vdop_arr      = np.full(n_epochs, np.nan, dtype=np.float32)
        n_sv_arr      = np.full(n_epochs, -1,     dtype=np.int16)
        h_acc_arr     = np.full(n_epochs, np.nan, dtype=np.float32)
        v_acc_arr     = np.full(n_epochs, np.nan, dtype=np.float32)
        pvt_mode_arr  = np.full(n_epochs, -1,     dtype=np.int8)
        mean_corr_arr = np.full(n_epochs, np.nan, dtype=np.float32)
        cpu_load_arr  = np.full(n_epochs, -1,     dtype=np.int8)
        temp_arr      = np.full(n_epochs, np.nan, dtype=np.float32)
        rx_error_arr  = np.full(n_epochs, 0,      dtype=np.int32)

        timestamps: list[np.datetime64] = []

        # Fill arrays from records
        for t_idx, (ts, pvt, dop, status, satvis, extra, obs_map) in enumerate(records):
            timestamps.append(np.datetime64(ts.replace(tzinfo=None), "ns"))

            # DOP block → pdop, hdop, vdop
            if dop is not None:
                try:
                    pdop_arr[t_idx] = float(dop["PDOP"]) * 0.01
                    hdop_arr[t_idx] = float(dop["HDOP"]) * 0.01
                    vdop_arr[t_idx] = float(dop["VDOP"]) * 0.01
                except (KeyError, TypeError, ValueError):
                    pass

            # PVTGeodetic → n_sv, accuracy, mode, correction age
            if pvt is not None:
                try:
                    n_sv_arr[t_idx]      = int(pvt.get("NrSV", pvt.get("NrSVAnt", -1)))
                    h_acc_arr[t_idx]     = float(pvt["HAccuracy"]) * 0.001
                    v_acc_arr[t_idx]     = float(pvt["VAccuracy"]) * 0.001
                    pvt_mode_arr[t_idx]  = int(pvt["Mode"])
                    mean_corr_arr[t_idx] = float(pvt["MeanCorrAge"]) * 0.01
                    # Also pick up DOP from PVTGeodetic if DOP block absent
                    if np.isnan(pdop_arr[t_idx]):
                        pdop_arr[t_idx] = float(pvt["PDOP"]) * 0.01
                        hdop_arr[t_idx] = float(pvt["HDOP"]) * 0.01
                        vdop_arr[t_idx] = float(pvt["VDOP"]) * 0.01
                except (KeyError, TypeError, ValueError):
                    pass

            # ReceiverStatus → cpu_load, temperature, rx_error
            if status is not None:
                try:
                    cpu_load_arr[t_idx] = int(status["CPULoad"])
                    temp_arr[t_idx]     = float(status["Temperature"]) * 0.1
                    rx_error_arr[t_idx] = int(status["RxError"])
                except (KeyError, TypeError, ValueError):
                    pass

            # SatVisibility → broadcast theta/phi to all sids for that sv
            for sat_info in satvis:
                try:
                    svid_raw   = int(sat_info["SVID"])
                    sys_code, prn = decode_svid(svid_raw)
                    sv         = f"{sys_code}{prn:02d}"
                    theta_deg  = 90.0 - int(sat_info["Elevation"]) * 0.01
                    phi_deg    = int(sat_info["Azimuth"]) * 0.01
                    rs         = int(sat_info["RiseSet"])
                    for s_idx in sids_for_sv.get(sv, []):
                        theta_arr[t_idx, s_idx]    = theta_deg
                        phi_arr[t_idx, s_idx]      = phi_deg
                        rise_set_arr[t_idx, s_idx] = rs
                except (KeyError, TypeError, ValueError):
                    pass

            # MeasExtra → per-(epoch, sid) signal quality
            for ch in extra:
                try:
                    type_byte = int(ch["Type"])
                    info_byte = int(ch.get("ObsInfo", ch.get("Info", 0)))
                    sig_num   = decode_signal_num(type_byte, info_byte)
                    rx_ch     = int(ch["RxChannel"])
                    svid      = obs_map.get((rx_ch, sig_num))
                    if svid is None:
                        continue
                    sig_def = SIGNAL_TABLE.get(sig_num)
                    if sig_def is None:
                        continue
                    sys_code2, prn2 = decode_svid(svid)
                    sv2  = f"{sys_code2}{prn2:02d}"
                    sid  = f"{sv2}|{sig_def.band}|{sig_def.code}"
                    s_idx = sid_to_idx.get(sid)
                    if s_idx is None:
                        continue
                    mp_raw = int(
                        ch.get("MPCorrection ", ch.get("MPCorrection", 0))
                    )
                    mp_corr_arr[t_idx, s_idx]  = mp_raw * 0.001
                    raw_cv = ch.get("CodeVar")
                    raw_rv = ch.get("CarrierVar")
                    if raw_cv is not None:
                        code_var_arr[t_idx, s_idx] = float(raw_cv)
                    if raw_rv is not None:
                        carr_var_arr[t_idx, s_idx] = float(raw_rv)
                except (KeyError, TypeError, ValueError):
                    pass

        # Build Dataset
        freq_center = np.asarray(
            [sid_props[s]["freq_center"] for s in sorted_sids], dtype=np.float32
        )
        freq_min = np.asarray(
            [sid_props[s]["freq_min"] for s in sorted_sids], dtype=np.float32
        )
        freq_max = np.asarray(
            [sid_props[s]["freq_max"] for s in sorted_sids], dtype=np.float32
        )

        coords: dict[str, Any] = {
            "epoch": ("epoch", timestamps, COORDS_METADATA["epoch"]),
            "sid":   xr.DataArray(sorted_sids, dims=["sid"], attrs=COORDS_METADATA["sid"]),
            "sv":     ("sid", [sid_props[s]["sv"]     for s in sorted_sids], COORDS_METADATA["sv"]),
            "system": ("sid", [sid_props[s]["system"] for s in sorted_sids], COORDS_METADATA["system"]),
            "band":   ("sid", [sid_props[s]["band"]   for s in sorted_sids], COORDS_METADATA["band"]),
            "code":   ("sid", [sid_props[s]["code"]   for s in sorted_sids], COORDS_METADATA["code"]),
            "freq_center": ("sid", freq_center, COORDS_METADATA["freq_center"]),
            "freq_min":    ("sid", freq_min,    COORDS_METADATA["freq_min"]),
            "freq_max":    ("sid", freq_max,    COORDS_METADATA["freq_max"]),
            # Epoch-level scalars (1-D over epoch)
            "pdop":            ("epoch", pdop_arr),
            "hdop":            ("epoch", hdop_arr),
            "vdop":            ("epoch", vdop_arr),
            "n_sv":            ("epoch", n_sv_arr),
            "h_accuracy_m":    ("epoch", h_acc_arr),
            "v_accuracy_m":    ("epoch", v_acc_arr),
            "pvt_mode":        ("epoch", pvt_mode_arr),
            "mean_corr_age_s": ("epoch", mean_corr_arr),
            "cpu_load":        ("epoch", cpu_load_arr),
            "temperature_c":   ("epoch", temp_arr),
            "rx_error":        ("epoch", rx_error_arr),
        }

        attrs = get_global_attrs()
        attrs["Created"] = datetime.now(timezone.utc).isoformat()
        attrs["Software"] = f"{attrs['Software']}, Version: {get_version_from_pyproject()}"
        attrs["File Hash"] = self.file_hash

        ds = xr.Dataset(
            data_vars={
                "theta":           (["epoch", "sid"], theta_arr,    _THETA_ATTRS),
                "phi":             (["epoch", "sid"], phi_arr,      _PHI_ATTRS),
                "rise_set":        (["epoch", "sid"], rise_set_arr),
                "mp_correction_m": (["epoch", "sid"], mp_corr_arr),
                "code_var":        (["epoch", "sid"], code_var_arr),
                "carrier_var":     (["epoch", "sid"], carr_var_arr),
            },
            coords=coords,
            attrs=attrs,
        )

        if pad_global_sid:
            from canvod.auxiliary.preprocessing import pad_to_global_sid
            ds = pad_to_global_sid(ds)

        return ds

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
