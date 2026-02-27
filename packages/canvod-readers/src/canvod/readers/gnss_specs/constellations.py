"""GNSS constellation definitions and frequency lookup tables."""

from __future__ import annotations

import json
import re
import sqlite3
import threading
from abc import ABC, abstractmethod
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, NoReturn

import pandas as pd
import requests
from canvod.readers.gnss_specs.constants import FREQ_UNIT, UREG
from natsort import natsorted

if TYPE_CHECKING:
    import pint

# Register SQLite adapters for datetime (Python 3.12+ compatibility)
sqlite3.register_adapter(datetime, lambda dt: dt.isoformat())
sqlite3.register_converter(
    "DATETIME",
    lambda s: datetime.fromisoformat(s.decode()),
)


# ================================================================
# -------------------- Shared Wikipedia Cache --------------------
# ================================================================
class WikipediaCache:
    """Shared cache for all GNSS constellation satellite lists.

    Parameters
    ----------
    cache_hours : int, optional
        How long cache entries are considered valid. Default is 6.

    """

    REQUEST_TIMEOUT: ClassVar[float] = 10.0

    def __init__(self, cache_hours: int = 6) -> None:
        """Initialize the shared Wikipedia cache."""
        self.cache_file: str = "gnss_satellites_cache.db"
        self.cache_hours: int = cache_hours
        self.headers: dict[str, str] = {
            "User-Agent": "GNSSResearch/1.0 (your.email@example.com)"
        }
        self._locks: dict[str, threading.Lock] = {}
        self._init_db()

    def _init_db(self) -> None:
        """Initialize SQLite database if it does not exist."""
        conn = sqlite3.connect(self.cache_file, detect_types=sqlite3.PARSE_DECLTYPES)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS satellite_cache (
                constellation TEXT PRIMARY KEY,
                svs_data TEXT NOT NULL,
                raw_data TEXT,
                fetched_at DATETIME NOT NULL,
                url TEXT
            )
            """)
        conn.commit()
        conn.close()

    def _get_lock(self, constellation: str) -> threading.Lock:
        """Return the per-constellation lock.

        Parameters
        ----------
        constellation : str
            Constellation identifier (e.g., "GPS").

        Returns
        -------
        threading.Lock
            A per-constellation threading lock.

        """
        if constellation not in self._locks:
            self._locks[constellation] = threading.Lock()
        return self._locks[constellation]

    def get_cached_svs(self, constellation: str) -> list[str] | None:
        """Retrieve fresh cached SV list for a constellation.

        Parameters
        ----------
        constellation : str
            Constellation identifier.

        Returns
        -------
        list of str or None
            List of SV PRNs if cache is valid, else None.

        """
        conn = sqlite3.connect(
            self.cache_file,
            detect_types=sqlite3.PARSE_DECLTYPES,
        )
        cutoff = datetime.now(UTC) - timedelta(hours=self.cache_hours)
        cursor = conn.execute(
            "SELECT svs_data FROM satellite_cache WHERE constellation = ? "
            "AND fetched_at > ?",
            (constellation, cutoff),
        )
        result = cursor.fetchone()
        conn.close()
        return json.loads(result[0]) if result else None

    def get_stale_cache(self, constellation: str) -> list[str] | None:
        """Retrieve most recent cached SV list (ignores freshness).

        Parameters
        ----------
        constellation : str
            Constellation identifier.

        Returns
        -------
        list of str or None
            List of SV PRNs if present in cache, else None.

        """
        conn = sqlite3.connect(
            self.cache_file,
            detect_types=sqlite3.PARSE_DECLTYPES,
        )
        cursor = conn.execute(
            "SELECT svs_data FROM satellite_cache WHERE constellation = ? "
            "ORDER BY fetched_at DESC LIMIT 1",
            (constellation,),
        )
        result = cursor.fetchone()
        conn.close()
        return json.loads(result[0]) if result else None

    def fetch_and_cache(
        self,
        constellation: str,
        url: str,
        table_index: int,
        prn_column: str,
        status_filter: dict[str, str] | None = None,
        re_pattern: str = r"\b[A-Z]\d{2}\b",
    ) -> list[str]:
        r"""Fetch constellation satellite list from Wikipedia, clean it, cache.

        Parameters
        ----------
        constellation : str
            Constellation identifier.
        url : str
            Wikipedia URL to fetch from.
        table_index : int
            Index of table in page that contains PRN column.
        prn_column : str
            Column containing PRN identifiers.
        status_filter : dict, optional
            Dictionary with ``{"column": ..., "value": ...}`` for filtering
            (e.g., only "Operational").
        re_pattern : str, optional
            Regex to clean PRN identifiers (default: ``r"\\b[A-Z]\\d{2}\\b"``).

        Returns
        -------
        list of str
            Sorted list of SV identifiers.

        """
        lock = self._get_lock(constellation)
        with lock:
            cached = self.get_cached_svs(constellation)
            if cached:
                return cached

            try:
                response = requests.get(
                    url,
                    headers=self.headers,
                    timeout=self.REQUEST_TIMEOUT,
                )
                response.raise_for_status()
                tables = pd.read_html(response.content)
                if not tables or len(tables) <= table_index:
                    msg = f"No suitable table found at index {table_index}"
                    self._raise_value_error(msg)
                df = tables[table_index]

                if status_filter:
                    df = df[
                        df[status_filter["column"]].str.contains(
                            status_filter["value"],
                            case=True,
                            na=False,
                        )
                    ]

                if prn_column not in df.columns:
                    potential_cols = [col for col in df.columns if "prn" in col.lower()]
                    if potential_cols:
                        prn_column = potential_cols[0]
                    else:
                        msg = f"PRN column '{prn_column}' not found in {df.columns}"
                        self._raise_value_error(msg)

                prn_data: list[str] = list(df[prn_column])
                clean_list: list[str] = [
                    m.group()
                    for item in prn_data
                    if isinstance(item, str)
                    if (m := re.search(re_pattern, item))
                ]
                if not clean_list:
                    msg = "No valid PRN data found after cleaning"
                    self._raise_value_error(msg)

                conn = sqlite3.connect(
                    self.cache_file,
                    detect_types=sqlite3.PARSE_DECLTYPES,
                )
                conn.execute(
                    """
                    INSERT OR REPLACE INTO satellite_cache
                    (constellation, svs_data, raw_data, fetched_at, url)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        constellation,
                        json.dumps(sorted(set(clean_list))),
                        df.to_json(),
                        datetime.now(UTC).isoformat(),
                        url,
                    ),
                )
                conn.commit()
                conn.close()

                return sorted(set(clean_list))
            except (
                requests.RequestException,
                ValueError,
                KeyError,
                IndexError,
                TypeError,
                pd.errors.ParserError,
            ):
                stale = self.get_stale_cache(constellation)
                if stale:
                    return stale
                raise

    @staticmethod
    def _raise_value_error(message: str) -> NoReturn:
        """Raise a ValueError with a formatted message."""
        raise ValueError(message)


# Shared instance
_wikipedia_cache = WikipediaCache()

# ================================================================
# ------------ Pre-Compiled Regex for Data Validation ------------
# ================================================================

# Pre-compiled regex patterns used for better perfromance in data validation
SV_PATTERN = re.compile(r"^[GRECJSI]\d{2}$")  # e.g., G01, R12, E25
OBS_TYPE_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9\d]?[A-Z0-9]?$")  # e.g., *1C, *5X


# ================================================================
# -------------------- Base Class --------------------
# ================================================================
class ConstellationBase(ABC):
    r"""Abstract base class for GNSS constellations.

    Notes
    -----
    This class uses ``ABC`` and defines the abstract ``freqs_lut`` property.

    Parameters
    ----------
    constellation : str
        Name of the constellation (e.g., "GPS", "GALILEO").
    url : str, optional
        Wikipedia URL to fetch SV list from.
    re_pattern : str, optional
        Regex pattern to extract PRNs (default ``r"\\b[A-Z]\\d{2}\\b"``).
    table_index : int, optional
        Index of the HTML table to parse for PRNs (default 0).
    prn_column : str, optional
        Column name containing PRNs (default "PRN").
    status_filter : dict, optional
        Filter definition with keys ``{"column": ..., "value": ...}``.
    use_wiki : bool, optional
        If True, fetch SVs from Wikipedia (default True).
    static_svs : list of str, optional
        Provide a static list of SVs if not using Wikipedia.
    aggregate_fdma : bool, optional
        If True, aggregate FDMA bands when supported (default True).

    """

    BANDS: ClassVar[dict[str, str]] = {}
    BAND_CODES: ClassVar[dict[str, list[str]]] = {}
    BAND_PROPERTIES: ClassVar[dict[str, dict[str, pint.Quantity]]] = {}
    AUX_FREQ: ClassVar[pint.Quantity] = 1575.42 * UREG.MHz

    def __init__(
        self,
        constellation: str,
        url: str | None = None,
        re_pattern: str = r"\b[A-Z]\d{2}\b",
        table_index: int = 0,
        prn_column: str = "PRN",
        status_filter: dict[str, str] | None = None,
        use_wiki: bool = True,
        static_svs: list[str] | None = None,
        aggregate_fdma: bool = True,
    ) -> None:
        """Initialize the constellation base."""
        self.constellation: str = constellation
        self.url: str | None = url
        self.re_pattern: str = re_pattern
        self.table_index: int = table_index
        self.prn_column: str = prn_column
        self.status_filter: dict[str, str] | None = status_filter
        self.svs: list[str] = (
            static_svs if static_svs else (self.get_svs() if use_wiki and url else [])
        )
        self.x1: dict[str, pint.Quantity] = {"X1": self.AUX_FREQ}
        self.aggregate_fdma = aggregate_fdma

    def get_svs(self) -> list[str]:
        """Fetch the list of SVs for this constellation.

        Returns
        -------
        list of str
            List of PRNs (satellite identifiers).

        """
        cached = _wikipedia_cache.get_cached_svs(self.constellation)
        if cached:
            return cached
        return _wikipedia_cache.fetch_and_cache(
            constellation=self.constellation,
            url=self.url,
            table_index=self.table_index,
            prn_column=self.prn_column,
            status_filter=self.status_filter,
            re_pattern=self.re_pattern,
        )

    @property
    def bands_freqs(self) -> dict[str, pint.Quantity]:
        """Generate RINEX observation codes mapped to frequencies.

        Returns
        -------
        dict
            Keys are obs codes (e.g., ``"*1C"``), values are frequencies in Hz.

        """
        out: dict[str, pint.Quantity] = {}
        for band_num, band_name in self.BANDS.items():
            freq = self.BAND_PROPERTIES[band_name]["freq"].to(FREQ_UNIT)
            for code in self.BAND_CODES[band_name]:
                out[f"*{band_num}{code}"] = freq
        return out

    @property
    @abstractmethod
    def freqs_lut(self) -> dict[str, pint.Quantity]:
        """Build mapping of SV|obs_code to frequency.

        Returns
        -------
        dict
            Keys of the form ``"SV|*1C"`` and values are frequencies.

        """


# ================================================================
# ------------ 1. Global Navigation Satellite Systems ------------
# ================================================================
class GALILEO(ConstellationBase):
    """Galileo constellation model.

    - Band numbers and codes from RINEX v3.04 Guide:
      http://acc.igs.org/misc/rinex304.pdf (Table 6).
    - Band frequencies and bandwidths from Galileo ICD:
      https://galileognss.eu/wp-content/uploads/2021/01/Galileo_OS_SIS_ICD_v2.0.pdf
      (Tables 2 & 3).

    Might need adaptation for future Galileo signals and RINEX versions.

    Note 1:
    -------
    The E5a and E5b signals are part of the E5 signal in its full bandwidth.

    Note 2:
    -------
    Bandwidths specified here refer to the Receiver Reference Bandwidths.

    Notes
    -----
    This class fetches the current satellite list from Wikipedia.

    """

    BANDS: ClassVar[dict[str, str]] = {
        "1": "E1",
        "5": "E5a",
        "7": "E5b",
        "6": "E6",
        "8": "E5",
    }
    BAND_CODES: ClassVar[dict[str, list[str]]] = {
        "E1": ["A", "B", "C", "X", "Z"],
        "E5a": ["I", "Q", "X"],
        "E5b": ["I", "Q", "X"],
        "E5": ["I", "Q", "X"],
        "E6": ["A", "B", "C", "X", "Z"],
    }
    BAND_PROPERTIES: ClassVar[dict[str, dict[str, pint.Quantity]]] = {
        "E1": {
            "freq": 1575.42 * UREG.MHz,
            "bandwidth": 24.552 * UREG.MHz,
            "system": "E",
        },
        "E5a": {
            "freq": 1176.45 * UREG.MHz,
            "bandwidth": 20.46 * UREG.MHz,
            "system": "E",
        },
        "E5b": {
            "freq": 1207.14 * UREG.MHz,
            "bandwidth": 20.46 * UREG.MHz,
            "system": "E",
        },
        "E6": {
            "freq": 1278.75 * UREG.MHz,
            "bandwidth": 40.92 * UREG.MHz,
            "system": "E",
        },
        "E5": {
            "freq": 1191.795 * UREG.MHz,
            "bandwidth": 51.15 * UREG.MHz,
            "system": "E",
        },
    }

    def __init__(self) -> None:
        """Initialize Galileo constellation."""
        super().__init__(
            constellation="GALILEO",
            url="https://en.wikipedia.org/wiki/List_of_Galileo_satellites",
            re_pattern=r"\bE\d{2}\b",
            table_index=1,
            prn_column="PRN",
            use_wiki=False,
            static_svs=[f"E{x:02d}" for x in range(1, 37)],  # E01-E36
        )

    @property
    def freqs_lut(self) -> dict[str, pint.Quantity]:
        """Build mapping of SV|obs_code to frequency.

        Returns
        -------
        dict
            Keys of format "SV|*1C" mapped to frequencies

        """
        out = {
            f"{sv}|{obs}": freq
            for obs, freq in self.bands_freqs.items()
            for sv in self.svs
        }
        out.update({f"{sv}|X1": self.x1["X1"] for sv in self.svs})
        return {k: out[k] for k in natsorted(out.keys())}


class GPS(ConstellationBase):
    """GPS constellation model.

    - Band numbers and codes from RINEX v3.04 Guide:
      http://acc.igs.org/misc/rinex304.pdf (Table 4).
    - L1/L2 frequencies and bandwidths from GPS L1/L2 ICD:
      https://www.gps.gov/technical/icwg/IS-GPS-200N.pdf (3.3.1.1 Frequency
      Plan).
    - L5 frequency and bandwidth from GPS L5 ICD:
      https://www.gps.gov/technical/icwg/IS-GPS-705J.pdf (3.3.1.1 Frequency
      Plan).

    Note:
    ----
    L1/L2 bandwidth technically depends on the GPS Block. Blocks IIR, IIR-M and
    IIF have a bandwidth of 20.46 MHz, while Block III and IIIF has a bandwidth
    of 30.69 MHz. We assume the larger bandwidth here.

    Parameters
    ----------
    use_wiki : bool, default False
        If False, uses static list G01-G32. If True, fetches from Wikipedia.

    """

    BANDS: ClassVar[dict[str, str]] = {"1": "L1", "2": "L2", "5": "L5"}
    BAND_CODES: ClassVar[dict[str, list[str]]] = {
        "L1": ["C", "S", "L", "X", "P", "W", "Y", "M", "N"],
        "L2": ["C", "D", "S", "L", "X", "P", "W", "Y", "M", "N"],
        "L5": ["I", "Q", "X"],
    }
    BAND_PROPERTIES: ClassVar[dict[str, dict[str, pint.Quantity]]] = {
        "L1": {
            "freq": 1575.42 * UREG.MHz,
            "bandwidth": 30.69 * UREG.MHz,
            "system": "G",
        },
        "L2": {
            "freq": 1227.60 * UREG.MHz,
            "bandwidth": 30.69 * UREG.MHz,
            "system": "G",
        },
        "L5": {"freq": 1176.45 * UREG.MHz, "bandwidth": 24 * UREG.MHz, "system": "G"},
    }

    def __init__(self, use_wiki: bool = False) -> None:
        """Initialize GPS constellation."""
        super().__init__(
            constellation="GPS",
            url="https://en.wikipedia.org/wiki/List_of_GPS_satellites",
            re_pattern=r"\bG\d{2}\b",
            table_index=0,
            prn_column="PRN",
            status_filter={"column": "Status", "value": "Operational"},
            use_wiki=use_wiki,
            static_svs=[f"G{x:02d}" for x in range(1, 33)],
        )

    @property
    def freqs_lut(self) -> dict[str, pint.Quantity]:
        """See base class."""
        out = {
            f"{sv}|{obs}": freq
            for obs, freq in self.bands_freqs.items()
            for sv in self.svs
        }
        out.update({f"{sv}|X1": self.x1["X1"] for sv in self.svs})
        return {k: out[k] for k in natsorted(out.keys())}


class BEIDOU(ConstellationBase):
    """BeiDou constellation model.

    - Band numbers and codes from RINEX v3.04 Guide:
      http://acc.igs.org/misc/rinex304.pdf (Table 9).
    - B1I (Rinex: B1-2) frequency and bandwidth from B1I ICD:
      http://en.beidou.gov.cn/SYSTEMS/ICD/201902/P020190227702348791891.pdf
      (4.2.1 Carrier Frequency, 4.2.7 Signal Bandwidth).
    - B1C (Rinex: B1) frequency and bandwidth from B1C ICD:
      http://en.beidou.gov.cn/SYSTEMS/ICD/201806/P020180608519640359959.pdf
      (4 Signal Characteristics).
    - B2b frequency and bandwidth from B2b ICD:
      http://en.beidou.gov.cn/SYSTEMS/ICD/202008/P020231201537880833625.pdf
      (4 Signal Characteristics).
    - B2a frequency and bandwidth from B2a ICD:
      http://en.beidou.gov.cn/SYSTEMS/ICD/201806/P020180608518432765621.pdf
      (4 Signal Characteristics).
    - B3I (Rinex B3) frequency and bandwidth from B3I ICD:
      http://en.beidou.gov.cn/SYSTEMS/ICD/201806/P020180608516798097666.pdf
      (4.2.1 Carrier Frequency, 4.2.7 Signal Bandwidth).

    Note 1:
    -------
    Band names used here do not refer to the Rinex band names, but to the
    BeiDou signal names.

    Note 2:
    -------
    No ICD for the combined B2 band was found. The center frequency is taken
    from the Rinex v3.04 Guide, perfectly centered between B2a and B2b. The
    bandwidth is speculative, assumed to cover both B2a and B2b signals and
    their bandwidths.

    Notes
    -----
    This class fetches the current satellite list from Wikipedia.

    """

    BANDS: ClassVar[dict[str, str]] = {
        "2": "B1I",
        "1": "B1C",
        "5": "B2a",
        "7": "B2b",
        "6": "B3I",
        "8": "B2",
    }
    BAND_CODES: ClassVar[dict[str, list[str]]] = {
        "B1I": ["I", "Q", "X"],
        "B1C": ["D", "P", "X", "A", "N"],
        "B2a": ["D", "P", "X"],
        "B2b": ["I", "Q", "X", "D", "P", "Z"],
        "B3I": ["I", "Q", "X", "A"],
        "B2": ["D", "P", "X"],
    }
    BAND_PROPERTIES: ClassVar[dict[str, dict[str, pint.Quantity]]] = {
        "B1I": {
            "freq": 1561.098 * UREG.MHz,
            "bandwidth": 4.092 * UREG.MHz,
            "system": "C",
        },
        "B1C": {
            "freq": 1575.42 * UREG.MHz,
            "bandwidth": 32.736 * UREG.MHz,
            "system": "C",
        },
        "B2a": {
            "freq": 1176.45 * UREG.MHz,
            "bandwidth": 20.46 * UREG.MHz,
            "system": "C",
        },
        "B2b": {
            "freq": 1207.14 * UREG.MHz,
            "bandwidth": 20.46 * UREG.MHz,
            "system": "C",
        },
        "B3I": {
            "freq": 1268.52 * UREG.MHz,
            "bandwidth": 20.46 * UREG.MHz,
            "system": "C",
        },
        "B2": {
            "freq": 1191.795 * UREG.MHz,
            "bandwidth": 51.15 * UREG.MHz,  # speculative
            "system": "C",
        },
    }

    def __init__(self) -> None:
        """Initialize BeiDou constellation."""
        super().__init__(
            constellation="BEIDOU",
            url="https://en.wikipedia.org/wiki/List_of_BeiDou_satellites",
            re_pattern=r"\bC\d{2}\b",
            table_index=2,
            prn_column="PRN[8]",
            status_filter={"column": "Status[8][9]", "value": "Operational"},
            use_wiki=False,
            static_svs=[f"C{x:02d}" for x in range(1, 64)],  # C01-C63
        )

    @property
    def freqs_lut(self) -> dict[str, pint.Quantity]:
        """See base class."""
        out = {
            f"{sv}|{obs}": freq
            for obs, freq in self.bands_freqs.items()
            for sv in self.svs
        }
        out.update({f"{sv}|X1": self.x1["X1"] for sv in self.svs})
        return {k: out[k] for k in natsorted(out.keys())}


class GLONASS(ConstellationBase):
    """GLONASS constellation model (uses FDMA for L1/L2).

    Notes
    -----

    References
    ----------
      - Band numbers, codes, frequencies and FDMA equations from RINEX v3.04
        Guide: http://acc.igs.org/misc/rinex304.pdf (Table 5).
      - Bandwidths from GLONASS ICD:
        https://www.unavco.org/help/glossary/docs/
        ICD_GLONASS_4.0_(1998)_en.pdf (3.3.1.4 Spurious emissions).
      - GLONASS channel assignment from: see included channel file.

    G1/G2 is treated as a single band here, although it consists of sub-bands
    according to FDMA (see `GLONASS.band_G1_equation()`). The center frequency
    is the average of all sub-band frequencies. The bandwidth spans all
    sub-band widths, so the center frequency differs slightly from the FDMA
    base value.

    Attributes
    ----------
    glonass_channel_pth : Path, optional
        Path to GLONASS channel assignment file (default:
        "GLONASS_channels.txt" in the same directory as this file).
    aggregate_fdma : bool, default True
        If True, aggregate FDMA sub-bands into single G1/G2 bands.
        If False, maintain individual FDMA channel frequencies.

    Raises
    ------
    FileNotFoundError
        If GLONASS channel file does not exist.

    """

    BANDS: ClassVar[dict[str, str]] = {"3": "G3", "4": "G1a", "6": "G2a"}
    BAND_CODES: ClassVar[dict[str, list[str]]] = {
        "G1": ["C", "P"],
        "G2": ["C", "P"],
        "G3": ["I", "Q", "X"],
        "G1a": ["A", "B", "X"],
        "G2a": ["A", "B", "X"],
    }
    BAND_PROPERTIES: ClassVar[dict[str, dict[str, pint.Quantity]]] = {
        "G1a": {
            "freq": 1600.995 * UREG.MHz,
            "bandwidth": 7.875 * UREG.MHz,
            "system": "R",
        },
        "G2a": {
            "freq": 1248.06 * UREG.MHz,
            "bandwidth": 7.875 * UREG.MHz,
            "system": "R",
        },
        "G3": {
            "freq": 1202.025 * UREG.MHz,
            "bandwidth": 7.875 * UREG.MHz,
            "system": "R",
        },
    }

    AGGR_BANDS: ClassVar[dict[str, str]] = {
        "1": "G1",
        "2": "G2",
    }
    AGGR_BAND_CODES: ClassVar[dict[str, list[str]]] = {
        "G1": ["C", "P"],
        "G2": ["C", "P"],
    }

    AGGR_G1_G2_BAND_PROPERTIES: ClassVar[dict[str, dict[str, pint.Quantity]]] = {
        "G1": {
            "freq": 1602.28125 * UREG.MHz,  # see Note on G1 & G2
            "bandwidth": 8.3345 * UREG.MHz,  # see Note on G1 & G2
            "system": "R",
        },
        "G2": {
            "freq": 1246.21875 * UREG.MHz,  # see Note on G1 & G2
            "bandwidth": 6.7095 * UREG.MHz,  # see Note on G1 & G2
            "system": "R",
        },
    }

    SV_DEPENDENT_BANDS: ClassVar[list[str]] = ["*1C", "*1P", "*2C", "*2P"]
    G1_G2_subband_bandwidth: ClassVar[pint.Quantity] = 1.022 * UREG.MHz

    def __init__(
        self,
        glonass_channel_pth: Path | None = Path(__file__).parent
        / "GLONASS_channels.txt",
        aggregate_fdma: bool = True,
    ) -> None:
        """Initialize GLONASS constellation with FDMA channel assignments."""
        if not glonass_channel_pth.exists():
            msg = f"{glonass_channel_pth} does not exist"
            raise FileNotFoundError(msg)
        self.pth = glonass_channel_pth
        self.svs: list[str] = [f"R{i:02d}" for i in range(1, 25)]
        self.x1 = {"X1": self.AUX_FREQ}
        self.aggregate_fdma = aggregate_fdma

        if self.aggregate_fdma:
            # Aggregate mode: G1 and G2 are single bands
            self.BANDS = {**self.BANDS, **self.AGGR_BANDS}
            self.BAND_CODES = {**self.BAND_CODES, **self.AGGR_BAND_CODES}
            self.BAND_PROPERTIES = {
                **self.BAND_PROPERTIES,
                **self.AGGR_G1_G2_BAND_PROPERTIES,
            }
        else:
            # Non-aggregate mode: Map 1/2 to FDMA bands
            # Note: Frequencies will be computed per-SV in freqs_lut
            self.BANDS = {**self.BANDS, "1": "G1_FDMA", "2": "G2_FDMA"}
            self.BAND_CODES = {
                **self.BAND_CODES,
                "G1_FDMA": ["C", "P"],
                "G2_FDMA": ["C", "P"],
            }
            # Add placeholder properties (actual freqs are SV-dependent)
            self.BAND_PROPERTIES = {
                **self.BAND_PROPERTIES,
                "G1_FDMA": {
                    "freq": 1602.0 * UREG.MHz,  # Nominal center
                    "bandwidth": 9.0 * UREG.MHz,  # FDMA range
                    "system": "R",
                    "fdma": True,  # Flag for SV-dependent frequency
                },
                "G2_FDMA": {
                    "freq": 1246.0 * UREG.MHz,  # Nominal center
                    "bandwidth": 7.0 * UREG.MHz,  # FDMA range
                    "system": "R",
                    "fdma": True,  # Flag for SV-dependent frequency
                },
            }

    def get_channel_used_by_SV(self, sv: str) -> int:
        """Return the GLONASS channel number for a satellite.

        Parameters
        ----------
        sv : str
            GLONASS satellite identifier (e.g., "R01").

        Returns
        -------
        int
            Channel number for this satellite.

        """
        slot = int(sv[1:3])
        return self.glonass_slots_channels[slot]

    @property
    def glonass_slots_channels(self) -> dict[int, int]:
        """Parse GLONASS channel file.

        Returns
        -------
        dict
            Mapping slot → channel.

        """
        slot_channel_dict: dict[int, int] = {}
        with self.pth.open() as file:
            lines = file.readlines()
            for i in range(len(lines)):
                if "slot" in lines[i] and "Channel" in lines[i + 1]:
                    slots_line = lines[i].strip().split("|")[1:-1]
                    channels_line = lines[i + 1].strip().split("|")[1:-1]
                    for slot, channel in zip(slots_line, channels_line, strict=False):
                        if (
                            slot.strip().isdigit()
                            and channel.strip().lstrip("-").isdigit()
                        ):
                            slot_channel_dict[int(slot.strip())] = int(channel.strip())
        return slot_channel_dict

    def band_G1_equation(self, sv: str) -> pint.Quantity:
        """Compute L1 frequency for a given SV."""
        return ((1602 + self.get_channel_used_by_SV(sv) * 9 / 16) * UREG.MHz).to(
            FREQ_UNIT
        )

    def band_G2_equation(self, sv: str) -> pint.Quantity:
        """Compute L2 frequency for a given SV."""
        return ((1246 + self.get_channel_used_by_SV(sv) * 7 / 16) * UREG.MHz).to(
            FREQ_UNIT
        )

    def freqs_G1_G2_lut(self) -> dict[str, pint.Quantity]:
        """Build the FDMA-dependent L1/L2 frequency LUT.

        Returns
        -------
        dict
            SV|obs_code → frequency for FDMA-dependent L1/L2 bands.

        """
        out: dict[str, pint.Quantity] = {}
        for band in self.SV_DEPENDENT_BANDS:
            for sv in self.svs:
                freq = (
                    self.band_G1_equation(sv)
                    if band.startswith("*1")
                    else self.band_G2_equation(sv)
                )
                out[f"{sv}|{band}"] = freq
        return out

    @property
    def freqs_lut(self) -> dict[str, pint.Quantity]:
        """Build mapping of SV|obs_code to frequency including FDMA channels.

        Returns
        -------
        dict
            Keys of format "SV|*1C" mapped to frequencies, including
            FDMA-dependent L1/L2.

        """
        out = {
            f"{sv}|{obs}": freq
            for obs, freq in self.bands_freqs.items()
            for sv in self.svs
        }

        if not self.aggregate_fdma:
            out.update(self.freqs_G1_G2_lut())

        out.update({f"{sv}|X1": self.x1["X1"] for sv in self.svs})
        return {k: out[k] for k in natsorted(out.keys())}


# ================================================================
# ----------- 2. Satellite-based Augmentation Systems  -----------
# ================================================================


class SBAS(ConstellationBase):
    """SBAS constellation model (WAAS, EGNOS, GAGAN, MSAS, SDCM).

    - Band numbers and codes from RINEX v3.04 Guide:
      http://acc.igs.org/misc/rinex304.pdf (Table 7).
    - L5 frequency and bandwidth from GPS L5 ICD:
      https://www.gps.gov/technical/icwg/IS-GPS-705J.pdf (3.3.1.1 Frequency
      Plan).
    - L1 frequency and bandwidth from GPS L1/L2 ICD:
      https://www.gps.gov/technical/icwg/IS-GPS-200N.pdf (3.3.1.1 Frequency
      Plan).

    Notes
    -----
    Uses a static list S01-S36 as PRNs are region-specific.

    """

    BANDS: ClassVar[dict[str, str]] = {"1": "L1", "5": "L5"}
    BAND_CODES: ClassVar[dict[str, list[str]]] = {
        "L1": ["C"],
        "L5": ["I", "Q", "X"],
    }
    BAND_PROPERTIES: ClassVar[dict[str, dict[str, pint.Quantity]]] = {
        "L1": {
            "freq": 1575.42 * UREG.MHz,
            "bandwidth": 30.69 * UREG.MHz,
            "system": "S",
        },
        "L5": {"freq": 1176.45 * UREG.MHz, "bandwidth": 24.0 * UREG.MHz, "system": "S"},
    }

    def __init__(self) -> None:
        """Initialize SBAS constellation."""
        super().__init__(
            constellation="SBAS",
            url="https://en.wikipedia.org/wiki/List_of_SBAS_satellites",
            re_pattern=r"\bS\d{2}\b",
            table_index=0,
            prn_column="PRN",
            use_wiki=False,  # SBAS PRNs are region-specific
            static_svs=[f"S{x:02d}" for x in range(1, 37)],
        )

    @property
    def freqs_lut(self) -> dict[str, pint.Quantity]:
        """See base class."""
        out = {
            f"{sv}|{obs}": freq
            for obs, freq in self.bands_freqs.items()
            for sv in self.svs
        }
        out.update({f"{sv}|X1": self.x1["X1"] for sv in self.svs})
        return {k: out[k] for k in natsorted(out.keys())}


# ================================================================
# ----------- 3. Regional Navigation Satellite Systems  ----------
# ================================================================


class IRNSS(ConstellationBase):
    """IRNSS (NavIC) constellation model.

    - Band numbers and codes from RINEX v3.04 Guide:
      http://acc.igs.org/misc/rinex304.pdf (Table 10).
    - L5 frequencies and bandwidths from NavIC ICD:
      https://www.isro.gov.in/media_isro/pdf/Publications/Vispdf/Pdf2017/1a_messgingicd_receiver_incois_approved_ver_1.2.pdf
      (Table 1).
    - S band frequency and bandwidth from Navipedia:
      https://gssc.esa.int/navipedia/index.php/IRNSS_Signal_Plan#cite_note-IRNSS_ICD-2

    Notes
    -----
    This class fetches the current satellite list from Wikipedia.

    """

    BANDS: ClassVar[dict[str, str]] = {"5": "L5", "9": "S"}
    BAND_CODES: ClassVar[dict[str, list[str]]] = {
        "L5": ["A", "B", "C", "X"],
        "S": ["A", "B", "C", "X"],
    }
    BAND_PROPERTIES: ClassVar[dict[str, dict[str, pint.Quantity]]] = {
        "L5": {"freq": 1176.45 * UREG.MHz, "bandwidth": 24.0 * UREG.MHz, "system": "I"},
        "S": {"freq": 2492.028 * UREG.MHz, "bandwidth": 16.5 * UREG.MHz, "system": "I"},
    }

    def __init__(self) -> None:
        """Initialize IRNSS (NavIC) constellation."""
        super().__init__(
            constellation="IRNSS",
            url="https://en.wikipedia.org/wiki/Indian_Regional_Navigation_Satellite_System#List_of_satellites",
            re_pattern=r"\bI\d{2}\b",
            table_index=3,
            prn_column="PRN",
            status_filter={"column": "Status", "value": "Operational"},
            use_wiki=False,
            static_svs=[f"I{x:02d}" for x in range(1, 15)],  # I01-I14
        )

    @property
    def freqs_lut(self) -> dict[str, pint.Quantity]:
        """See base class."""
        out = {
            f"{sv}|{obs}": freq
            for obs, freq in self.bands_freqs.items()
            for sv in self.svs
        }
        out.update({f"{sv}|X1": self.x1["X1"] for sv in self.svs})
        return {k: out[k] for k in natsorted(out.keys())}


class QZSS(ConstellationBase):
    """QZSS constellation model (GPS-compatible + unique L6).

    - Band numbers and codes from RINEX v3.04 Guide:
      http://acc.igs.org/misc/rinex304.pdf (Table 8).
    - L1, L2, L5 frequencies and bandwidths from QZSS ICD:
      https://qzss.go.jp/en/technical/download/pdf/ps-is-qzss/is-qzss-pnt-006.pdf?t=1757949673838
      (Table 3.1.2-1).
    - L6 frequency and bandwidth from Navipedia:
      https://gssc.esa.int/navipedia/index.php?title=QZSS_Signal_Plan

    Note:
    ----
    Bandwidth technically depends on the GPS Block. Like with `GPS`, we assume
    the larger bandwidth here.

    Notes:
    -----
    Uses a static list J01-J10.

    """

    BANDS: ClassVar[dict[str, str]] = {
        "1": "L1",
        "2": "L2",
        "5": "L5",
        "6": "L6",
    }
    BAND_CODES: ClassVar[dict[str, list[str]]] = {
        "L1": ["C", "S", "L", "X", "Z"],
        "L2": ["S", "L", "X"],
        "L5": ["I", "Q", "X", "D", "P", "Z"],
        "L6": ["S", "L", "X", "E", "Z"],
    }
    BAND_PROPERTIES: ClassVar[dict[str, dict[str, pint.Quantity]]] = {
        "L1": {
            "freq": 1575.42 * UREG.MHz,
            "bandwidth": 30.69 * UREG.MHz,
            "system": "J",
        },
        "L2": {
            "freq": 1227.60 * UREG.MHz,
            "bandwidth": 30.69 * UREG.MHz,
            "system": "J",
        },
        "L5": {"freq": 1176.45 * UREG.MHz, "bandwidth": 24.0 * UREG.MHz, "system": "J"},
        "L6": {"freq": 1278.75 * UREG.MHz, "bandwidth": 42.0 * UREG.MHz, "system": "J"},
    }

    def __init__(self) -> None:
        """Initialize QZSS constellation."""
        super().__init__(
            constellation="QZSS",
            url="https://en.wikipedia.org/wiki/Quasi-Zenith_Satellite_System",
            re_pattern=r"\bJ\d{2}\b",
            table_index=2,
            prn_column="PRN",
            use_wiki=False,
            static_svs=[f"J{x:02d}" for x in range(1, 11)],
        )

    @property
    def freqs_lut(self) -> dict[str, pint.Quantity]:
        """See base class."""
        out = {
            f"{sv}|{obs}": freq
            for obs, freq in self.bands_freqs.items()
            for sv in self.svs
        }
        out.update({f"{sv}|X1": self.x1["X1"] for sv in self.svs})
        return {k: out[k] for k in natsorted(out.keys())}


if __name__ == "__main__":
    gal = GALILEO()
    # gps = GPS()
    # bds = BEIDOU()
    # irnss = IRNSS()
    # glonass = GLONASS()

    # Example usage
    print("Galileo Frequencies LUT:")
    for k, v in gal.freqs_lut.items():
        print(f"{k}: {v}")

    # print("\nGPS Frequencies LUT:")
    # for k, v in gps.freqs_lut.items():
    #     print(f"{k}: {v}")

    # print("\nBeiDou Frequencies LUT:")
    # for k, v in bds.freqs_lut.items():
    #     print(f"{k}: {v}")

    # print("\nIRNSS Frequencies LUT:")
    # for k, v in irnss.freqs_lut.items():
    #     print(f"{k}: {v}")

    # print("\nGLONASS Frequencies LUT:")
    # glonass_freqs = glonass.freqs_lut
    # for k, v in glonass_freqs.items():
    #     print(f"{k}: {v}")

    # glonass = GLONASS(aggregate_fdma=False)
    # print("\nGLONASS Frequencies LUT:")
    # glonass_freqs2 = glonass.freqs_lut
    # for k, v in glonass_freqs.items():
    #     print(f"{k}: {v}")

    # print(len(glonass_freqs), len(glonass_freqs2))
    # print(
    #     len({x.magnitude for x in glonass_freqs.values()}),
    #     len({x.magnitude for x in glonass_freqs2.values()}),
    # )

    # print({x.magnitude for x in glonass_freqs.values()})
    # print({x.magnitude for x in glonass_freqs2.values()})
