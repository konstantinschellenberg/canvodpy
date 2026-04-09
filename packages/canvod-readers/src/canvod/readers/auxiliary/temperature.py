"""Temperature readers for GNSS receiver auxiliary data.

Reads receiver-internal temperature from files produced alongside RINEX
observations (e.g. by ``teqc ++rx_state``).  The parsed time series is
returned as an :class:`xarray.Dataset` with a single ``T_receiver``
variable on the ``epoch`` dimension, ready to be merged into a RINEX
Icechunk store.

Supported formats
-----------------
- **binex** (default): ``teqc`` ``++rx_state`` output
  (``T= XX.XX C`` lines alongside datetime stamps).
- **csv**: Generic CSV with ``timestamp`` and ``temperature_C`` columns.

Usage
-----
>>> reader = TemperatureReader.create("binex", Path("SEPT315a.21.temperature"))
>>> ds = reader.read()
>>> ds
<xarray.Dataset>
Dimensions:      (epoch: 5760)
Coordinates:
  * epoch        (epoch) datetime64[ns]
Data variables:
    T_receiver   (epoch) float32 44.0 44.0 ...
"""

from __future__ import annotations

import abc
import csv
import logging
import re
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd
import xarray as xr

logger = logging.getLogger(__name__)

# ── ABC ─────────────────────────────────────────────────────────────────


class TemperatureReader(abc.ABC):
    """Abstract base class for receiver temperature readers."""

    def __init__(self, file_path: Path) -> None:
        self.file_path = Path(file_path)

    @abc.abstractmethod
    def read(self) -> xr.Dataset:
        """Parse the temperature file and return a Dataset.

        Returns
        -------
        xr.Dataset
            Dataset with dimension ``epoch`` (datetime64[ns]) and data
            variable ``T_receiver`` (float32, degrees Celsius).
        """

    @staticmethod
    def create(
        fmt: Literal["binex", "csv"],
        file_path: Path,
    ) -> TemperatureReader:
        """Factory: instantiate the correct reader for *fmt*."""
        readers: dict[str, type[TemperatureReader]] = {
            "binex": BinexTemperatureReader,
            "csv": CsvTemperatureReader,
        }
        if fmt not in readers:
            raise ValueError(
                f"Unknown temperature format {fmt!r}. Choose from: {sorted(readers)}"
            )
        return readers[fmt](file_path)


# ── Binex / teqc rx_state format ────────────────────────────────────────

# Pattern: "2021 Nov 11 00:00:00.000  T=  44.00 C"
_BINEX_RE = re.compile(
    r"^(\d{4}\s+\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\.\d{3})\s+T=\s*([\d.+-]+)\s*C"
)


class BinexTemperatureReader(TemperatureReader):
    """Read ``teqc ++rx_state`` temperature files (``.temperature``).

    Expected line format::

        2021 Nov 11 00:00:15.000  T=  44.00 C  Vpe=  11.95 V
    """

    def read(self) -> xr.Dataset:
        if not self.file_path.exists():
            raise FileNotFoundError(self.file_path)

        epochs: list[np.datetime64] = []
        temps: list[float] = []

        with self.file_path.open("r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                m = _BINEX_RE.match(line.strip())
                if m is None:
                    continue
                dt = pd.Timestamp(m.group(1))
                epochs.append(np.datetime64(dt, "ns"))
                temps.append(float(m.group(2)))

        if not epochs:
            logger.warning("No temperature records found in %s", self.file_path)
            return xr.Dataset(
                {"T_receiver": ("epoch", np.array([], dtype=np.float32))},
                coords={"epoch": np.array([], dtype="datetime64[ns]")},
            )

        epoch_arr = np.array(epochs, dtype="datetime64[ns]")
        temp_arr = np.array(temps, dtype=np.float32)

        logger.info(
            "Read %d temperature records from %s (%.1f–%.1f C)",
            len(temps),
            self.file_path.name,
            float(np.nanmin(temp_arr)),
            float(np.nanmax(temp_arr)),
        )

        return xr.Dataset(
            {"T_receiver": ("epoch", temp_arr)},
            coords={"epoch": epoch_arr},
        )


# ── Generic CSV format ──────────────────────────────────────────────────


class CsvTemperatureReader(TemperatureReader):
    """Read temperature from a CSV with ``timestamp`` and ``temperature_C`` columns."""

    def read(self) -> xr.Dataset:
        if not self.file_path.exists():
            raise FileNotFoundError(self.file_path)

        epochs: list[np.datetime64] = []
        temps: list[float] = []

        with self.file_path.open("r", encoding="utf-8", errors="replace") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                dt = pd.Timestamp(row["timestamp"])
                epochs.append(np.datetime64(dt, "ns"))
                temps.append(float(row["temperature_C"]))

        epoch_arr = np.array(epochs, dtype="datetime64[ns]")
        temp_arr = np.array(temps, dtype=np.float32)

        logger.info(
            "Read %d temperature records from %s",
            len(temps),
            self.file_path.name,
        )

        return xr.Dataset(
            {"T_receiver": ("epoch", temp_arr)},
            coords={"epoch": epoch_arr},
        )


# ── Helper: find temperature file alongside a RINEX file ───────────────


def find_temperature_file(rinex_path: Path) -> Path | None:
    """Locate the ``.temperature`` file alongside a RINEX file.

    Searches in the same directory for ``<stem>.temperature`` where
    ``<stem>`` is derived by stripping the RINEX extension.  For example,
    ``SEPT315a.21.obs`` → ``SEPT315a.21.temperature``.
    """
    parent = rinex_path.parent
    # Strip the final extension (.obs, .rnx, .21o, etc.)
    stem = rinex_path.stem  # e.g. "SEPT315a.21" from "SEPT315a.21.obs"
    candidate = parent / f"{stem}.temperature"
    if candidate.exists():
        return candidate
    # Also try with full name + .temperature (e.g. "SEPT315a.21o.temperature")
    candidate2 = parent / f"{rinex_path.name}.temperature"
    if candidate2.exists():
        return candidate2
    return None
