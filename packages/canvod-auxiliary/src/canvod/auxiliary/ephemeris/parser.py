"""SP3 format parser for orbit files."""

import datetime
from pathlib import Path
from typing import Any

import numpy as np
import xarray as xr
from canvod.auxiliary._internal import UREG


class Sp3Parser:
    """Parser for SP3 (Standard Product #3) orbit files.

    Handles parsing of SP3 format files containing precise satellite orbit data.
    Implements optimized single-pass reading for performance.

    Parameters
    ----------
    fpath : Path
        Path to SP3 file.
    dimensionless : bool, default True
        If True, strip units from output.
    """

    def __init__(self, fpath: Path, dimensionless: bool = True) -> None:
        """Initialize SP3 parser."""
        self.fpath = Path(fpath)
        self.dimensionless = dimensionless

    def parse(self) -> xr.Dataset:
        """Parse SP3 file to xarray Dataset.

        Returns
        -------
        xr.Dataset
            Dataset with satellite positions (X, Y, Z) in meters.

        Raises
        ------
        FileNotFoundError
            If file does not exist.
        ValueError
            If file format is invalid.
        """
        if not self.fpath.exists():
            raise FileNotFoundError(f"SP3 file not found: {self.fpath}")

        epochs: list[datetime.datetime] = []
        epoch_data: list[tuple[Any]] = []
        svs: set[str] = set()
        current_epoch_idx: int = -1

        with open(self.fpath) as f:
            # Skip header until first epoch marker
            for line in f:
                if line.startswith("*"):
                    current_epoch = self._parse_epoch_line(line)
                    epochs.append(current_epoch)
                    current_epoch_idx += 1
                    break

            # Single pass through file
            for line in f:
                if line.startswith("*"):
                    current_epoch = self._parse_epoch_line(line)
                    epochs.append(current_epoch)
                    current_epoch_idx += 1

                elif line.startswith("P"):
                    sv_code = line[1:4].strip()
                    svs.add(sv_code)

                    x = self._parse_coordinate(line[4:18])
                    y = self._parse_coordinate(line[18:32])
                    z = self._parse_coordinate(line[32:46])

                    epoch_data.append(
                        (
                            current_epoch_idx,
                            sv_code,
                            x if x is not None else np.nan,
                            y if y is not None else np.nan,
                            z if z is not None else np.nan,
                        )
                    )

                elif line.startswith("EOF"):
                    break

        # Build arrays
        sv_list = sorted(svs)
        sv_idx = {sv: i for i, sv in enumerate(sv_list)}
        shape = (len(epochs), len(sv_list))

        x_data = np.full(shape, np.nan)
        y_data = np.full(shape, np.nan)
        z_data = np.full(shape, np.nan)

        for epoch_idx, sv, x, y, z in epoch_data:
            j = sv_idx[sv]
            x_data[epoch_idx, j] = x
            y_data[epoch_idx, j] = y
            z_data[epoch_idx, j] = z

        # Convert to meters
        x_data = (x_data * UREG.kilometer).to(UREG.meter)
        y_data = (y_data * UREG.kilometer).to(UREG.meter)
        z_data = (z_data * UREG.kilometer).to(UREG.meter)

        if self.dimensionless:
            x_data = x_data.magnitude
            y_data = y_data.magnitude
            z_data = z_data.magnitude

        # Create dataset
        ds = xr.Dataset(
            data_vars={
                "X": (("epoch", "sv"), x_data),
                "Y": (("epoch", "sv"), y_data),
                "Z": (("epoch", "sv"), z_data),
            },
            coords={
                "epoch": np.array(epochs, dtype="datetime64[ns]"),
                "sv": np.array(sv_list),
            },
        )

        # Add variable attributes
        for var, attrs in self._get_variable_attributes().items():
            if var in ds:
                ds[var].attrs = attrs

        return ds

    def _parse_epoch_line(self, line: str) -> datetime.datetime:
        """Parse epoch marker line.

        Parameters
        ----------
        line : str
            Line starting with "*" containing timestamp.

        Returns
        -------
        datetime.datetime
            Parsed datetime.

        Examples
        --------
        >>> _parse_epoch_line("* 2024 1 15 0 0 0.00000000")
        datetime.datetime(2024, 1, 15, 0, 0)
        """
        parts = line.split()
        return datetime.datetime(
            year=int(parts[1]),
            month=int(parts[2]),
            day=int(parts[3]),
            hour=int(parts[4]),
            minute=int(parts[5]),
            second=0,
        )

    def _parse_coordinate(self, coord_str: str) -> float | None:
        """Parse coordinate field from SP3 file.

        Handles edge cases like missing spaces and invalid data markers.

        Parameters
        ----------
        coord_str : str
            14-character coordinate string.

        Returns
        -------
        float | None
            Coordinate value in kilometers, or None if missing.

        Raises
        ------
        ValueError
            If coordinate cannot be parsed.
        """
        coord_str = coord_str.strip()
        if not coord_str or coord_str == "999999.999999":
            return None

        try:
            return float(coord_str)
        except ValueError:
            # Handle missing spaces between fields
            if "." in coord_str:
                parts = coord_str.split(".")
                if len(parts) == 2:
                    integer_part = parts[0].replace(" ", "")
                    return float(f"{integer_part}.{parts[1]}")
            raise

    def _get_variable_attributes(self) -> dict[str, dict[str, str]]:
        """Get standardized attributes for position variables."""
        return {
            "X": {
                "long_name": "x-coordinate in ECEF",
                "standard_name": r"x_{ECEF}",
                "short_name": "x",
                "units": "m",
                "axis": "x",
                "description": "x-coordinate in ECEF (Earth-Centered, Earth-Fixed) frame",
            },
            "Y": {
                "long_name": "y-coordinate in ECEF",
                "standard_name": r"y_{ECEF}",
                "short_name": "y",
                "units": "m",
                "axis": "y",
                "description": "y-coordinate in ECEF (Earth-Centered, Earth-Fixed) frame",
            },
            "Z": {
                "long_name": "z-coordinate in ECEF",
                "standard_name": r"z_{ECEF}",
                "short_name": "z",
                "units": "m",
                "axis": "z",
                "description": "z-coordinate in ECEF (Earth-Centered, Earth-Fixed) frame",
            },
        }
