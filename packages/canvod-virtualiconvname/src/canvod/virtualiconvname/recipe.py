"""Naming recipe: user-defined mapping from arbitrary filenames to canVOD names.

A recipe is a small YAML file that describes:
1. The canonical identity of a receiver (site, agency, sampling, etc.)
2. How to extract date/time fields from the user's physical filenames.

The field extraction is a sequential left-to-right walk over the filename.
Each entry in the ``fields`` list consumes characters and either extracts
a named value or skips literal characters.

Recognized field names
----------------------
- ``year``         4-digit year (e.g. 2025)
- ``yy``           2-digit year (80-99 → 19xx, 00-79 → 20xx)
- ``doy``          day of year (1-366)
- ``month``        month (01-12)
- ``day``          day of month (01-31)
- ``hour``         hour (00-23)
- ``minute``       minute (00-59)
- ``hour_letter``  RINEX hour code (a-x, single char)
- ``skip``         ignore these characters

Example recipe (YAML)
---------------------
::

    name: rosalia_reference
    description: Rosalia forest, reference receiver

    site: ROS
    agency: TUW
    receiver_number: 1
    receiver_type: reference
    sampling: "05S"
    period: "15M"
    content: "AA"
    file_type: rnx

    layout: yyddd_subdirs
    glob: "*.??o"

    # Example: rref001a15.25o
    fields:
      - skip: 4
      - doy: 3
      - hour_letter: 1
      - minute: 2
      - skip: 1
      - yy: 2
      - skip: 1

Example recipe for exotic filenames
------------------------------------
::

    # Example: STATION_2025_042_00_15.rinex
    fields:
      - skip: 8
      - year: 4
      - skip: 1
      - doy: 3
      - skip: 1
      - hour: 2
      - skip: 1
      - minute: 2
      - skip: 6
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, model_validator

from .config_models import DirectoryLayout
from .convention import (
    AgencyId,
    CanVODFilename,
    ContentCode,
    Duration,
    FileType,
    ReceiverType,
    SiteId,
)
from .mapping import VirtualFile
from .patterns import hour_letter_to_int, resolve_year_from_yy

# Field names the parser understands
KNOWN_FIELDS = frozenset(
    {
        "year",
        "yy",
        "doy",
        "month",
        "day",
        "hour",
        "minute",
        "hour_letter",
        "skip",
    }
)


class NamingRecipe(BaseModel):
    """A user-defined mapping from arbitrary filenames to canVOD names.

    Serialize to YAML for sharing, to JSON for API transport.
    """

    name: str = Field(description="Recipe identifier (e.g. 'rosalia_reference')")
    description: str = ""

    # Canonical identity
    site: SiteId
    agency: AgencyId
    receiver_number: int = Field(ge=1, le=99)
    receiver_type: Literal["reference", "canopy"] = "canopy"
    sampling: Duration = "05S"
    period: Duration = "15M"
    content: ContentCode = "AA"
    file_type: Literal["rnx", "sbf", "ubx", "nmea"] = "rnx"

    # Directory layout
    layout: DirectoryLayout = DirectoryLayout.YYDDD_SUBDIRS

    # File discovery
    glob: str = Field(
        description="Glob pattern to find files (e.g. '*.??o', '*.rinex')"
    )

    # Field extraction: sequential left-to-right
    # Each entry is a single-key dict: {field_name: width}
    fields: list[dict[str, int]] = Field(
        description="Sequential field extraction. Each entry: {field_name: width}"
    )

    @model_validator(mode="after")
    def _validate_fields(self) -> NamingRecipe:
        for i, entry in enumerate(self.fields):
            if len(entry) != 1:
                msg = (
                    f"fields[{i}]: each entry must be a single "
                    f"key-value pair, got {entry}"
                )
                raise ValueError(msg)
            field_name = next(iter(entry))
            width = entry[field_name]
            if field_name not in KNOWN_FIELDS:
                msg = (
                    f"fields[{i}]: unknown field '{field_name}'. "
                    f"Known: {sorted(KNOWN_FIELDS)}"
                )
                raise ValueError(msg)
            if not isinstance(width, int) or width < 1:
                msg = f"fields[{i}]: width must be a positive integer, got {width}"
                raise ValueError(msg)
        return self

    # -- Parsing ---------------------------------------------------------------

    def parse_filename(self, filename: str) -> dict[str, str | int]:
        """Extract fields from a physical filename.

        Parameters
        ----------
        filename
            Bare filename (no directory components).

        Returns
        -------
        dict
            Extracted field values. Integer fields (year, doy, etc.) are
            returned as ``int``.  ``hour_letter`` is returned as ``str``.
            ``skip`` fields are not included.

        Raises
        ------
        ValueError
            If the filename is too short for the field spec.
        """
        pos = 0
        result: dict[str, str | int] = {}

        for entry in self.fields:
            field_name = next(iter(entry))
            width = entry[field_name]

            if pos + width > len(filename):
                msg = (
                    f"Filename {filename!r} too short: need {width} chars "
                    f"at position {pos} for '{field_name}', "
                    f"but only {len(filename) - pos} remain"
                )
                raise ValueError(msg)

            raw = filename[pos : pos + width]
            pos += width

            if field_name == "skip":
                continue
            elif field_name == "hour_letter":
                result["hour_letter"] = raw
            else:
                try:
                    result[field_name] = int(raw)
                except ValueError:
                    msg = (
                        f"Cannot parse '{field_name}' as integer "
                        f"from {raw!r} in {filename!r}"
                    )
                    raise ValueError(msg) from None

        return result

    def to_virtual_file(self, file_path: Path) -> VirtualFile:
        """Map a physical file to a VirtualFile using this recipe.

        Parameters
        ----------
        file_path
            Path to the physical file.

        Returns
        -------
        VirtualFile
            The mapped virtual file.

        Raises
        ------
        ValueError
            If the filename cannot be parsed.
        """
        parsed = self.parse_filename(file_path.name)

        def _require_int(parsed_key: str) -> int:
            value = parsed[parsed_key]
            if isinstance(value, int):
                return value
            msg = (
                f"Recipe '{self.name}': expected integer field '{parsed_key}' "
                f"for {file_path.name!r}, got {value!r}"
            )
            raise ValueError(msg)

        def _require_str(parsed_key: str) -> str:
            value = parsed[parsed_key]
            if isinstance(value, str):
                return value
            msg = (
                f"Recipe '{self.name}': expected string field '{parsed_key}' "
                f"for {file_path.name!r}, got {value!r}"
            )
            raise ValueError(msg)

        # Resolve year
        if "year" in parsed:
            year = _require_int("year")
        elif "yy" in parsed:
            year = resolve_year_from_yy(_require_int("yy"))
        else:
            raise ValueError(
                f"Recipe '{self.name}': no 'year' or 'yy' field "
                f"in parsed result for {file_path.name!r}"
            )

        # Resolve DOY (from doy directly, or from month+day)
        if "doy" in parsed:
            doy = _require_int("doy")
        elif "month" in parsed and "day" in parsed:
            from datetime import date

            doy = (
                date(year, _require_int("month"), _require_int("day"))
                .timetuple()
                .tm_yday
            )
        else:
            raise ValueError(
                f"Recipe '{self.name}': no 'doy' or 'month'+'day' fields "
                f"in parsed result for {file_path.name!r}"
            )

        # Resolve hour
        if "hour" in parsed:
            hour = _require_int("hour")
        elif "hour_letter" in parsed:
            hour = hour_letter_to_int(_require_str("hour_letter"))
        else:
            hour = 0

        # Resolve minute
        minute = _require_int("minute") if "minute" in parsed else 0

        # Determine period: daily if hour=0 and minute=0 and no hour field
        period = self.period
        if hour == 0 and minute == 0:
            has_hour = any("hour" in e or "hour_letter" in e for e in self.fields)
            if not has_hour:
                period = "01D"

        rx_type = (
            ReceiverType.REFERENCE
            if self.receiver_type == "reference"
            else ReceiverType.ACTIVE
        )

        conventional = CanVODFilename(
            site=self.site,
            receiver_type=rx_type,
            receiver_number=self.receiver_number,
            agency=self.agency,
            year=year,
            doy=doy,
            hour=hour,
            minute=minute,
            period=period,
            sampling=self.sampling,
            content=self.content,
            file_type=FileType(self.file_type),
        )

        return VirtualFile(physical_path=file_path, conventional_name=conventional)

    @property
    def expected_length(self) -> int:
        """Total number of characters consumed by the field spec."""
        return sum(next(iter(e.values())) for e in self.fields)

    def matches(self, filename: str) -> bool:
        """Check if a filename can be parsed by this recipe."""
        if len(filename) != self.expected_length:
            return False
        try:
            self.parse_filename(filename)
            return True
        except ValueError:
            return False

    # -- Serialization ---------------------------------------------------------

    def to_yaml(self) -> str:
        """Serialize to YAML string."""
        return yaml.dump(
            self.model_dump(mode="json"),
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )

    def save(self, path: Path) -> None:
        """Write recipe to a YAML file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.to_yaml(), encoding="utf-8")

    @classmethod
    def from_yaml(cls, text: str) -> NamingRecipe:
        """Load from a YAML string."""
        data = yaml.safe_load(text)
        return cls.model_validate(data)

    @classmethod
    def load(cls, path: Path) -> NamingRecipe:
        """Load from a YAML file."""
        text = path.read_text(encoding="utf-8")
        return cls.from_yaml(text)
