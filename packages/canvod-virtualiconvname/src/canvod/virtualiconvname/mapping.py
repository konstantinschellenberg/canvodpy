"""Virtual renaming engine: map physical files to canVOD conventional names.

The ``FilenameMapper`` discovers files on disk according to the configured
directory layout and source pattern, then wraps each in a ``VirtualFile``
that pairs the physical path with its canVOD conventional name.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from natsort import natsorted

from .config_models import DirectoryLayout, ReceiverNamingConfig, SiteNamingConfig
from .convention import CanVODFilename, FileType, ReceiverType
from .patterns import (
    BUILTIN_PATTERNS,
    auto_match_order,
    hour_letter_to_int,
    match_pattern,
    resolve_year_from_yy,
)


@dataclass(frozen=True)
class VirtualFile:
    """Physical file mapped to its canVOD conventional name."""

    physical_path: Path
    conventional_name: CanVODFilename

    @property
    def canonical_str(self) -> str:
        """The conventional filename as a string."""
        return self.conventional_name.name

    def open(self, mode: str = "rb"):
        """Open the physical file."""
        return self.physical_path.open(mode)


# -- Extension → FileType mapping --------------------------------------------

_EXT_TO_FILETYPE: dict[str, FileType] = {
    ".rnx": FileType.RNX,
    ".crx": FileType.RNX,
    ".sbf": FileType.SBF,
    ".ubx": FileType.UBX,
    ".nmea": FileType.NMEA,
    # RINEX v2 observation types
    ".obs": FileType.RNX,
}

# RINEX v2 type characters that indicate observation files
_RNX2_OBS_CHARS = set("oOdD")

_COMPRESSION_EXTS = {".zip", ".gz", ".bz2", ".zst", ".Z"}


def _detect_file_type(path: Path) -> tuple[FileType, str | None]:
    """Detect FileType and compression from a file path.

    Returns
    -------
    tuple[FileType, str | None]
        (file_type, compression_ext_without_dot_or_None)
    """
    suffixes = path.suffixes
    compression = None

    if suffixes and suffixes[-1] in _COMPRESSION_EXTS:
        compression = suffixes[-1].lstrip(".")
        suffixes = suffixes[:-1]

    if not suffixes:
        # Septentrio SBF: file ends with .YY_ (e.g. .25_)
        stem = path.stem if compression else path.name
        if stem.endswith("_"):
            return FileType.SBF, compression
        raise ValueError(f"Cannot detect file type for {path.name}")

    ext = suffixes[-1].lower()

    # Direct extension match
    if ext in _EXT_TO_FILETYPE:
        return _EXT_TO_FILETYPE[ext], compression

    # RINEX v2: .YYo, .YYd, etc. — last char indicates type
    if len(ext) == 4 and ext[1:3].isdigit():
        type_char = ext[3]
        if type_char in _RNX2_OBS_CHARS:
            return FileType.RNX, compression

    # Septentrio SBF: .YY_
    if ext.endswith("_") and len(ext) == 4 and ext[1:3].isdigit():
        return FileType.SBF, compression

    raise ValueError(f"Cannot detect file type for {path.name}")


class FilenameMapper:
    """Maps physical files to canVOD conventional names.

    Parameters
    ----------
    site_naming
        Site-level naming config.
    receiver_naming
        Receiver-level naming config.
    receiver_type
        Whether this receiver is ``"reference"`` or ``"canopy"``.
    receiver_base_dir
        Absolute path to the receiver's data directory.
    """

    def __init__(
        self,
        site_naming: SiteNamingConfig,
        receiver_naming: ReceiverNamingConfig,
        receiver_type: Literal["reference", "canopy"],
        receiver_base_dir: Path,
    ) -> None:
        self.site_naming = site_naming
        self.receiver_naming = receiver_naming
        self.receiver_type_str = receiver_type
        self.receiver_base_dir = receiver_base_dir

        self._rx_type = (
            ReceiverType.REFERENCE
            if receiver_type == "reference"
            else ReceiverType.ACTIVE
        )

    def discover_all(self) -> list[VirtualFile]:
        """Discover all files and map them to conventional names."""
        files = self._discover_files()
        results = []
        for path in natsorted(files, key=lambda p: p.name):
            try:
                vf = self.map_single_file(path)
                results.append(vf)
            except (ValueError, KeyError):
                continue
        return results

    def discover_for_date(self, year: int, doy: int) -> list[VirtualFile]:
        """Discover files for a specific date."""
        layout = self.receiver_naming.directory_layout

        if layout == DirectoryLayout.YYDDD_SUBDIRS:
            yy = year % 100
            dir_name = f"{yy:02d}{doy:03d}"
            search_dir = self.receiver_base_dir / dir_name
        elif layout == DirectoryLayout.YYYYDDD_SUBDIRS:
            dir_name = f"{year:04d}{doy:03d}"
            search_dir = self.receiver_base_dir / dir_name
        else:
            # FLAT: discover all and filter
            all_files = self.discover_all()
            return [
                vf
                for vf in all_files
                if vf.conventional_name.year == year and vf.conventional_name.doy == doy
            ]

        if not search_dir.is_dir():
            return []

        files = self._glob_in_dir(search_dir)
        results = []
        for path in natsorted(files, key=lambda p: p.name):
            try:
                vf = self.map_single_file(path, year=year, doy=doy)
                results.append(vf)
            except (ValueError, KeyError):
                continue
        return results

    def map_single_file(
        self, file_path: Path, *, year: int | None = None, doy: int | None = None
    ) -> VirtualFile:
        """Map a single physical file to its canVOD conventional name.

        Parameters
        ----------
        file_path
            Path to the physical file.
        year, doy
            Optional date override (e.g. from directory name).

        Raises
        ------
        ValueError
            If the file cannot be matched or mapped.
        """
        filename = file_path.name
        pattern_name = self.receiver_naming.source_pattern

        result = match_pattern(filename, pattern_name)
        if result is None:
            raise ValueError(f"No pattern matched for {filename!r}")

        pat, m = result
        groups = m.groupdict()

        # Extract year
        if year is None:
            if "year" in groups and groups["year"] is not None:
                year = int(groups["year"])
            elif "yy" in groups and groups["yy"] is not None:
                year = resolve_year_from_yy(int(groups["yy"]))
            else:
                raise ValueError(f"Cannot determine year from {filename!r}")

        # Extract DOY
        if doy is None:
            if "doy" in groups and groups["doy"] is not None:
                doy = int(groups["doy"])
            else:
                raise ValueError(f"Cannot determine DOY from {filename!r}")

        # Extract hour
        if "hour" in groups and groups["hour"] is not None:
            hour = int(groups["hour"])
        elif "hour_letter" in groups and groups["hour_letter"] is not None:
            hour = hour_letter_to_int(groups["hour_letter"])
        else:
            hour = 0

        # Extract minute
        if "minute" in groups and groups["minute"] is not None:
            minute = int(groups["minute"])
        else:
            minute = 0

        # Sampling and period from regex or config defaults
        sampling = (
            groups.get("sampling")
            or self.receiver_naming.sampling
            or self.site_naming.default_sampling
        )
        period = (
            groups.get("period")
            or self.receiver_naming.period
            or self.site_naming.default_period
        )

        # Content from config
        content = self.receiver_naming.content or self.site_naming.default_content

        # Agency from config
        agency = self.receiver_naming.agency or self.site_naming.agency

        # File type and compression from extension
        file_type, compression = _detect_file_type(file_path)

        conventional = CanVODFilename(
            site=self.site_naming.site_id,
            receiver_type=self._rx_type,
            receiver_number=self.receiver_naming.receiver_number,
            agency=agency,
            year=year,
            doy=doy,
            hour=hour,
            minute=minute,
            period=period,
            sampling=sampling,
            content=content,
            file_type=file_type,
            compression=compression,
        )

        return VirtualFile(physical_path=file_path, conventional_name=conventional)

    @staticmethod
    def detect_overlaps(
        vfs: list[VirtualFile],
    ) -> list[tuple[VirtualFile, VirtualFile]]:
        """Detect temporal overlaps among virtual files.

        Groups files by ``(year, doy)`` and checks whether any file's time
        range contains or overlaps another's.  A ``01D`` file alongside
        ``15M`` files for the same day is the canonical overlap case.

        Returns
        -------
        list[tuple[VirtualFile, VirtualFile]]
            Pairs of overlapping files.
        """
        from collections import defaultdict

        by_date: dict[tuple[int, int], list[VirtualFile]] = defaultdict(list)
        for vf in vfs:
            cn = vf.conventional_name
            by_date[(cn.year, cn.doy)].append(vf)

        overlaps: list[tuple[VirtualFile, VirtualFile]] = []
        for group in by_date.values():
            if len(group) < 2:
                continue
            # Compute (start_minutes, end_minutes) for each file
            ranges: list[tuple[int, int, VirtualFile]] = []
            for vf in group:
                cn = vf.conventional_name
                start_min = cn.hour * 60 + cn.minute
                duration_sec = int(cn.batch_duration.total_seconds())
                end_min = start_min + duration_sec // 60
                ranges.append((start_min, end_min, vf))

            # O(n^2) pairwise check — fine for <100 files per day
            for i in range(len(ranges)):
                for j in range(i + 1, len(ranges)):
                    s_i, e_i, vf_i = ranges[i]
                    s_j, e_j, vf_j = ranges[j]
                    # Overlap if intervals intersect
                    if s_i < e_j and s_j < e_i:
                        overlaps.append((vf_i, vf_j))
        return overlaps

    # -- Private helpers ------------------------------------------------------

    def _discover_files(self) -> list[Path]:
        """Discover all data files according to directory layout."""
        layout = self.receiver_naming.directory_layout

        if layout == DirectoryLayout.FLAT:
            return self._glob_in_dir(self.receiver_base_dir)

        # Subdirectory layouts
        if not self.receiver_base_dir.is_dir():
            return []

        if layout == DirectoryLayout.YYDDD_SUBDIRS:
            dir_pattern = re.compile(r"^\d{5}$")
        else:
            dir_pattern = re.compile(r"^\d{7}$")

        files: list[Path] = []
        for subdir in sorted(self.receiver_base_dir.iterdir()):
            if subdir.is_dir() and dir_pattern.match(subdir.name):
                files.extend(self._glob_in_dir(subdir))
        return files

    def _glob_in_dir(self, directory: Path) -> list[Path]:
        """Glob for data files in a directory using the source pattern's globs."""
        pattern_name = self.receiver_naming.source_pattern

        if pattern_name == "auto":
            globs: set[str] = set()
            for name in auto_match_order():
                globs.update(BUILTIN_PATTERNS[name].file_globs)
        else:
            globs = set(BUILTIN_PATTERNS[pattern_name].file_globs)

        files: list[Path] = []
        seen: set[Path] = set()
        for g in sorted(globs):
            for path in directory.glob(g):
                if path.is_file() and path not in seen:
                    seen.add(path)
                    files.append(path)
        return files
