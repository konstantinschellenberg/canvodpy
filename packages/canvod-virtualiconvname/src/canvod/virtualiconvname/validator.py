"""Pre-pipeline validation of data directories against naming convention.

The ``DataDirectoryValidator`` ensures every file entering the pipeline can be
mapped to a ``CanVODFilename``.  Validation is a hard gate: if any files are
unmatched or temporal overlaps exist, processing is blocked with a clear
diagnostic message.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from .config_models import ReceiverNamingConfig, SiteNamingConfig
from .convention import FileType
from .mapping import FilenameMapper, VirtualFile

# Map reader_format config values to accepted FileType(s)
_READER_FORMAT_FILETYPES: dict[str, set[FileType]] = {
    "rinex3": {FileType.RNX},
    "rinex": {FileType.RNX},
    "sbf": {FileType.SBF},
}


@dataclass
class ValidationReport:
    """Result of validating a receiver's data directory."""

    matched: list[VirtualFile] = field(default_factory=list)
    unmatched: list[Path] = field(default_factory=list)
    overlaps: list[tuple[VirtualFile, VirtualFile]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    skipped_format: list[VirtualFile] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        """True if no blocking issues found."""
        return not self.unmatched and not self.overlaps


class DataDirectoryValidator:
    """Pre-pipeline validation of data directories against naming convention."""

    def validate_receiver(
        self,
        site_naming: SiteNamingConfig,
        receiver_naming: ReceiverNamingConfig,
        receiver_type: Literal["reference", "canopy"],
        receiver_base_dir: Path,
        reader_format: str | None = None,
    ) -> ValidationReport:
        """Validate all files in a receiver directory.

        Parameters
        ----------
        site_naming
            Site-level naming config.
        receiver_naming
            Receiver-level naming config.
        receiver_type
            ``"reference"`` or ``"canopy"``.
        receiver_base_dir
            Absolute path to the receiver's data directory.
        reader_format
            If set (e.g. ``"rinex3"``, ``"sbf"``), only validate files
            matching that format.  Files of other formats are skipped
            (reported in ``skipped_format``).  ``"auto"`` or ``None``
            validates all formats.

        Returns
        -------
        ValidationReport
            Validation results.

        Raises
        ------
        ValueError
            If validation fails (unmatched files or overlaps detected).
        """
        mapper = FilenameMapper(
            site_naming=site_naming,
            receiver_naming=receiver_naming,
            receiver_type=receiver_type,
            receiver_base_dir=receiver_base_dir,
        )

        report = ValidationReport()

        # Discover all physical files
        all_physical = mapper._discover_files()

        # Determine which file types to accept
        accepted_types: set[FileType] | None = None
        if reader_format and reader_format != "auto":
            accepted_types = _READER_FORMAT_FILETYPES.get(reader_format)

        # Try to map each file
        for path in all_physical:
            try:
                vf = mapper.map_single_file(path)
            except ValueError, KeyError:
                report.unmatched.append(path)
                continue

            # Filter by reader_format
            if accepted_types and vf.conventional_name.file_type not in accepted_types:
                report.skipped_format.append(vf)
                continue

            report.matched.append(vf)

        # Check for duplicate canonical names
        seen_names: dict[str, VirtualFile] = {}
        for vf in report.matched:
            name = vf.canonical_str
            if name in seen_names:
                report.warnings.append(
                    f"Duplicate canonical name '{name}': "
                    f"{seen_names[name].physical_path} and {vf.physical_path}"
                )
            else:
                seen_names[name] = vf

        # Detect temporal overlaps
        report.overlaps = FilenameMapper.detect_overlaps(report.matched)

        if not report.is_valid:
            raise ValueError(_format_validation_error(report, receiver_base_dir))

        return report


def _format_validation_error(report: ValidationReport, base_dir: Path) -> str:
    """Format a clear diagnostic message for validation failures."""
    lines = [f"Data directory validation failed for {base_dir}:"]

    if report.unmatched:
        lines.append(
            f"\n  {len(report.unmatched)} file(s) could not be mapped"
            " to canonical names:"
        )
        for p in report.unmatched[:20]:
            lines.append(f"    - {p.name}")
        if len(report.unmatched) > 20:
            lines.append(f"    ... and {len(report.unmatched) - 20} more")

    if report.overlaps:
        lines.append(f"\n  {len(report.overlaps)} temporal overlap(s) detected:")
        for vf_a, vf_b in report.overlaps[:10]:
            lines.append(
                f"    - {vf_a.canonical_str} ({vf_a.physical_path.name}) "
                f"overlaps {vf_b.canonical_str} ({vf_b.physical_path.name})"
            )
        if len(report.overlaps) > 10:
            lines.append(f"    ... and {len(report.overlaps) - 10} more")

    return "\n".join(lines)
