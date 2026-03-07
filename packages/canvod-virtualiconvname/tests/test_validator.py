"""Tests for DataDirectoryValidator."""

from __future__ import annotations

from pathlib import Path

import pytest

from canvod.virtualiconvname import (
    DataDirectoryValidator,
    ReceiverNamingConfig,
    SiteNamingConfig,
)

TEST_DATA = Path(__file__).parent / "test_data"


@pytest.fixture
def site_naming() -> SiteNamingConfig:
    return SiteNamingConfig(site_id="ROS", agency="TUW")


@pytest.fixture
def receiver_naming() -> ReceiverNamingConfig:
    return ReceiverNamingConfig(
        receiver_number=1,
        source_pattern="auto",
        directory_layout="flat",
    )


@pytest.fixture
def validator() -> DataDirectoryValidator:
    return DataDirectoryValidator()


def _create_file(directory: Path, name: str) -> Path:
    """Create a dummy file."""
    f = directory / name
    f.write_bytes(b"\x00")
    return f


class TestValidationPasses:
    """Validation passes for well-formed directories."""

    def test_valid_canonical_files(self, validator, site_naming, receiver_naming):
        """Canonical .rnx files in test_data/valid_names pass validation."""
        report = validator.validate_receiver(
            site_naming=site_naming,
            receiver_naming=receiver_naming,
            receiver_type="reference",
            receiver_base_dir=TEST_DATA / "valid_names",
        )
        assert report.is_valid
        assert len(report.matched) == 2

    def test_valid_rinex3_files_tmpdir(
        self, tmp_path, validator, site_naming, receiver_naming
    ):
        _create_file(tmp_path, "ROSR01TUW_R_20250010000_01D_05S_AA.rnx")
        _create_file(tmp_path, "ROSR01TUW_R_20250020000_01D_05S_AA.rnx")

        report = validator.validate_receiver(
            site_naming=site_naming,
            receiver_naming=receiver_naming,
            receiver_type="reference",
            receiver_base_dir=tmp_path,
        )
        assert report.is_valid
        assert len(report.matched) == 2

    def test_valid_rinex_v2_short_names(self, tmp_path, validator, site_naming):
        """RINEX v2 short names are discovered and mapped (non-overlapping days)."""
        # Use different DOYs to avoid overlap (a=hour 0, both are 01D)
        _create_file(tmp_path, "ract001a.25o")  # DOY 001
        _create_file(tmp_path, "ract002a.25o")  # DOY 002

        receiver_naming = ReceiverNamingConfig(
            receiver_number=1,
            source_pattern="auto",
            directory_layout="flat",
        )

        report = validator.validate_receiver(
            site_naming=site_naming,
            receiver_naming=receiver_naming,
            receiver_type="canopy",
            receiver_base_dir=tmp_path,
        )
        assert report.is_valid
        assert len(report.matched) == 2

    def test_valid_sbf_names(self, tmp_path, validator, site_naming):
        """Septentrio SBF names are discovered and mapped (non-overlapping days)."""
        _create_file(tmp_path, "ract001a00.25_")  # DOY 001
        _create_file(tmp_path, "ract002a00.25_")  # DOY 002

        receiver_naming = ReceiverNamingConfig(
            receiver_number=1,
            source_pattern="auto",
            directory_layout="flat",
        )

        report = validator.validate_receiver(
            site_naming=site_naming,
            receiver_naming=receiver_naming,
            receiver_type="canopy",
            receiver_base_dir=tmp_path,
        )
        assert report.is_valid
        assert len(report.matched) == 2

    def test_empty_directory_is_valid(
        self, tmp_path, validator, site_naming, receiver_naming
    ):
        report = validator.validate_receiver(
            site_naming=site_naming,
            receiver_naming=receiver_naming,
            receiver_type="canopy",
            receiver_base_dir=tmp_path,
        )
        assert report.is_valid
        assert len(report.matched) == 0


class TestValidationRejectsUnmatched:
    """Validation rejects directories with unmappable files."""

    def test_unmatched_files_raise(
        self, tmp_path, validator, site_naming, receiver_naming
    ):
        """Files that match globs but fail regex → unmatched error."""
        _create_file(tmp_path, "garbage.25o")

        with pytest.raises(ValueError, match="could not be mapped"):
            validator.validate_receiver(
                site_naming=site_naming,
                receiver_naming=receiver_naming,
                receiver_type="canopy",
                receiver_base_dir=tmp_path,
            )

    def test_non_conventional_names_from_test_data(
        self, validator, site_naming, receiver_naming
    ):
        """Files in test_data/invalid_names are not discoverable by globs.

        Since globs only match GNSS file extensions, truly random files
        like .txt/.csv/.dat/.jpg are never picked up — the directory
        appears empty and valid.
        """
        report = validator.validate_receiver(
            site_naming=site_naming,
            receiver_naming=receiver_naming,
            receiver_type="canopy",
            receiver_base_dir=TEST_DATA / "invalid_names",
        )
        # Non-GNSS files are invisible to globs → 0 matched, 0 unmatched → valid
        assert report.is_valid
        assert len(report.matched) == 0

    def test_mixed_valid_and_invalid_names(
        self, validator, site_naming, receiver_naming
    ):
        """Directory with valid + non-GNSS files: non-GNSS files are invisible.

        Only files matching GNSS globs are attempted. The .xyz file is
        ignored by glob discovery.
        """
        report = validator.validate_receiver(
            site_naming=site_naming,
            receiver_naming=receiver_naming,
            receiver_type="reference",
            receiver_base_dir=TEST_DATA / "valid_names" / "mixed",
        )
        # Only the .rnx file is discovered; .xyz is invisible to globs
        assert report.is_valid
        assert len(report.matched) == 1

    def test_glob_matched_but_pattern_unmatched_file_rejects(
        self, tmp_path, validator, site_naming, receiver_naming
    ):
        """A .25o file that doesn't match any naming pattern → rejected."""
        # 3-char station name is too short for RINEX v2 (needs 4 chars)
        _create_file(tmp_path, "abc001a.25o")

        with pytest.raises(ValueError, match="could not be mapped"):
            validator.validate_receiver(
                site_naming=site_naming,
                receiver_naming=receiver_naming,
                receiver_type="canopy",
                receiver_base_dir=tmp_path,
            )


class TestDuplicateDetection:
    """Validation detects duplicate canonical names as overlaps."""

    def test_duplicate_canonical_name_is_overlap(
        self, tmp_path, validator, site_naming
    ):
        """Two physical files mapping to same time range → overlap error."""
        # Same RINEX v2 name, different compression → same canonical time range
        _create_file(tmp_path, "ract001a.25o")
        _create_file(tmp_path, "ract001a.25o.gz")

        receiver_naming = ReceiverNamingConfig(
            receiver_number=1,
            source_pattern="auto",
            directory_layout="flat",
        )

        with pytest.raises(ValueError, match="overlap"):
            validator.validate_receiver(
                site_naming=site_naming,
                receiver_naming=receiver_naming,
                receiver_type="canopy",
                receiver_base_dir=tmp_path,
            )


class TestOverlapDetection:
    """Validation detects temporal overlaps."""

    def test_daily_plus_subdaily_overlap(self, tmp_path, validator, site_naming):
        """01D file alongside 15M files for the same day."""
        _create_file(tmp_path, "ROSR01TUW_R_20250010000_01D_05S_AA.rnx")
        _create_file(tmp_path, "ROSR01TUW_R_20250010000_15M_05S_AA.rnx")
        _create_file(tmp_path, "ROSR01TUW_R_20250010015_15M_05S_AA.rnx")

        receiver_naming = ReceiverNamingConfig(
            receiver_number=1,
            source_pattern="auto",
            directory_layout="flat",
        )

        with pytest.raises(ValueError, match="overlap"):
            validator.validate_receiver(
                site_naming=site_naming,
                receiver_naming=receiver_naming,
                receiver_type="reference",
                receiver_base_dir=tmp_path,
            )

    def test_non_overlapping_subdaily_passes(self, tmp_path, validator, site_naming):
        """Non-overlapping 15M files pass validation."""
        _create_file(tmp_path, "ROSR01TUW_R_20250010000_15M_05S_AA.rnx")
        _create_file(tmp_path, "ROSR01TUW_R_20250010015_15M_05S_AA.rnx")
        _create_file(tmp_path, "ROSR01TUW_R_20250010030_15M_05S_AA.rnx")
        _create_file(tmp_path, "ROSR01TUW_R_20250010045_15M_05S_AA.rnx")

        receiver_naming = ReceiverNamingConfig(
            receiver_number=1,
            source_pattern="auto",
            directory_layout="flat",
        )

        report = validator.validate_receiver(
            site_naming=site_naming,
            receiver_naming=receiver_naming,
            receiver_type="reference",
            receiver_base_dir=tmp_path,
        )
        assert report.is_valid
        assert len(report.matched) == 4
