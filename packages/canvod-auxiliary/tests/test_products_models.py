"""Tests for canvod.auxiliary.products.models Pydantic validation models."""

import pytest
from pydantic import ValidationError

from canvod.auxiliary.products.models import (
    ClkHeader,
    FileValidationResult,
    Sp3Header,
)

# ---------------------------------------------------------------------------
# Sp3Header
# ---------------------------------------------------------------------------


class TestSp3Header:
    """Tests for Sp3Header validation model."""

    def _valid_kwargs(self) -> dict:
        return {
            "version": "#d",
            "epoch_count": 96,
            "data_used": "ORBIT",
            "coordinate_system": "IGS20",
            "orbit_type": "HLM",
            "agency": "COD",
            "gps_week": 2345,
            "seconds_of_week": 0.0,
            "epoch_interval": 900.0,
            "mjd_start": 60611,
            "fractional_day": 0.0,
            "num_satellites": 85,
        }

    def test_valid_header(self):
        h = Sp3Header(**self._valid_kwargs())
        assert h.version == "#d"
        assert h.epoch_count == 96

    def test_invalid_version(self):
        kw = self._valid_kwargs()
        kw["version"] = "v1"
        with pytest.raises(ValidationError):
            Sp3Header(**kw)

    def test_version_d(self):
        kw = self._valid_kwargs()
        kw["version"] = "#d"
        assert Sp3Header(**kw).version == "#d"

    def test_version_p(self):
        kw = self._valid_kwargs()
        kw["version"] = "#P"
        assert Sp3Header(**kw).version == "#P"

    def test_invalid_coordinate_system(self):
        kw = self._valid_kwargs()
        kw["coordinate_system"] = "XYZ123"
        with pytest.raises(ValidationError):
            Sp3Header(**kw)

    def test_epoch_count_must_be_positive(self):
        kw = self._valid_kwargs()
        kw["epoch_count"] = 0
        with pytest.raises(ValidationError):
            Sp3Header(**kw)

    def test_epoch_interval_must_be_positive(self):
        kw = self._valid_kwargs()
        kw["epoch_interval"] = 0
        with pytest.raises(ValidationError):
            Sp3Header(**kw)

    def test_gps_week_non_negative(self):
        kw = self._valid_kwargs()
        kw["gps_week"] = -1
        with pytest.raises(ValidationError):
            Sp3Header(**kw)

    def test_seconds_of_week_range(self):
        kw = self._valid_kwargs()
        kw["seconds_of_week"] = 604800  # Exactly 7 days — out of range
        with pytest.raises(ValidationError):
            Sp3Header(**kw)

    def test_num_satellites_max(self):
        kw = self._valid_kwargs()
        kw["num_satellites"] = 201
        with pytest.raises(ValidationError):
            Sp3Header(**kw)

    def test_agency_min_length(self):
        kw = self._valid_kwargs()
        kw["agency"] = "AB"
        with pytest.raises(ValidationError):
            Sp3Header(**kw)


# ---------------------------------------------------------------------------
# ClkHeader
# ---------------------------------------------------------------------------


class TestClkHeader:
    """Tests for ClkHeader validation model."""

    def _valid_kwargs(self) -> dict:
        return {
            "version": "3.04",
            "file_type": "C",
            "time_system": "GPS",
            "leap_seconds": 18,
            "agency": "COD",
            "num_solution_stations": 300,
            "num_solution_satellites": 85,
            "analysis_center": "CODE, Bern",
        }

    def test_valid_header(self):
        h = ClkHeader(**self._valid_kwargs())
        assert h.file_type == "C"

    def test_invalid_file_type(self):
        kw = self._valid_kwargs()
        kw["file_type"] = "O"
        with pytest.raises(ValidationError):
            ClkHeader(**kw)

    def test_invalid_time_system(self):
        kw = self._valid_kwargs()
        kw["time_system"] = "UTC"
        with pytest.raises(ValidationError):
            ClkHeader(**kw)

    def test_valid_time_systems(self):
        for ts in ["GPS", "GLO", "GAL", "BDS", "QZSS", "IRNSS"]:
            kw = self._valid_kwargs()
            kw["time_system"] = ts
            h = ClkHeader(**kw)
            assert h.time_system == ts

    def test_leap_seconds_non_negative(self):
        kw = self._valid_kwargs()
        kw["leap_seconds"] = -1
        with pytest.raises(ValidationError):
            ClkHeader(**kw)

    def test_num_solution_satellites_positive(self):
        kw = self._valid_kwargs()
        kw["num_solution_satellites"] = 0
        with pytest.raises(ValidationError):
            ClkHeader(**kw)


# ---------------------------------------------------------------------------
# FileValidationResult
# ---------------------------------------------------------------------------


class TestFileValidationResultExtended:
    """Extended tests for FileValidationResult."""

    def test_initially_valid(self, tmp_path):
        result = FileValidationResult(
            is_valid=True,
            file_path=tmp_path / "test.sp3",
            file_type="SP3",
        )
        assert result.is_valid is True

    def test_add_error_makes_invalid(self, tmp_path):
        result = FileValidationResult(
            is_valid=True,
            file_path=tmp_path / "test.sp3",
            file_type="SP3",
        )
        result.add_error("Missing X variable")
        assert result.is_valid is False
        assert len(result.errors) == 1

    def test_multiple_errors(self, tmp_path):
        result = FileValidationResult(
            is_valid=True,
            file_path=tmp_path / "test.sp3",
            file_type="SP3",
        )
        result.add_error("error 1")
        result.add_error("error 2")
        assert len(result.errors) == 2

    def test_warnings_dont_invalidate(self, tmp_path):
        result = FileValidationResult(
            is_valid=True,
            file_path=tmp_path / "test.sp3",
            file_type="SP3",
        )
        result.add_warning("minor issue")
        assert result.is_valid is True
        assert len(result.warnings) == 1

    def test_summary_includes_all_info(self, tmp_path):
        result = FileValidationResult(
            is_valid=False,
            errors=["err1", "err2"],
            warnings=["warn1"],
            file_path=tmp_path / "test.clk",
            file_type="CLK",
        )
        summary = result.summary()
        assert "INVALID" in summary
        assert "err1" in summary
        assert "err2" in summary
        assert "warn1" in summary
        assert "CLK" in summary

    def test_clk_file_type(self, tmp_path):
        result = FileValidationResult(
            is_valid=True,
            file_path=tmp_path / "test.clk",
            file_type="CLK",
        )
        assert result.file_type == "CLK"
