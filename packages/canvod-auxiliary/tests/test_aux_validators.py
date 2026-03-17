"""Tests for auxiliary data validators (clock and ephemeris)."""

import numpy as np
import pytest
import xarray as xr

from canvod.auxiliary.clock.validator import (
    check_clk_data_quality,
    validate_clk_dataset,
)
from canvod.auxiliary.ephemeris.validator import Sp3Validator
from canvod.auxiliary.products.models import FileValidationResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_clock_ds(
    n_epochs: int = 10,
    n_svs: int = 5,
    nan_fraction: float = 0.0,
    monotonic: bool = True,
) -> xr.Dataset:
    """Create a synthetic clock dataset."""
    rng = np.random.default_rng(42)
    if monotonic:
        epochs = np.array(
            [
                np.datetime64("2024-10-29T00:00:00") + np.timedelta64(i * 30, "s")
                for i in range(n_epochs)
            ]
        )
    else:
        epochs = np.array(
            [
                np.datetime64("2024-10-29T00:00:00") + np.timedelta64(i * 30, "s")
                for i in range(n_epochs)
            ]
        )
        # Shuffle to break monotonicity
        rng.shuffle(epochs)

    svs = [f"G{i:02d}" for i in range(1, n_svs + 1)]
    data = rng.uniform(-1e-3, 1e-3, (n_epochs, n_svs))

    if nan_fraction > 0:
        mask = rng.random((n_epochs, n_svs)) < nan_fraction
        data[mask] = np.nan

    return xr.Dataset(
        {"clock_offset": (["epoch", "sv"], data)},
        coords={"epoch": epochs, "sv": svs},
    )


def _make_sp3_ds(
    n_epochs: int = 10,
    n_svs: int = 5,
    nan_fraction: float = 0.0,
    include_xyz: bool = True,
) -> xr.Dataset:
    """Create a synthetic SP3 dataset."""
    rng = np.random.default_rng(42)
    epochs = np.array(
        [
            np.datetime64("2024-10-29T00:00:00") + np.timedelta64(i * 900, "s")
            for i in range(n_epochs)
        ]
    )
    svs = [f"G{i:02d}" for i in range(1, n_svs + 1)]

    ds_vars = {}
    if include_xyz:
        for var in ["X", "Y", "Z"]:
            data = rng.uniform(1e7, 3e7, (n_epochs, n_svs))
            if nan_fraction > 0:
                mask = rng.random((n_epochs, n_svs)) < nan_fraction
                data[mask] = np.nan
            ds_vars[var] = (["epoch", "sv"], data)

    return xr.Dataset(ds_vars, coords={"epoch": epochs, "sv": svs})


# ---------------------------------------------------------------------------
# validate_clk_dataset
# ---------------------------------------------------------------------------


class TestValidateClkDataset:
    """Tests for validate_clk_dataset()."""

    def test_valid_dataset(self):
        ds = _make_clock_ds()
        result = validate_clk_dataset(ds)

        assert result["has_clock_offset"] == True  # noqa: E712
        assert result["has_epoch"] == True  # noqa: E712
        assert result["has_sv"] == True  # noqa: E712
        assert result["valid_data_percent"] == pytest.approx(100.0)
        assert result["epochs_monotonic"] == True  # noqa: E712
        assert result["num_epochs"] == 10
        assert result["num_satellites"] == 5

    def test_missing_clock_offset(self):
        ds = xr.Dataset(
            {"other_var": (["epoch", "sv"], np.zeros((3, 2)))},
            coords={
                "epoch": np.array(
                    [
                        np.datetime64("2024-01-01") + np.timedelta64(i, "s")
                        for i in range(3)
                    ]
                ),
                "sv": ["G01", "G02"],
            },
        )
        result = validate_clk_dataset(ds)
        assert result["has_clock_offset"] is False
        assert result["valid_data_percent"] == 0.0

    def test_missing_epoch(self):
        ds = xr.Dataset(
            {"clock_offset": (["time", "sv"], np.zeros((3, 2)))},
            coords={
                "time": np.arange(3),
                "sv": ["G01", "G02"],
            },
        )
        result = validate_clk_dataset(ds)
        assert result["has_epoch"] is False
        assert result["epochs_monotonic"] is False
        assert result["num_epochs"] == 0

    def test_missing_sv(self):
        ds = xr.Dataset(
            {"clock_offset": (["epoch", "satellite"], np.zeros((3, 2)))},
            coords={
                "epoch": np.array(
                    [
                        np.datetime64("2024-01-01") + np.timedelta64(i, "s")
                        for i in range(3)
                    ]
                ),
                "satellite": ["G01", "G02"],
            },
        )
        result = validate_clk_dataset(ds)
        assert result["has_sv"] is False
        assert result["num_satellites"] == 0

    def test_partial_nan_coverage(self):
        ds = _make_clock_ds(nan_fraction=0.5)
        result = validate_clk_dataset(ds)
        assert result["valid_data_percent"] < 100.0
        assert result["valid_data_percent"] > 0.0

    def test_non_monotonic_epochs(self):
        ds = _make_clock_ds(monotonic=False)
        result = validate_clk_dataset(ds)
        assert result["epochs_monotonic"] == False  # noqa: E712


# ---------------------------------------------------------------------------
# check_clk_data_quality
# ---------------------------------------------------------------------------


class TestCheckClkDataQuality:
    """Tests for check_clk_data_quality()."""

    def test_high_quality_passes(self):
        ds = _make_clock_ds()
        assert check_clk_data_quality(ds) is True

    def test_low_coverage_fails(self):
        ds = _make_clock_ds(nan_fraction=0.5)
        assert check_clk_data_quality(ds, min_coverage=80.0) is False

    def test_missing_components_fails(self):
        ds = xr.Dataset({"other": (["x"], [1.0])})
        assert check_clk_data_quality(ds) is False

    def test_non_monotonic_fails(self):
        ds = _make_clock_ds(monotonic=False)
        assert check_clk_data_quality(ds) is False

    def test_custom_min_coverage(self):
        ds = _make_clock_ds(nan_fraction=0.3)
        # With 30% NaN, coverage ~70% — should pass at 60% threshold
        assert check_clk_data_quality(ds, min_coverage=50.0) is True


# ---------------------------------------------------------------------------
# Sp3Validator
# ---------------------------------------------------------------------------


class TestSp3Validator:
    """Tests for Sp3Validator."""

    def test_valid_dataset(self, tmp_path):
        ds = _make_sp3_ds()
        fpath = tmp_path / "test.sp3"
        fpath.touch()

        validator = Sp3Validator(ds, fpath)
        result = validator.validate()

        assert isinstance(result, FileValidationResult)
        assert result.is_valid is True
        assert result.errors == []

    def test_missing_variables(self, tmp_path):
        ds = _make_sp3_ds(include_xyz=False)
        fpath = tmp_path / "test.sp3"
        fpath.touch()

        validator = Sp3Validator(ds, fpath)
        result = validator.validate()

        assert result.is_valid is False
        assert any("Missing required variables" in e for e in result.errors)

    def test_missing_coordinates(self, tmp_path):
        ds = xr.Dataset(
            {
                "X": (["time", "satellite"], np.zeros((3, 2))),
                "Y": (["time", "satellite"], np.zeros((3, 2))),
                "Z": (["time", "satellite"], np.zeros((3, 2))),
            },
            coords={"time": np.arange(3), "satellite": ["G01", "G02"]},
        )
        fpath = tmp_path / "test.sp3"
        fpath.touch()

        validator = Sp3Validator(ds, fpath)
        result = validator.validate()

        assert result.is_valid is False
        assert any("Missing required coordinates" in e for e in result.errors)

    def test_excessive_nans_error(self, tmp_path):
        ds = _make_sp3_ds(nan_fraction=0.6)
        fpath = tmp_path / "test.sp3"
        fpath.touch()

        validator = Sp3Validator(ds, fpath)
        result = validator.validate()

        # >50% NaN should trigger error
        assert result.is_valid is False

    def test_moderate_nans_warning(self, tmp_path):
        ds = _make_sp3_ds(nan_fraction=0.15)
        fpath = tmp_path / "test.sp3"
        fpath.touch()

        validator = Sp3Validator(ds, fpath)
        result = validator.validate()

        # 15% NaN should trigger warning but still be valid
        assert result.is_valid is True
        assert len(result.warnings) > 0

    def test_summary(self, tmp_path):
        ds = _make_sp3_ds()
        fpath = tmp_path / "test.sp3"
        fpath.touch()

        validator = Sp3Validator(ds, fpath)
        validator.validate()
        summary = validator.get_summary()

        assert "VALID" in summary


# ---------------------------------------------------------------------------
# FileValidationResult
# ---------------------------------------------------------------------------


class TestFileValidationResult:
    """Tests for FileValidationResult model."""

    def test_creation(self, tmp_path):
        result = FileValidationResult(
            is_valid=True,
            file_path=tmp_path / "test.sp3",
            file_type="SP3",
        )
        assert result.is_valid is True
        assert result.errors == []
        assert result.warnings == []

    def test_add_error(self, tmp_path):
        result = FileValidationResult(
            is_valid=True,
            file_path=tmp_path / "test.sp3",
            file_type="SP3",
        )
        result.add_error("something wrong")
        assert result.is_valid is False
        assert "something wrong" in result.errors

    def test_add_warning(self, tmp_path):
        result = FileValidationResult(
            is_valid=True,
            file_path=tmp_path / "test.sp3",
            file_type="SP3",
        )
        result.add_warning("something minor")
        assert result.is_valid is True
        assert "something minor" in result.warnings

    def test_summary_valid(self, tmp_path):
        result = FileValidationResult(
            is_valid=True,
            file_path=tmp_path / "test.sp3",
            file_type="SP3",
        )
        assert "VALID" in result.summary()

    def test_summary_invalid(self, tmp_path):
        result = FileValidationResult(
            is_valid=False,
            errors=["error1"],
            file_path=tmp_path / "test.sp3",
            file_type="SP3",
        )
        summary = result.summary()
        assert "INVALID" in summary
        assert "error1" in summary
