"""Tests for Airflow task functions (canvodpy.workflows.tasks).

Tests use temporary directories and mock data to avoid filesystem
dependencies. Task functions are plain Python — no Airflow required.
"""

from __future__ import annotations

import shutil
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import xarray as xr
from canvodpy.workflows.tasks import (
    _discover_files_for_date,
    _resolve_date,
    check_rinex,
    check_sbf,
    cleanup,
    parse_sampling_interval_from_filename,
    validate_ingest,
)

# ---------------------------------------------------------------------------
# Utility tests
# ---------------------------------------------------------------------------


class TestResolveDate:
    """Test YYYYDOY / Airflow ds parsing."""

    def test_yyyydoy_format(self):
        d = _resolve_date("2025001")
        assert d.year == 2025
        assert d.doy == "001"

    def test_airflow_ds_format(self):
        d = _resolve_date("2025-01-01")
        assert d.year == 2025
        assert d.doy == "001"

    def test_mid_year(self):
        d = _resolve_date("2025-07-01")
        assert d.year == 2025
        assert d.doy == "182"


class TestParseSamplingInterval:
    """Test RINEX v3 filename sampling interval extraction."""

    def test_5_second(self):
        assert (
            parse_sampling_interval_from_filename(
                "ROSA01TUW_R_20250010000_15M_05S_AA.rnx"
            )
            == 5.0
        )

    def test_30_second(self):
        assert (
            parse_sampling_interval_from_filename(
                "ROSA01TUW_R_20250010000_01D_30S_AA.rnx"
            )
            == 30.0
        )

    def test_1_hz(self):
        result = parse_sampling_interval_from_filename(
            "ROSA01TUW_R_20250010000_01H_01Z_AA.rnx"
        )
        assert result == pytest.approx(1.0)

    def test_unparseable(self):
        assert parse_sampling_interval_from_filename("random_file.dat") is None

    def test_short_name(self):
        assert parse_sampling_interval_from_filename("short.rnx") is None


# ---------------------------------------------------------------------------
# check_rinex / check_sbf tests
# ---------------------------------------------------------------------------


class TestCheckRinex:
    """Test check_rinex with mock config and filesystem."""

    @pytest.fixture()
    def mock_site(self, tmp_path):
        """Create a mock site config and data directory."""
        # Create receiver directories with test files
        canopy_dir = tmp_path / "canopy" / "25001"
        canopy_dir.mkdir(parents=True)
        (canopy_dir / "ROSA01TUW_R_20250010000_15M_05S_AA.rnx").touch()

        ref_dir = tmp_path / "reference" / "25001"
        ref_dir.mkdir(parents=True)
        (ref_dir / "ROSR01TUW_R_20250010000_15M_05S_AA.rnx").touch()

        # Mock config
        canopy_cfg = MagicMock()
        canopy_cfg.directory = "canopy"
        canopy_cfg.type = "canopy"
        canopy_cfg.naming = None

        ref_cfg = MagicMock()
        ref_cfg.directory = "reference"
        ref_cfg.type = "reference"
        ref_cfg.naming = None

        site_cfg = MagicMock()
        site_cfg.receivers = {"canopy_01": canopy_cfg, "reference_01": ref_cfg}
        site_cfg.get_base_path.return_value = tmp_path
        site_cfg.naming = None

        config = MagicMock()
        config.sites.sites = {"TestSite": site_cfg}

        return config

    def test_all_files_present(self, mock_site):
        with patch("canvodpy.workflows.tasks.load_config", return_value=mock_site):
            result = check_rinex("TestSite", "2025001")
        assert result["ready"] is True
        assert result["receivers"]["canopy_01"]["has_files"] is True
        assert result["receivers"]["reference_01"]["has_files"] is True

    def test_missing_files_raises(self, mock_site):
        # Remove canopy files
        base = mock_site.sites.sites["TestSite"].get_base_path()
        shutil.rmtree(base / "canopy" / "25001")
        (base / "canopy" / "25001").mkdir(parents=True)

        with patch("canvodpy.workflows.tasks.load_config", return_value=mock_site):
            with pytest.raises(RuntimeError, match="missing receivers"):
                check_rinex("TestSite", "2025001")


class TestCheckSbf:
    """Test check_sbf with mock config and filesystem."""

    @pytest.fixture()
    def mock_site_sbf(self, tmp_path):
        canopy_dir = tmp_path / "canopy" / "25001"
        canopy_dir.mkdir(parents=True)
        (canopy_dir / "ROSA01TUW_R_20250010000_15M_05S_AA.sbf").touch()

        ref_dir = tmp_path / "reference" / "25001"
        ref_dir.mkdir(parents=True)
        (ref_dir / "ROSR01TUW_R_20250010000_15M_05S_AA.sbf").touch()

        canopy_cfg = MagicMock()
        canopy_cfg.directory = "canopy"
        canopy_cfg.type = "canopy"
        canopy_cfg.naming = None

        ref_cfg = MagicMock()
        ref_cfg.directory = "reference"
        ref_cfg.type = "reference"
        ref_cfg.naming = None

        site_cfg = MagicMock()
        site_cfg.receivers = {"canopy_01": canopy_cfg, "reference_01": ref_cfg}
        site_cfg.get_base_path.return_value = tmp_path
        site_cfg.naming = None

        config = MagicMock()
        config.sites.sites = {"TestSite": site_cfg}
        return config

    def test_sbf_files_found(self, mock_site_sbf):
        with patch("canvodpy.workflows.tasks.load_config", return_value=mock_site_sbf):
            result = check_sbf("TestSite", "2025001")
        assert result["ready"] is True
        assert all(
            f.endswith(".sbf") for r in result["receivers"].values() for f in r["files"]
        )

    def test_rinex_files_ignored_by_sbf_check(self, mock_site_sbf):
        """check_sbf should NOT find .rnx files."""
        base = mock_site_sbf.sites.sites["TestSite"].get_base_path()
        # Replace .sbf with .rnx
        for sbf in (base / "canopy" / "25001").glob("*.sbf"):
            sbf.rename(sbf.with_suffix(".rnx"))
        for sbf in (base / "reference" / "25001").glob("*.sbf"):
            sbf.rename(sbf.with_suffix(".rnx"))

        with patch("canvodpy.workflows.tasks.load_config", return_value=mock_site_sbf):
            with pytest.raises(RuntimeError, match="missing receivers"):
                check_sbf("TestSite", "2025001")


# ---------------------------------------------------------------------------
# validate_ingest tests
# ---------------------------------------------------------------------------


class TestValidateIngest:
    """Test validate_ingest quality gate."""

    def _make_good_ds(self) -> xr.Dataset:
        """Create a dataset that passes all checks."""
        n_epoch, n_sid = 100, 5
        rng = np.random.default_rng(42)
        return xr.Dataset(
            {"cn0": (("epoch", "sid"), rng.uniform(25, 50, (n_epoch, n_sid)))},
            coords={
                "epoch": np.datetime64("2025-01-01", "s")
                + np.arange(n_epoch).astype("timedelta64[s]"),
                "sid": [f"G{i:02d}|L1|C" for i in range(1, n_sid + 1)],
                "theta": (
                    ("epoch", "sid"),
                    rng.uniform(0, np.pi / 2, (n_epoch, n_sid)),
                ),
                "phi": (
                    ("epoch", "sid"),
                    rng.uniform(0, 2 * np.pi, (n_epoch, n_sid)),
                ),
            },
        )

    def test_good_data_passes(self):
        ds = self._make_good_ds()
        mock_site = MagicMock()
        mock_site.load_rinex_data.return_value = ds

        config = MagicMock()
        config.sites.sites = {"TestSite": MagicMock()}
        config.sites.sites["TestSite"].receivers = {"canopy_01": MagicMock()}

        with (
            patch("canvodpy.workflows.tasks.load_config", return_value=config),
            patch("canvod.store.GnssResearchSite", return_value=mock_site),
        ):
            result = validate_ingest("TestSite", "2025001")
        assert result["valid"] is True

    def test_bad_snr_fails(self):
        ds = self._make_good_ds()
        # Set SNR to impossible values
        ds["cn0"].values[0, 0] = 999.0

        mock_site = MagicMock()
        mock_site.load_rinex_data.return_value = ds

        config = MagicMock()
        config.sites.sites = {"TestSite": MagicMock()}
        config.sites.sites["TestSite"].receivers = {"canopy_01": MagicMock()}

        with (
            patch("canvodpy.workflows.tasks.load_config", return_value=config),
            patch("canvod.store.GnssResearchSite", return_value=mock_site),
        ):
            with pytest.raises(RuntimeError, match="validation failed"):
                validate_ingest("TestSite", "2025001")

    def test_bad_theta_fails(self):
        ds = self._make_good_ds()
        # Set theta beyond π/2
        ds.coords["theta"].values[0, 0] = 2.0

        mock_site = MagicMock()
        mock_site.load_rinex_data.return_value = ds

        config = MagicMock()
        config.sites.sites = {"TestSite": MagicMock()}
        config.sites.sites["TestSite"].receivers = {"canopy_01": MagicMock()}

        with (
            patch("canvodpy.workflows.tasks.load_config", return_value=config),
            patch("canvod.store.GnssResearchSite", return_value=mock_site),
        ):
            with pytest.raises(RuntimeError, match="validation failed"):
                validate_ingest("TestSite", "2025001")

    def test_empty_data_fails(self):
        ds = xr.Dataset(
            {"cn0": (("epoch", "sid"), np.empty((0, 0)))},
            coords={"epoch": [], "sid": []},
        )

        mock_site = MagicMock()
        mock_site.load_rinex_data.return_value = ds

        config = MagicMock()
        config.sites.sites = {"TestSite": MagicMock()}
        config.sites.sites["TestSite"].receivers = {"canopy_01": MagicMock()}

        with (
            patch("canvodpy.workflows.tasks.load_config", return_value=config),
            patch("canvod.store.GnssResearchSite", return_value=mock_site),
        ):
            with pytest.raises(RuntimeError, match="validation failed"):
                validate_ingest("TestSite", "2025001")

    def test_no_data_skips_receiver(self):
        """If load_rinex_data raises, receiver is skipped, not failed."""
        mock_site = MagicMock()
        mock_site.load_rinex_data.side_effect = KeyError("no group")

        config = MagicMock()
        config.sites.sites = {"TestSite": MagicMock()}
        config.sites.sites["TestSite"].receivers = {"canopy_01": MagicMock()}

        with (
            patch("canvodpy.workflows.tasks.load_config", return_value=config),
            patch("canvod.store.GnssResearchSite", return_value=mock_site),
        ):
            # Should not raise — receiver is skipped
            result = validate_ingest("TestSite", "2025001")
            assert "canopy_01" in result["checks"]


# ---------------------------------------------------------------------------
# cleanup tests
# ---------------------------------------------------------------------------


class TestCleanup:
    """Test cleanup task."""

    def test_removes_aux_zarr(self, tmp_path):
        aux_zarr = tmp_path / "aux_2025001.zarr"
        aux_zarr.mkdir()
        (aux_zarr / "data.bin").touch()

        mock_config = MagicMock()
        mock_config.processing.storage.get_aux_data_dir.return_value = tmp_path

        with patch("canvodpy.workflows.tasks.load_config", return_value=mock_config):
            result = cleanup("TestSite", "2025001")

        assert not aux_zarr.exists()
        assert len(result["cleaned"]) == 1

    def test_no_zarr_is_noop(self, tmp_path):
        mock_config = MagicMock()
        mock_config.processing.storage.get_aux_data_dir.return_value = tmp_path

        with patch("canvodpy.workflows.tasks.load_config", return_value=mock_config):
            result = cleanup("TestSite", "2025001")

        assert result["cleaned"] == []


# ---------------------------------------------------------------------------
# _discover_files_for_date tests
# ---------------------------------------------------------------------------


class TestDiscoverFilesForDate:
    """Test FilenameMapper integration with glob fallback."""

    def test_glob_fallback_when_no_naming(self, tmp_path):
        """Without naming config, falls back to raw glob."""
        recv_dir = tmp_path / "canopy" / "25001"
        recv_dir.mkdir(parents=True)
        (recv_dir / "TEST01TUW_R_20250010000_15M_05S_AA.rnx").touch()
        (recv_dir / "TEST01TUW_R_20250010000_01D_01S_MO.sbf").touch()

        site_cfg = MagicMock()
        site_cfg.naming = None

        rcfg = MagicMock()
        rcfg.naming = None
        rcfg.directory = "canopy"
        rcfg.type = "canopy"

        date_obj = _resolve_date("2025001")
        files, warnings = _discover_files_for_date(
            site_cfg, rcfg, "canopy_01", date_obj, tmp_path
        )

        assert len(files) >= 1  # Should find at least rnx or sbf

    def test_empty_dir_returns_empty(self, tmp_path):
        recv_dir = tmp_path / "canopy" / "25001"
        recv_dir.mkdir(parents=True)

        site_cfg = MagicMock()
        site_cfg.naming = None

        rcfg = MagicMock()
        rcfg.naming = None
        rcfg.directory = "canopy"
        rcfg.type = "canopy"

        date_obj = _resolve_date("2025001")
        files, warnings = _discover_files_for_date(
            site_cfg, rcfg, "canopy_01", date_obj, tmp_path
        )

        assert files == []

    def test_nonexistent_dir_returns_empty(self, tmp_path):
        site_cfg = MagicMock()
        site_cfg.naming = None

        rcfg = MagicMock()
        rcfg.naming = None
        rcfg.directory = "nonexistent"
        rcfg.type = "canopy"

        date_obj = _resolve_date("2025001")
        files, warnings = _discover_files_for_date(
            site_cfg, rcfg, "canopy_01", date_obj, tmp_path
        )

        assert files == []
