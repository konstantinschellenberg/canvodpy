"""Integration tests for audit tiers using synthetic data.

These tests exercise the full audit runner functions with synthetic
fixtures — no external data, no external tools, CI-safe.
"""

from __future__ import annotations

import numpy as np
import pytest
import xarray as xr

from canvod.audit.core import compare_datasets
from canvod.audit.runners.common import AuditResult
from canvod.audit.tolerances import Tolerance, ToleranceTier

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def canopy_ds():
    """Synthetic canopy station dataset with angles and SNR."""
    rng = np.random.default_rng(42)
    n_epochs, n_sids = 100, 10
    epochs = np.arange(
        np.datetime64("2025-01-01"),
        np.datetime64("2025-01-01") + np.timedelta64(n_epochs * 30, "s"),
        np.timedelta64(30, "s"),
    )
    sids = [f"G{i:02d}|L1|C" for i in range(1, n_sids + 1)]

    theta = rng.uniform(0, np.pi / 2, (n_epochs, n_sids))  # polar angle
    phi = rng.uniform(0, 2 * np.pi, (n_epochs, n_sids))  # azimuth

    return xr.Dataset(
        {
            "SNR": (["epoch", "sid"], rng.uniform(20, 50, (n_epochs, n_sids))),
            "theta": (["epoch", "sid"], theta),
            "phi": (["epoch", "sid"], phi),
        },
        coords={
            "epoch": epochs,
            "sid": sids,
            "sv": ("sid", [f"G{i:02d}" for i in range(1, n_sids + 1)]),
            "band": ("sid", ["L1"] * n_sids),
            "code": ("sid", ["C"] * n_sids),
        },
    )


@pytest.fixture
def reference_ds(canopy_ds):
    """Synthetic reference station — same structure, different SNR."""
    rng = np.random.default_rng(99)
    n_epochs, n_sids = canopy_ds.SNR.shape
    ds = canopy_ds.copy(deep=True)
    ds["SNR"] = (["epoch", "sid"], rng.uniform(30, 55, (n_epochs, n_sids)))
    return ds


# ---------------------------------------------------------------------------
# Tier 0: Self-consistency (canvodpy vs canvodpy)
# ---------------------------------------------------------------------------


class TestTier0:
    """Tier 0: pipeline self-consistency checks."""

    def test_identical_pipeline_output(self, canopy_ds):
        """Same input through same code → bit-identical output."""
        result = compare_datasets(
            canopy_ds, canopy_ds, tier=ToleranceTier.EXACT, label="tier0-self"
        )
        assert result.passed
        assert all(vs.rmse == 0.0 for vs in result.variable_stats.values())

    def test_reordered_epochs_still_align(self, canopy_ds):
        """Shuffled epoch order should align and match exactly."""
        rng = np.random.default_rng(7)
        perm = rng.permutation(len(canopy_ds.epoch))
        ds_shuffled = canopy_ds.isel(epoch=perm)

        result = compare_datasets(
            canopy_ds, ds_shuffled, tier=ToleranceTier.EXACT, label="tier0-reorder"
        )
        assert result.passed

    def test_subset_sids_detected(self, canopy_ds):
        """Comparing full vs subset should report dropped SIDs."""
        ds_sub = canopy_ds.isel(sid=slice(0, 5))
        result = compare_datasets(
            canopy_ds, ds_sub, tier=ToleranceTier.EXACT, label="tier0-subset"
        )
        assert result.passed  # shared subset is identical
        assert result.alignment.n_shared_sids == 5
        assert result.alignment.n_dropped_sids_a == 5

    def test_float_reordering_noise_numerical_annotation(self, canopy_ds):
        """Simulated float noise within NUMERICAL tolerance: passed=True, no failures."""
        rng = np.random.default_rng(11)
        ds_noisy = canopy_ds.copy(deep=True)
        for var in ds_noisy.data_vars:
            ds_noisy[var] = ds_noisy[var] + rng.normal(0, 1e-12, ds_noisy[var].shape)

        result = compare_datasets(
            canopy_ds, ds_noisy, tier=ToleranceTier.NUMERICAL, label="tier0-float"
        )
        # Within NUMERICAL tolerance → passed
        assert result.passed
        assert len(result.failures) == 0

    def test_float_reordering_noise_fails_exact(self, canopy_ds):
        """Same noise should fail EXACT."""
        rng = np.random.default_rng(11)
        ds_noisy = canopy_ds.copy(deep=True)
        for var in ds_noisy.data_vars:
            ds_noisy[var] = ds_noisy[var] + rng.normal(0, 1e-12, ds_noisy[var].shape)

        result = compare_datasets(
            canopy_ds, ds_noisy, tier=ToleranceTier.EXACT, label="tier0-float-exact"
        )
        assert not result.passed


# ---------------------------------------------------------------------------
# Tier 1: Internal consistency (SBF vs RINEX, broadcast vs agency)
# ---------------------------------------------------------------------------


class TestTier1:
    """Tier 1: internal consistency between data sources."""

    def test_snr_quantization_scientific_annotation(self, canopy_ds):
        """SBF 0.25 dB quantization: within SCIENTIFIC tolerance → passed=True, no failures."""
        ds_sbf = canopy_ds.copy(deep=True)
        # Simulate SBF quantization: round SNR to nearest 0.25 dB
        snr = ds_sbf["SNR"].values
        ds_sbf["SNR"] = (["epoch", "sid"], np.round(snr * 4) / 4)

        result = compare_datasets(
            canopy_ds,
            ds_sbf,
            variables=["SNR"],
            tier=ToleranceTier.SCIENTIFIC,
            label="tier1-snr-quant",
        )
        assert result.passed  # within SCIENTIFIC tolerance
        assert len(result.failures) == 0

    def test_snr_quantization_fails_exact(self, canopy_ds):
        """SBF quantization should fail EXACT."""
        ds_sbf = canopy_ds.copy(deep=True)
        snr = ds_sbf["SNR"].values
        ds_sbf["SNR"] = (["epoch", "sid"], np.round(snr * 4) / 4)

        result = compare_datasets(
            canopy_ds,
            ds_sbf,
            variables=["SNR"],
            tier=ToleranceTier.EXACT,
            label="tier1-snr-exact",
        )
        assert not result.passed

    def test_ephemeris_angular_diff_scientific_annotation(self, canopy_ds):
        """Broadcast vs agency angular diffs (~0.002 rad): within SCIENTIFIC tolerance → passed."""
        rng = np.random.default_rng(33)
        ds_broadcast = canopy_ds.copy(deep=True)
        # Simulate broadcast ephemeris angular error: ~0.002 rad (0.13°)
        ds_broadcast["theta"] = ds_broadcast["theta"] + rng.normal(
            0, 0.001, canopy_ds["theta"].shape
        )
        ds_broadcast["phi"] = ds_broadcast["phi"] + rng.normal(
            0, 0.005, canopy_ds["phi"].shape
        )

        result = compare_datasets(
            canopy_ds,
            ds_broadcast,
            variables=["theta", "phi"],
            tier=ToleranceTier.SCIENTIFIC,
            label="tier1-ephem-angular",
        )
        assert result.passed  # within SCIENTIFIC tolerance
        assert len(result.failures) == 0

    def test_large_ephemeris_diff_fails_scientific(self, canopy_ds):
        """Large angular error (>0.05 rad / 3°) should fail SCIENTIFIC."""
        ds_bad = canopy_ds.copy(deep=True)
        ds_bad["theta"] = ds_bad["theta"] + 0.1  # 5.7° systematic offset

        result = compare_datasets(
            canopy_ds,
            ds_bad,
            variables=["theta"],
            tier=ToleranceTier.SCIENTIFIC,
            label="tier1-ephem-bad",
        )
        assert not result.passed

    def test_nan_coverage_mismatch_detected(self, canopy_ds):
        """Different satellite visibility (NaN patterns) should be caught."""
        ds_fewer = canopy_ds.copy(deep=True)
        snr = ds_fewer["SNR"].values.copy()
        snr[:, 8:] = np.nan  # SBF sees fewer satellites
        ds_fewer["SNR"] = (["epoch", "sid"], snr)

        result = compare_datasets(
            canopy_ds,
            ds_fewer,
            variables=["SNR"],
            tier=ToleranceTier.SCIENTIFIC,
            tolerance_overrides={
                "SNR": Tolerance(atol=0.25, mae_atol=0.0, nan_rate_atol=0.01),
            },
            label="tier1-nan-coverage",
        )
        assert not result.passed
        assert "NaN" in result.failures["SNR"]


# ---------------------------------------------------------------------------
# Tier 2: Regression (freeze / compare checkpoint)
# ---------------------------------------------------------------------------


class TestTier2:
    """Tier 2: regression testing via freeze/compare."""

    def test_freeze_and_compare_identical(self, canopy_ds, tmp_path):
        """Freeze a dataset, load it back, compare — should be EXACT."""
        from canvod.audit.tiers.regression import (
            compare_against_checkpoint,
            freeze_checkpoint,
        )

        cp_path = freeze_checkpoint(
            canopy_ds,
            tmp_path / "checkpoint.nc",
            metadata={"version": "0.1.0", "git_hash": "abc123"},
        )
        assert cp_path.exists()

        result = compare_against_checkpoint(
            canopy_ds, cp_path, tier=ToleranceTier.EXACT
        )
        assert result.passed

    def test_regression_detected_after_change(self, canopy_ds, tmp_path):
        """Modifying data after freeze should be caught as regression."""
        from canvod.audit.tiers.regression import (
            compare_against_checkpoint,
            freeze_checkpoint,
        )

        cp_path = freeze_checkpoint(canopy_ds, tmp_path / "checkpoint.nc")

        # Simulate a code change that shifts SNR by 0.1
        ds_changed = canopy_ds.copy(deep=True)
        ds_changed["SNR"] = ds_changed["SNR"] + 0.1

        result = compare_against_checkpoint(
            ds_changed, cp_path, tier=ToleranceTier.EXACT
        )
        assert not result.passed
        assert "SNR" in result.failures

    def test_regression_metadata_preserved(self, canopy_ds, tmp_path):
        """Checkpoint metadata should survive the round-trip."""
        from canvod.audit.tiers.regression import freeze_checkpoint

        cp_path = freeze_checkpoint(
            canopy_ds,
            tmp_path / "checkpoint.nc",
            metadata={"version": "0.3.0", "note": "post-bugfix baseline"},
        )

        ds_loaded = xr.open_dataset(cp_path)
        assert ds_loaded.attrs["checkpoint_version"] == "0.3.0"
        assert ds_loaded.attrs["checkpoint_note"] == "post-bugfix baseline"
        ds_loaded.close()


# ---------------------------------------------------------------------------
# Tier 3: External comparison (canvodpy vs gnssvod)
# ---------------------------------------------------------------------------


class TestTier3:
    """Tier 3: external comparison with gnssvod-style data."""

    def test_adapter_sid_to_prn(self, canopy_ds):
        """GnssvodAdapter correctly maps SID → PRN."""
        from canvod.audit.runners.vs_gnssvod import GnssvodAdapter

        adapter = GnssvodAdapter(canopy_ds, band_filter="L1|C", snr_col="S1C")
        ds_adapted = adapter.to_gnssvod_dataset()

        # SIDs should now be PRNs
        assert list(ds_adapted.sid.values) == [f"G{i:02d}" for i in range(1, 11)]
        assert "S1C" in ds_adapted.data_vars

    def test_adapter_angle_conversion(self, canopy_ds):
        """theta/phi (rad) → Elevation/Azimuth (deg) conversion."""
        from canvod.audit.runners.vs_gnssvod import GnssvodAdapter

        adapter = GnssvodAdapter(canopy_ds, band_filter="L1|C", snr_col="S1C")
        ds_adapted = adapter.to_gnssvod_dataset()

        assert "Elevation" in ds_adapted.data_vars
        assert "Azimuth" in ds_adapted.data_vars

        el = ds_adapted["Elevation"].values
        az = ds_adapted["Azimuth"].values
        assert np.all(el[~np.isnan(el)] >= 0)
        assert np.all(el[~np.isnan(el)] <= 90)
        assert np.all(az[~np.isnan(az)] >= 0)
        assert np.all(az[~np.isnan(az)] < 360)

    def test_full_comparison_self_consistency(self, canopy_ds):
        """Adapted canvodpy vs itself should pass SCIENTIFIC."""
        from canvod.audit.runners.vs_gnssvod import GnssvodAdapter

        adapter = GnssvodAdapter(canopy_ds, band_filter="L1|C", snr_col="S1C")
        ds_gnssvod_style = adapter.to_gnssvod_dataset()

        result = compare_datasets(
            ds_gnssvod_style,
            ds_gnssvod_style,
            tier=ToleranceTier.SCIENTIFIC,
            label="tier3-self",
        )
        assert result.passed

    def test_gnssvod_df_round_trip(self):
        """DataFrame → xarray → compare should work end-to-end."""
        import pandas as pd

        from canvod.audit.runners.vs_gnssvod import gnssvod_df_to_xarray

        rng = np.random.default_rng(42)
        epochs = pd.date_range("2025-01-01", periods=50, freq="30s")
        svs = ["G01", "G02", "G03"]

        rows = []
        for epoch in epochs:
            for sv in svs:
                rows.append(
                    {
                        "Epoch": epoch,
                        "SV": sv,
                        "S1C": rng.uniform(20, 50),
                        "Azimuth": rng.uniform(0, 360),
                        "Elevation": rng.uniform(5, 90),
                    }
                )
        df = pd.DataFrame(rows)

        ds = gnssvod_df_to_xarray(df)
        result = compare_datasets(
            ds, ds, tier=ToleranceTier.EXACT, label="tier3-df-roundtrip"
        )
        assert result.passed

    def test_merge_codes_then_compare(self):
        """Merged multi-code dataset should match single-code baseline."""
        from canvod.audit.runners.vs_gnssvod import gnssvod_merge_codes

        n_epochs = 50
        rng = np.random.default_rng(42)
        snr_values = rng.uniform(20, 50, n_epochs)

        # Single-code: only G01|L1|C
        ds_single = xr.Dataset(
            {"SNR": (["epoch", "sid"], snr_values[:, np.newaxis])},
            coords={"epoch": np.arange(n_epochs), "sid": ["G01|L1|C"]},
        )

        # Multi-code: G01|L1|C (same data) + G01|L1|W (all NaN)
        ds_multi = xr.Dataset(
            {
                "SNR": (
                    ["epoch", "sid"],
                    np.column_stack([snr_values, np.full(n_epochs, np.nan)]),
                )
            },
            coords={"epoch": np.arange(n_epochs), "sid": ["G01|L1|C", "G01|L1|W"]},
        )

        merged = gnssvod_merge_codes(ds_multi, band_num="1")
        assert list(merged.sid.values) == ["G01"]

        # After merge, SNR should match single-code exactly
        np.testing.assert_array_equal(merged["SNR"].sel(sid="G01").values, snr_values)


# ---------------------------------------------------------------------------
# Edge cases: alignment and error handling
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Edge cases that must not produce silent wrong results."""

    def test_no_shared_coords_fails(self):
        """Datasets with zero overlap must fail, not silently pass."""
        ds_a = xr.Dataset(
            {"x": (["epoch", "sid"], np.ones((5, 3)))},
            coords={
                "epoch": np.arange(
                    np.datetime64("2025-01-01"),
                    np.datetime64("2025-01-01") + np.timedelta64(5, "D"),
                    np.timedelta64(1, "D"),
                ),
                "sid": ["A", "B", "C"],
            },
        )
        ds_b = xr.Dataset(
            {"x": (["epoch", "sid"], np.ones((5, 3)))},
            coords={
                "epoch": np.arange(
                    np.datetime64("2026-01-01"),
                    np.datetime64("2026-01-01") + np.timedelta64(5, "D"),
                    np.timedelta64(1, "D"),
                ),
                "sid": ["D", "E", "F"],
            },
        )
        result = compare_datasets(ds_a, ds_b, tier=ToleranceTier.EXACT)
        assert not result.passed
        assert "_alignment" in result.failures

    def test_all_nan_variable_fails(self, canopy_ds):
        """A variable that is all-NaN in one dataset should fail."""
        ds_nan = canopy_ds.copy(deep=True)
        ds_nan["SNR"] = ds_nan["SNR"] * np.nan

        result = compare_datasets(
            canopy_ds,
            ds_nan,
            variables=["SNR"],
            tier=ToleranceTier.SCIENTIFIC,
            tolerance_overrides={
                "SNR": Tolerance(atol=0.25, mae_atol=0.0, nan_rate_atol=0.01),
            },
            label="edge-all-nan",
        )
        assert not result.passed

    def test_audit_result_aggregation(self, canopy_ds):
        """AuditResult correctly aggregates pass/fail across tiers."""
        audit = AuditResult()

        r_pass = compare_datasets(
            canopy_ds, canopy_ds, tier=ToleranceTier.EXACT, label="pass"
        )
        ds_bad = canopy_ds.copy(deep=True)
        ds_bad["SNR"] = ds_bad["SNR"] + 1.0
        r_fail = compare_datasets(
            canopy_ds, ds_bad, tier=ToleranceTier.EXACT, label="fail"
        )

        audit.results["pass"] = r_pass
        audit.results["fail"] = r_fail

        assert not audit.passed
        assert audit.n_passed == 1
        assert audit.n_total == 2
        assert "1/2 passed" in audit.summary()
