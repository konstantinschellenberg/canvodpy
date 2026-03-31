"""Tests for the core comparison engine."""

from __future__ import annotations

import xarray as xr

from canvod.audit import compare_datasets
from canvod.audit.tolerances import Tolerance, ToleranceTier


def test_identical_datasets_pass_exact(synthetic_ds: xr.Dataset):
    """Comparing a dataset to itself should pass at EXACT tier."""
    result = compare_datasets(
        synthetic_ds, synthetic_ds, tier=ToleranceTier.EXACT, label="self-check"
    )
    assert result.passed
    assert len(result.failures) == 0
    assert all(vs.rmse == 0.0 for vs in result.variable_stats.values())
    assert all(vs.max_abs_diff == 0.0 for vs in result.variable_stats.values())


def test_perturbed_datasets_fail_exact(
    synthetic_ds: xr.Dataset, perturbed_ds: xr.Dataset
):
    """Numerical noise should fail at EXACT tier."""
    result = compare_datasets(synthetic_ds, perturbed_ds, tier=ToleranceTier.EXACT)
    assert not result.passed


def test_perturbed_datasets_numerical_annotation(
    synthetic_ds: xr.Dataset, perturbed_ds: xr.Dataset
):
    """Numerical noise (1e-10): within NUMERICAL tolerance → passed=True, no failures.

    exact_match is False (values differ), but passed reflects tolerance outcome
    not bit-identity. exact_match is still recorded per-variable for information.
    """
    result = compare_datasets(synthetic_ds, perturbed_ds, tier=ToleranceTier.NUMERICAL)
    # Within NUMERICAL tolerance → passed
    assert result.passed
    assert len(result.failures) == 0
    # exact_match is False because values differ (even if only by 1e-10)
    assert all(not vs.exact_match for vs in result.variable_stats.values())


def test_nan_disagreement_detected(synthetic_ds: xr.Dataset, damaged_ds: xr.Dataset):
    """NaN disagreement should be reported and fail with strict NaN tolerance."""
    result = compare_datasets(
        synthetic_ds,
        damaged_ds,
        tier=ToleranceTier.SCIENTIFIC,
        tolerance_overrides={
            "SNR": Tolerance(atol=1.0, mae_atol=0.0, nan_rate_atol=0.01),
        },
    )
    assert "SNR" in result.failures
    assert "NaN" in result.failures["SNR"]


def test_alignment_info(synthetic_ds: xr.Dataset):
    """Alignment should report shared and dropped coordinates."""
    ds_subset = synthetic_ds.isel(sid=slice(0, 10), epoch=slice(0, 50))
    result = compare_datasets(synthetic_ds, ds_subset, tier=ToleranceTier.EXACT)

    assert result.alignment is not None
    assert result.alignment.n_shared_sids == 10
    assert result.alignment.n_shared_epochs == 50
    assert result.alignment.n_dropped_sids_a == 10
    assert result.alignment.n_dropped_epochs_a == 50


def test_to_polars(synthetic_ds: xr.Dataset):
    """Result should convert to a polars DataFrame."""
    result = compare_datasets(synthetic_ds, synthetic_ds, tier=ToleranceTier.EXACT)
    df = result.to_polars()
    assert len(df) == len(result.variable_stats)
    assert "variable" in df.columns
    assert "rmse" in df.columns


def test_summary_string(synthetic_ds: xr.Dataset):
    """Summary should be a non-empty string with key info."""
    result = compare_datasets(
        synthetic_ds, synthetic_ds, tier=ToleranceTier.EXACT, label="test"
    )
    summary = result.summary()
    assert "test" in summary
    assert "PASSED" in summary


def test_specific_variables(synthetic_ds: xr.Dataset):
    """Should only compare specified variables."""
    result = compare_datasets(
        synthetic_ds,
        synthetic_ds,
        variables=["SNR"],
        tier=ToleranceTier.EXACT,
    )
    assert list(result.variable_stats.keys()) == ["SNR"]


def test_epoch_dtype_mismatch(synthetic_ds: xr.Dataset):
    """Datasets with different datetime64 resolutions should still align."""

    # Create a copy with microsecond-resolution epochs (simulates pandas origin)
    ds_us = synthetic_ds.copy(deep=True)
    epochs_us = synthetic_ds.epoch.values.astype("datetime64[us]")
    ds_us = ds_us.assign_coords(epoch=epochs_us)

    # Ensure the two datasets have different datetime64 resolutions
    assert synthetic_ds.epoch.values.dtype != ds_us.epoch.values.dtype

    result = compare_datasets(
        synthetic_ds, ds_us, tier=ToleranceTier.EXACT, label="dtype-mix"
    )
    assert result.passed
    assert result.alignment.n_shared_epochs == len(synthetic_ds.epoch)
