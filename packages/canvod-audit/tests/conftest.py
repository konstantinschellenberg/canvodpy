"""Shared fixtures for canvod-audit tests."""

from __future__ import annotations

import numpy as np
import pytest
import xarray as xr


@pytest.fixture
def synthetic_ds() -> xr.Dataset:
    """A clean synthetic GNSS dataset with (epoch, sid) dims."""
    rng = np.random.default_rng(42)
    n_epochs, n_sids = 100, 20

    epochs = np.arange(
        np.datetime64("2025-01-01"),
        np.datetime64("2025-01-01") + np.timedelta64(n_epochs * 30, "s"),
        np.timedelta64(30, "s"),
    )
    sids = [f"G{i:02d}|L1|C" for i in range(1, n_sids + 1)]

    return xr.Dataset(
        {
            "SNR": (["epoch", "sid"], rng.uniform(20, 50, (n_epochs, n_sids))),
            "carrier_phase": (
                ["epoch", "sid"],
                rng.standard_normal((n_epochs, n_sids)),
            ),
            "sat_x": (["epoch", "sid"], rng.uniform(-3e7, 3e7, (n_epochs, n_sids))),
        },
        coords={"epoch": epochs, "sid": sids},
    )


@pytest.fixture
def perturbed_ds(synthetic_ds: xr.Dataset) -> xr.Dataset:
    """Synthetic dataset with small perturbations (simulates numerical noise)."""
    rng = np.random.default_rng(99)
    ds = synthetic_ds.copy(deep=True)

    for var in ds.data_vars:
        noise = rng.normal(0, 1e-10, ds[var].shape)
        ds[var] = ds[var] + noise

    return ds


@pytest.fixture
def damaged_ds(synthetic_ds: xr.Dataset) -> xr.Dataset:
    """Synthetic dataset with NaNs and missing satellites."""
    ds = synthetic_ds.copy(deep=True)

    snr = ds["SNR"].values.copy()
    snr[:, 15:] = np.nan  # 25% of satellites have no SNR
    ds["SNR"] = (["epoch", "sid"], snr)

    return ds
