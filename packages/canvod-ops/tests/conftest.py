"""Shared fixtures for canvod-ops tests."""

import numpy as np
import pandas as pd
import pytest
import xarray as xr


@pytest.fixture
def sample_ds() -> xr.Dataset:
    """Create a small (epoch, sid) dataset with 1-second spacing.

    Returns a dataset with:
    - 120 epochs at 1-second intervals
    - 3 SIDs
    - 1 data var ``SNR``
    - sid-only coord ``sv``
    - (epoch, sid) coords ``phi`` and ``theta``
    """
    rng = np.random.default_rng(42)

    n_epoch = 120
    n_sid = 3
    sids = ["G01_L1C", "G02_L1C", "G05_L1C"]

    epochs = pd.date_range("2024-01-01", periods=n_epoch, freq="1s")

    snr = rng.uniform(20.0, 50.0, size=(n_epoch, n_sid))
    phi = rng.uniform(0.0, 2 * np.pi, size=(n_epoch, n_sid))
    theta = rng.uniform(0.0, np.pi / 2, size=(n_epoch, n_sid))

    ds = xr.Dataset(
        {"SNR": (("epoch", "sid"), snr)},
        coords={
            "epoch": epochs.values,
            "sid": sids,
            "sv": ("sid", ["G01", "G02", "G05"]),
            "phi": (("epoch", "sid"), phi),
            "theta": (("epoch", "sid"), theta),
        },
        attrs={"File Hash": "abc123", "source": "test"},
    )
    return ds


@pytest.fixture
def coarse_ds() -> xr.Dataset:
    """Dataset already at 1-minute spacing (should trigger early exit)."""
    rng = np.random.default_rng(99)

    n_epoch = 10
    n_sid = 2
    sids = ["G01_L1C", "G02_L1C"]

    epochs = pd.date_range("2024-01-01", periods=n_epoch, freq="1min")
    snr = rng.uniform(20.0, 50.0, size=(n_epoch, n_sid))

    ds = xr.Dataset(
        {"SNR": (("epoch", "sid"), snr)},
        coords={
            "epoch": epochs.values,
            "sid": sids,
        },
        attrs={"File Hash": "def456"},
    )
    return ds


@pytest.fixture
def ds_no_phi_theta() -> xr.Dataset:
    """Dataset without phi/theta coords (grid assignment should skip)."""
    rng = np.random.default_rng(7)

    n_epoch = 10
    n_sid = 2
    sids = ["G01_L1C", "G02_L1C"]

    epochs = pd.date_range("2024-01-01", periods=n_epoch, freq="1s")
    snr = rng.uniform(20.0, 50.0, size=(n_epoch, n_sid))

    ds = xr.Dataset(
        {"SNR": (("epoch", "sid"), snr)},
        coords={
            "epoch": epochs.values,
            "sid": sids,
        },
        attrs={"File Hash": "ghi789"},
    )
    return ds
