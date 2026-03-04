"""Tests for UpdateStatistics Op."""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from canvod.ops.statistics.op import UpdateStatistics, update_statistics
from canvod.ops.statistics.profile import ProfileRegistry
from canvod.streamstats import CellSignalKey


@pytest.fixture
def sample_ds_with_cell_ids() -> xr.Dataset:
    """Dataset with cell_id coordinate for statistics testing."""
    rng = np.random.default_rng(42)
    n_epoch = 20
    n_sid = 3
    sids = ["G01_L1C", "G02_L1C", "G05_L1C"]
    epochs = pd.date_range("2024-01-01", periods=n_epoch, freq="1s")

    snr = rng.uniform(20.0, 50.0, size=(n_epoch, n_sid))
    # Assign cells: cycle through 3 cell IDs
    cell_ids = np.tile([1, 2, 3], (n_epoch, 1)).astype(np.float64)

    ds = xr.Dataset(
        {"SNR": (("epoch", "sid"), snr)},
        coords={
            "epoch": epochs.values,
            "sid": sids,
            "cell_id_equal_area_2.0deg": (("epoch", "sid"), cell_ids),
        },
        attrs={"File Hash": "test123"},
    )
    return ds


class TestUpdateStatistics:
    def test_skip_without_cell_id(self, ds_no_phi_theta):
        """Op should skip gracefully if no cell_id coord."""
        reg = ProfileRegistry()
        op = UpdateStatistics(reg, receiver_type="canopy", variables=["SNR"])
        out, result = op(ds_no_phi_theta)

        assert "skipped" in result.notes
        assert len(reg) == 0
        # Dataset passes through unchanged
        xr.testing.assert_identical(out, ds_no_phi_theta)

    def test_updates_registry(self, sample_ds_with_cell_ids):
        reg = ProfileRegistry()
        op = UpdateStatistics(reg, receiver_type="canopy", variables=["SNR"])
        out, result = op(sample_ds_with_cell_ids)

        assert len(reg) > 0
        assert "updates" in result.notes

        # Check a specific key exists
        key = CellSignalKey(1, "G01_L1C", "SNR", "canopy")
        assert key in reg
        assert reg[key].welford.count > 0

    def test_passes_ds_through(self, sample_ds_with_cell_ids):
        """Dataset should not be modified."""
        reg = ProfileRegistry()
        op = UpdateStatistics(reg, receiver_type="canopy")
        out, _ = op(sample_ds_with_cell_ids)
        xr.testing.assert_identical(out, sample_ds_with_cell_ids)

    def test_all_data_vars_when_none(self, sample_ds_with_cell_ids):
        """When variables=None, all data vars should be profiled."""
        reg = ProfileRegistry()
        op = UpdateStatistics(reg, receiver_type="canopy", variables=None)
        op(sample_ds_with_cell_ids)

        variables = {k.variable for k in reg.keys()}
        assert "SNR" in variables

    def test_convenience_function(self, sample_ds_with_cell_ids):
        reg = ProfileRegistry()
        out = update_statistics(
            sample_ds_with_cell_ids, reg, receiver_type="canopy", variables=["SNR"]
        )
        xr.testing.assert_identical(out, sample_ds_with_cell_ids)
        assert len(reg) > 0
