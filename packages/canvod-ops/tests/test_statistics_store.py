"""Tests for StatisticsStore (Zarr serialization)."""

import numpy as np
import pytest
import zarr

from canvod.ops.statistics.profile import ProfileRegistry
from canvod.ops.statistics.store import StatisticsStore
from canvod.streamstats import CellSignalKey


@pytest.mark.integration
class TestStatisticsStore:
    def test_save_load_roundtrip(self, tmp_path):
        store_path = tmp_path / "stats.zarr"
        root = zarr.open_group(str(store_path), mode="w")
        store = StatisticsStore(root)

        reg = ProfileRegistry()
        key = CellSignalKey(1, "G01_L1C", "SNR", "canopy")
        data = np.array([25.0, 30.0, 35.0, 40.0, 45.0])
        reg.update_batch(key, data)

        store.save(reg, "canopy")

        # Load back
        root2 = zarr.open_group(str(store_path), mode="r")
        store2 = StatisticsStore(root2)
        loaded = store2.load("canopy")

        assert len(loaded) == 1
        assert key in loaded
        assert loaded[key].welford.count == 5
        assert loaded[key].welford.mean == pytest.approx(35.0)

    def test_quantile_snapshot_saved(self, tmp_path):
        store_path = tmp_path / "stats.zarr"
        root = zarr.open_group(str(store_path), mode="w")
        store = StatisticsStore(root)

        reg = ProfileRegistry()
        rng = np.random.default_rng(42)
        key = CellSignalKey(1, "G01_L1C", "SNR", "canopy")
        data = rng.uniform(20, 50, size=1000)
        reg.update_batch(key, data)

        store.save(reg, "canopy")

        # Read quantiles array directly
        root2 = zarr.open_group(str(store_path), mode="r")
        quantiles = np.asarray(root2["canopy"]["quantiles"])
        assert quantiles.shape[-1] == 11  # 11 default probs
        # Median should be roughly 35
        median_idx = 5  # 0.5 is index 5 in DEFAULT_QUANTILE_PROBS
        assert 30.0 < quantiles[0, 0, 0, median_idx] < 40.0

    def test_incremental_save(self, tmp_path):
        store_path = tmp_path / "stats.zarr"
        root = zarr.open_group(str(store_path), mode="w")
        store = StatisticsStore(root)

        # First save
        reg1 = ProfileRegistry()
        key = CellSignalKey(1, "G01_L1C", "SNR", "canopy")
        reg1.update_batch(key, np.array([10.0, 20.0]))
        store.save(reg1, "canopy")

        # Second save (overwrite)
        reg2 = ProfileRegistry()
        reg2.update_batch(key, np.array([30.0, 40.0]))
        root_w = zarr.open_group(str(store_path), mode="w")
        store_w = StatisticsStore(root_w)
        store_w.save(reg2, "canopy")

        # Load should reflect second save
        root3 = zarr.open_group(str(store_path), mode="r")
        loaded = StatisticsStore(root3).load("canopy")
        assert loaded[key].welford.count == 2
        assert loaded[key].welford.mean == pytest.approx(35.0)

    def test_idempotency(self, tmp_path):
        store_path = tmp_path / "stats.zarr"
        root = zarr.open_group(str(store_path), mode="w")
        store = StatisticsStore(root)

        assert not store.is_epoch_range_processed("canopy", "2024-01-01", "2024-01-02")

        store.record_epoch_range("canopy", "2024-01-01", "2024-01-02")
        assert store.is_epoch_range_processed("canopy", "2024-01-01", "2024-01-02")
        assert not store.is_epoch_range_processed("canopy", "2024-01-02", "2024-01-03")

    def test_list_receiver_types(self, tmp_path):
        store_path = tmp_path / "stats.zarr"
        root = zarr.open_group(str(store_path), mode="w")
        store = StatisticsStore(root)

        reg = ProfileRegistry()
        key_c = CellSignalKey(1, "G01_L1C", "SNR", "canopy")
        key_r = CellSignalKey(1, "G01_L1C", "SNR", "reference")
        reg.update(key_c, 25.0)
        reg.update(key_r, 30.0)

        store.save(reg, "canopy")
        store.save(reg, "reference")

        assert sorted(store.list_receiver_types()) == ["canopy", "reference"]
