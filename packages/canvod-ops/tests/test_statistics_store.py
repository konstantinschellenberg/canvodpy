"""Tests for StatisticsStore (Zarr serialization)."""

import numpy as np
import pytest
import zarr

from canvod.ops.statistics.profile import ProfileRegistry
from canvod.ops.statistics.store import StatisticsStore
from canvod.streamstats import (
    BOCPDAccumulator,
    CellSignalKey,
    ClimatologyGrid,
)


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


@pytest.mark.integration
class TestClimatologyStore:
    def test_save_load_roundtrip(self, tmp_path):
        store_path = tmp_path / "stats.zarr"
        root = zarr.open_group(str(store_path), mode="w")
        store = StatisticsStore(root)

        grid = ClimatologyGrid()
        doys = np.array([100, 100, 100], dtype=np.int32)
        hours = np.array([12.0, 12.3, 12.7], dtype=np.float64)
        values = np.array([10.0, 20.0, 30.0], dtype=np.float64)
        grid.update_batch(doys, hours, values)

        store.save_climatology({"SNR": grid}, "canopy")

        # Re-open read-only and load
        root2 = zarr.open_group(str(store_path), mode="r")
        store2 = StatisticsStore(root2)
        loaded = store2.load_climatology("canopy")

        assert "SNR" in loaded
        mean, std, count = loaded["SNR"].climatology_at(100, 12.5)
        assert count == 3
        assert mean == pytest.approx(20.0)

    def test_empty_load_returns_empty(self, tmp_path):
        store_path = tmp_path / "stats.zarr"
        root = zarr.open_group(str(store_path), mode="w")
        store = StatisticsStore(root)

        assert store.load_climatology("canopy") == {}

    def test_multiple_variables(self, tmp_path):
        store_path = tmp_path / "stats.zarr"
        root = zarr.open_group(str(store_path), mode="w")
        store = StatisticsStore(root)

        grids = {}
        for var in ["SNR", "L1C"]:
            g = ClimatologyGrid()
            g.update_batch(
                np.array([50], dtype=np.int32),
                np.array([6.0], dtype=np.float64),
                np.array([42.0], dtype=np.float64),
            )
            grids[var] = g

        store.save_climatology(grids, "canopy")

        root2 = zarr.open_group(str(store_path), mode="r")
        loaded = StatisticsStore(root2).load_climatology("canopy")
        assert sorted(loaded.keys()) == ["L1C", "SNR"]


@pytest.mark.integration
class TestBOCPDStore:
    def test_save_load_roundtrip(self, tmp_path):
        store_path = tmp_path / "stats.zarr"
        root = zarr.open_group(str(store_path), mode="w")
        store = StatisticsStore(root)

        acc = BOCPDAccumulator()
        rng = np.random.default_rng(42)
        for val in rng.normal(0, 1, size=20):
            acc.update(float(val))

        store.save_bocpd({"SNR": acc}, "canopy")

        root2 = zarr.open_group(str(store_path), mode="r")
        loaded = StatisticsStore(root2).load_bocpd("canopy")

        assert "SNR" in loaded
        assert loaded["SNR"].count == 20
        assert loaded["SNR"].result.n_observations == 20

    def test_state_preserved_across_reload(self, tmp_path):
        store_path = tmp_path / "stats.zarr"
        root = zarr.open_group(str(store_path), mode="w")
        store = StatisticsStore(root)

        acc = BOCPDAccumulator()
        for val in [1.0, 2.0, 3.0, 4.0, 5.0]:
            acc.update(val)

        original_prob = acc.result.changepoint_prob
        original_run_len = acc.result.map_run_length

        store.save_bocpd({"SNR": acc}, "canopy")

        root2 = zarr.open_group(str(store_path), mode="r")
        loaded = StatisticsStore(root2).load_bocpd("canopy")

        assert loaded["SNR"].result.changepoint_prob == pytest.approx(original_prob)
        assert loaded["SNR"].result.map_run_length == original_run_len

    def test_empty_load_returns_empty(self, tmp_path):
        store_path = tmp_path / "stats.zarr"
        root = zarr.open_group(str(store_path), mode="w")
        store = StatisticsStore(root)

        assert store.load_bocpd("canopy") == {}


@pytest.mark.integration
class TestAnomalySummaryStore:
    def test_save_single_day(self, tmp_path):
        store_path = tmp_path / "stats.zarr"
        root = zarr.open_group(str(store_path), mode="w")
        store = StatisticsStore(root)

        summary = {
            "SNR": (100, 10, 3, 1, 0.8, 3.5),
            "L1C": (90, 15, 5, 2, 1.1, 4.2),
        }
        store.save_anomaly_summary("canopy", "2024-01-01", summary)

        dates, variables, data = store.load_anomaly_summaries("canopy")
        assert dates == ["2024-01-01"]
        assert variables == ["L1C", "SNR"]  # sorted
        assert data.shape == (1, 2, 6)

    def test_multi_day_append(self, tmp_path):
        store_path = tmp_path / "stats.zarr"
        root = zarr.open_group(str(store_path), mode="w")
        store = StatisticsStore(root)

        day1 = {"SNR": (100, 10, 3, 1, 0.8, 3.5)}
        day2 = {"SNR": (95, 12, 4, 2, 1.0, 4.0)}
        store.save_anomaly_summary("canopy", "2024-01-01", day1)
        store.save_anomaly_summary("canopy", "2024-01-02", day2)

        dates, variables, data = store.load_anomaly_summaries("canopy")
        assert dates == ["2024-01-01", "2024-01-02"]
        assert data.shape == (2, 1, 6)
        # Check day 2 values
        assert data[1, 0, 0] == pytest.approx(95.0)  # n_normal
        assert data[1, 0, 5] == pytest.approx(4.0)  # max_abs_z

    def test_empty_load(self, tmp_path):
        store_path = tmp_path / "stats.zarr"
        root = zarr.open_group(str(store_path), mode="w")
        store = StatisticsStore(root)

        dates, variables, data = store.load_anomaly_summaries("canopy")
        assert dates == []
        assert variables == []
        assert data.shape[0] == 0


@pytest.mark.integration
class TestIdempotencyRanges:
    """Test all 5 range-tracking pairs."""

    @pytest.fixture()
    def store(self, tmp_path):
        store_path = tmp_path / "stats.zarr"
        root = zarr.open_group(str(store_path), mode="w")
        return StatisticsStore(root)

    def test_epoch_range(self, store):
        assert not store.is_epoch_range_processed("canopy", "2024-01-01", "2024-01-02")
        store.record_epoch_range("canopy", "2024-01-01", "2024-01-02")
        assert store.is_epoch_range_processed("canopy", "2024-01-01", "2024-01-02")
        assert not store.is_epoch_range_processed("canopy", "2024-01-02", "2024-01-03")

    def test_climatology_range(self, store):
        assert not store.is_climatology_range_processed(
            "canopy", "2024-01-01", "2024-01-02"
        )
        store.record_climatology_range("canopy", "2024-01-01", "2024-01-02")
        assert store.is_climatology_range_processed(
            "canopy", "2024-01-01", "2024-01-02"
        )
        assert not store.is_climatology_range_processed(
            "canopy", "2024-01-02", "2024-01-03"
        )

    def test_bocpd_range(self, store):
        assert not store.is_bocpd_range_processed("canopy", "2024-01-01", "2024-01-02")
        store.record_bocpd_range("canopy", "2024-01-01", "2024-01-02")
        assert store.is_bocpd_range_processed("canopy", "2024-01-01", "2024-01-02")
        assert not store.is_bocpd_range_processed("ref", "2024-01-01", "2024-01-02")

    def test_anomaly_range(self, store):
        assert not store.is_anomaly_range_processed(
            "canopy", "2024-01-01", "2024-01-02"
        )
        store.record_anomaly_range("canopy", "2024-01-01", "2024-01-02")
        assert store.is_anomaly_range_processed("canopy", "2024-01-01", "2024-01-02")

    def test_pipeline_completed(self, store):
        assert not store.is_pipeline_completed("canopy", "2024-01-01", "2024-01-02")
        store.record_pipeline_completed("canopy", "2024-01-01", "2024-01-02")
        assert store.is_pipeline_completed("canopy", "2024-01-01", "2024-01-02")
        assert not store.is_pipeline_completed("canopy", "2024-01-03", "2024-01-04")
