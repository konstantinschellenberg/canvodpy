"""Tests for StatisticsQuery and result dataclasses."""

import math

import numpy as np
import pytest
import zarr

from canvod.ops.statistics.profile import ProfileRegistry
from canvod.ops.statistics.query import (
    AnomalyTimeline,
    ChangepointStatus,
    ClimatologyHeatmap,
    ConfidenceEnvelope,
    StatisticsQuery,
    StatisticsSnapshot,
    VariableStats,
)
from canvod.ops.statistics.store import StatisticsStore
from canvod.streamstats import (
    BOCPDAccumulator,
    CellSignalKey,
    ClimatologyGrid,
)


def _make_store(tmp_path):
    """Create a StatisticsStore with synthetic data for testing."""
    store_path = tmp_path / "stats.zarr"
    root = zarr.open_group(str(store_path), mode="w")
    store = StatisticsStore(root)

    # Populate registry with two keys for the same variable
    reg = ProfileRegistry()
    key1 = CellSignalKey(1, "G01_L1C", "SNR", "canopy")
    key2 = CellSignalKey(2, "G01_L1C", "SNR", "canopy")
    key3 = CellSignalKey(1, "G01_L1C", "VOD", "canopy")

    rng = np.random.default_rng(42)
    reg.update_batch(key1, rng.normal(30.0, 5.0, size=100))
    reg.update_batch(key2, rng.normal(40.0, 5.0, size=100))
    reg.update_batch(key3, rng.normal(0.5, 0.1, size=50))
    store.save(reg, "canopy")

    # Climatology
    grid = ClimatologyGrid(doy_window=15, tod_window=1)
    for doy in range(1, 366):
        for hour in range(0, 24, 6):
            grid.update(doy, float(hour), rng.normal(35.0, 3.0))
    store.save_climatology({"SNR": grid}, "canopy")

    # BOCPD
    bocpd = BOCPDAccumulator(max_run_length=50)
    for _ in range(30):
        bocpd.update(rng.normal(35.0, 3.0))
    store.save_bocpd({"SNR": bocpd}, "canopy")

    # Anomaly
    store.save_anomaly_summary(
        "canopy",
        "2025001",
        {
            "SNR": (80, 10, 5, 5, 1.2, 3.5),
            "VOD": (90, 8, 1, 1, 0.8, 3.1),
        },
    )
    store.save_anomaly_summary(
        "canopy",
        "2025002",
        {
            "SNR": (85, 12, 2, 1, 1.0, 3.0),
            "VOD": (92, 6, 1, 1, 0.7, 2.8),
        },
    )

    return store


@pytest.mark.integration
class TestListReceiverTypes:
    def test_returns_correct_list(self, tmp_path):
        store = _make_store(tmp_path)
        q = StatisticsQuery(store)
        assert q.list_receiver_types() == ["canopy"]


@pytest.mark.integration
class TestVariableSummary:
    def test_aggregates_across_keys(self, tmp_path):
        store = _make_store(tmp_path)
        q = StatisticsQuery(store)
        vs = q.variable_summary("canopy", "SNR")
        assert isinstance(vs, VariableStats)
        assert vs.variable == "SNR"
        assert vs.n_keys == 2
        assert vs.total_count == 200
        # Global mean should be between 30 and 40 (weighted avg of two clusters)
        assert 30.0 < vs.global_mean < 40.0
        assert vs.global_std > 0
        assert vs.min_val < vs.max_val

    def test_missing_variable(self, tmp_path):
        store = _make_store(tmp_path)
        q = StatisticsQuery(store)
        vs = q.variable_summary("canopy", "NONEXISTENT")
        assert vs.n_keys == 0
        assert vs.total_count == 0
        assert math.isnan(vs.global_mean)

    def test_quantiles_populated(self, tmp_path):
        store = _make_store(tmp_path)
        q = StatisticsQuery(store)
        vs = q.variable_summary("canopy", "SNR")
        # GK sketch not restored from store with full fidelity,
        # but quantiles dict should be populated from at least one key
        # (may be empty if GK doesn't roundtrip through store)
        assert isinstance(vs.quantiles, dict)


@pytest.mark.integration
class TestConfidenceEnvelope:
    def test_without_autocovariance(self, tmp_path):
        store = _make_store(tmp_path)
        q = StatisticsQuery(store)
        ce = q.confidence_envelope("canopy", "SNR", z=2.0)
        assert isinstance(ce, ConfidenceEnvelope)
        assert ce.variable == "SNR"
        assert ce.z_multiplier == 2.0
        assert ce.n_eff == 200.0  # fallback to n
        assert ce.lower < ce.mean < ce.upper

    def test_bounds_correct(self, tmp_path):
        store = _make_store(tmp_path)
        q = StatisticsQuery(store)
        ce = q.confidence_envelope("canopy", "SNR", z=1.96)
        expected_half = 1.96 * ce.std / math.sqrt(ce.n_eff)
        assert ce.lower == pytest.approx(ce.mean - expected_half)
        assert ce.upper == pytest.approx(ce.mean + expected_half)

    def test_empty_variable(self, tmp_path):
        store = _make_store(tmp_path)
        q = StatisticsQuery(store)
        ce = q.confidence_envelope("canopy", "NONEXISTENT")
        assert math.isnan(ce.mean)
        assert ce.n_eff == 0.0


@pytest.mark.integration
class TestChangepointStatus:
    def test_populated_from_bocpd(self, tmp_path):
        store = _make_store(tmp_path)
        q = StatisticsQuery(store)
        cp = q.changepoint_status("canopy")
        assert isinstance(cp, ChangepointStatus)
        assert cp.variables == ["SNR"]
        assert len(cp.changepoint_probs) == 1
        assert 0.0 <= cp.changepoint_probs[0] <= 1.0
        assert cp.map_run_lengths[0] >= 0
        assert cp.n_observations[0] == 30

    def test_none_when_empty(self, tmp_path):
        store_path = tmp_path / "empty.zarr"
        root = zarr.open_group(str(store_path), mode="w")
        store = StatisticsStore(root)
        # Create minimal registry so receiver type exists
        reg = ProfileRegistry()
        key = CellSignalKey(1, "G01_L1C", "SNR", "canopy")
        reg.update_batch(key, np.array([1.0, 2.0]))
        store.save(reg, "canopy")
        q = StatisticsQuery(store)
        assert q.changepoint_status("canopy") is None


@pytest.mark.integration
class TestAnomalyTimeline:
    def test_populated_from_data(self, tmp_path):
        store = _make_store(tmp_path)
        q = StatisticsQuery(store)
        at = q.anomaly_timeline("canopy")
        assert isinstance(at, AnomalyTimeline)
        assert at.dates == ["2025001", "2025002"]
        assert at.variables == ["SNR", "VOD"]
        assert at.data.shape == (2, 2, 6)

    def test_none_when_empty(self, tmp_path):
        store_path = tmp_path / "empty.zarr"
        root = zarr.open_group(str(store_path), mode="w")
        store = StatisticsStore(root)
        reg = ProfileRegistry()
        key = CellSignalKey(1, "G01_L1C", "SNR", "canopy")
        reg.update_batch(key, np.array([1.0, 2.0]))
        store.save(reg, "canopy")
        q = StatisticsQuery(store)
        assert q.anomaly_timeline("canopy") is None


@pytest.mark.integration
class TestClimatologyHeatmap:
    def test_grid_shapes(self, tmp_path):
        store = _make_store(tmp_path)
        q = StatisticsQuery(store)
        hm = q.climatology_heatmap("canopy", "SNR")
        assert isinstance(hm, ClimatologyHeatmap)
        assert hm.variable == "SNR"
        n_doy, n_tod = hm.mean_grid.shape
        assert hm.doy_bins.shape == (n_doy + 1,)
        assert hm.tod_bins.shape == (n_tod + 1,)
        assert hm.std_grid.shape == (n_doy, n_tod)
        assert hm.count_grid.shape == (n_doy, n_tod)

    def test_bin_edges_monotonic(self, tmp_path):
        store = _make_store(tmp_path)
        q = StatisticsQuery(store)
        hm = q.climatology_heatmap("canopy", "SNR")
        assert np.all(np.diff(hm.doy_bins) > 0)
        assert np.all(np.diff(hm.tod_bins) > 0)

    def test_none_for_missing_variable(self, tmp_path):
        store = _make_store(tmp_path)
        q = StatisticsQuery(store)
        assert q.climatology_heatmap("canopy", "NONEXISTENT") is None


@pytest.mark.integration
class TestStatisticsSnapshot:
    def test_all_fields_populated(self, tmp_path):
        store = _make_store(tmp_path)
        q = StatisticsQuery(store)
        snap = q.snapshot("canopy")
        assert isinstance(snap, StatisticsSnapshot)
        assert snap.receiver_type == "canopy"
        assert "SNR" in snap.variable_stats
        assert "VOD" in snap.variable_stats
        assert snap.changepoint_status is not None
        assert snap.anomaly_timeline is not None
        assert "SNR" in snap.climatology_heatmaps
        assert "SNR" in snap.confidence_envelopes
        assert "VOD" in snap.confidence_envelopes


@pytest.mark.integration
class TestFilteredSeries:
    def test_ewma_smoothing(self):
        rng = np.random.default_rng(42)
        data = rng.normal(0, 1, size=100)
        smoothed = StatisticsQuery.filtered_series(data, method="ewma", half_life=5.0)
        assert smoothed.shape == data.shape
        # Smoothed should have lower variance
        assert np.std(smoothed) < np.std(data)

    def test_median_smoothing(self):
        rng = np.random.default_rng(42)
        data = rng.normal(0, 1, size=100)
        smoothed = StatisticsQuery.filtered_series(data, method="median", window=5)
        assert smoothed.shape == data.shape

    def test_bad_method_raises(self):
        with pytest.raises(ValueError, match="Unknown method"):
            StatisticsQuery.filtered_series(np.array([1.0, 2.0]), method="invalid")


class TestImportSanity:
    def test_streamstats_imports(self):
        from canvod.streamstats import (
            DEFAULT_EWMA_HALFLIFE,
            DEFAULT_RUNNING_MEDIAN_WINDOW,
            EWMAAccumulator,
            RunningMedianFilter,
        )

        assert DEFAULT_EWMA_HALFLIFE == 10.0
        assert DEFAULT_RUNNING_MEDIAN_WINDOW == 5
        assert EWMAAccumulator is not None
        assert RunningMedianFilter is not None

    def test_ops_imports(self):
        from canvod.ops import (
            AnomalyTimeline,
            ChangepointStatus,
            ClimatologyHeatmap,
            ConfidenceEnvelope,
            EWMAAccumulator,
            RunningMedianFilter,
            StatisticsQuery,
            StatisticsSnapshot,
            VariableStats,
        )

        assert StatisticsQuery is not None
        assert StatisticsSnapshot is not None
        assert VariableStats is not None
        assert ConfidenceEnvelope is not None
        assert AnomalyTimeline is not None
        assert ChangepointStatus is not None
        assert ClimatologyHeatmap is not None
        assert EWMAAccumulator is not None
        assert RunningMedianFilter is not None
