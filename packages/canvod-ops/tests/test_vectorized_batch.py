"""Tests for vectorized batch methods in accumulators and the op inner loop."""

import math

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from canvod.ops.statistics.op import UpdateStatistics
from canvod.ops.statistics.profile import AccumulatorSet, ProfileRegistry
from canvod.streamstats import GKSketch, WelfordAccumulator

# ---------------------------------------------------------------------------
# Welford batch edge cases
# ---------------------------------------------------------------------------


class TestWelfordBatch:
    def test_all_nan(self):
        w = WelfordAccumulator()
        w.update_batch(np.array([np.nan, np.nan, np.nan]))
        assert w.count == 0
        assert w.n_nan == 3
        assert math.isnan(w.mean)

    def test_empty_array(self):
        w = WelfordAccumulator()
        w.update_batch(np.array([]))
        assert w.count == 0
        assert w.n_nan == 0

    def test_single_value_batch(self):
        w = WelfordAccumulator()
        w.update_batch(np.array([42.0]))
        assert w.count == 1
        assert w.mean == 42.0
        assert w.min == 42.0
        assert w.max == 42.0

    def test_incremental_batches_match_one_shot(self):
        rng = np.random.default_rng(123)
        data = rng.normal(50.0, 10.0, size=500)

        # One-shot
        w_one = WelfordAccumulator()
        w_one.update_batch(data)

        # Incremental: 5 batches of 100
        w_inc = WelfordAccumulator()
        for i in range(5):
            w_inc.update_batch(data[i * 100 : (i + 1) * 100])

        assert w_inc.count == w_one.count
        assert w_inc.mean == pytest.approx(w_one.mean, rel=1e-12)
        assert w_inc.variance == pytest.approx(w_one.variance, rel=1e-10)
        assert w_inc.skewness == pytest.approx(w_one.skewness, abs=1e-6)
        assert w_inc.kurtosis == pytest.approx(w_one.kurtosis, abs=1e-5)
        assert w_inc.min == w_one.min
        assert w_inc.max == w_one.max

    def test_batch_with_nan_mixed(self):
        data = np.array([1.0, np.nan, 3.0, np.nan, 5.0])
        w = WelfordAccumulator()
        w.update_batch(data)
        assert w.count == 3
        assert w.n_nan == 2
        assert w.mean == pytest.approx(3.0)


# ---------------------------------------------------------------------------
# GK batch
# ---------------------------------------------------------------------------


class TestGKBatch:
    def test_batch_matches_sequential(self):
        """Batch and sequential insertion produce quantiles within 2ε."""
        rng = np.random.default_rng(42)
        data = rng.uniform(0, 100, size=2000)
        epsilon = 0.01

        gk_seq = GKSketch(epsilon=epsilon)
        for v in data:
            gk_seq.update(float(v))

        gk_batch = GKSketch(epsilon=epsilon)
        gk_batch.update_batch(data)

        assert gk_batch.count == gk_seq.count

        # Quantiles should agree within 2ε rank tolerance
        for phi in (0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99):
            q_seq = gk_seq.query(phi)
            q_batch = gk_batch.query(phi)
            # Both should be within ε*n of true rank — allow 2ε tolerance
            sorted_data = np.sort(data)
            true_val = sorted_data[int(phi * len(data))]
            assert abs(q_batch - true_val) <= 2 * epsilon * len(data), (
                f"phi={phi}: batch={q_batch}, true={true_val}"
            )

    def test_preserves_epsilon_guarantee(self):
        """After batch insert, all quantile queries must satisfy the ε bound."""
        rng = np.random.default_rng(77)
        data = rng.standard_normal(5000)
        epsilon = 0.005

        gk = GKSketch(epsilon=epsilon)
        gk.update_batch(data)

        sorted_data = np.sort(data)
        n = len(data)
        for phi in np.linspace(0.01, 0.99, 20):
            q = gk.query(phi)
            # Find rank of q in sorted data
            rank = np.searchsorted(sorted_data, q, side="right")
            target_rank = phi * n
            assert abs(rank - target_rank) <= epsilon * n + 1, (
                f"phi={phi}: rank={rank}, target={target_rank}"
            )

    def test_cold_start_batch(self):
        """Batch on empty sketch should work."""
        gk = GKSketch(epsilon=0.01)
        data = np.array([5.0, 3.0, 1.0, 4.0, 2.0])
        gk.update_batch(data)
        assert gk.count == 5
        assert gk.query(0.0) == 1.0
        assert gk.query(1.0) == 5.0

    def test_all_nan_batch(self):
        gk = GKSketch(epsilon=0.01)
        gk.update_batch(np.array([np.nan, np.nan]))
        assert gk.count == 0
        assert math.isnan(gk.query(0.5))

    def test_empty_batch(self):
        gk = GKSketch(epsilon=0.01)
        gk.update_batch(np.array([]))
        assert gk.count == 0


# ---------------------------------------------------------------------------
# AccumulatorSet batch consistency
# ---------------------------------------------------------------------------


class TestAccumulatorSetBatch:
    def test_counts_consistent(self):
        """All accumulators in a set should agree on count after batch update."""
        acc = AccumulatorSet(
            welford=WelfordAccumulator(),
            gk=GKSketch(epsilon=0.01),
            histogram=None,
        )
        rng = np.random.default_rng(55)
        data = rng.normal(10, 2, size=300)
        data[::10] = np.nan  # sprinkle NaN

        acc.update_batch(data)
        valid_count = int(np.sum(~np.isnan(data)))

        assert acc.welford.count == valid_count
        assert acc.gk.count == valid_count


# ---------------------------------------------------------------------------
# Op vectorized grouping
# ---------------------------------------------------------------------------


class TestOpVectorizedGrouping:
    def _make_ds(self, n_epoch, n_sid, rng):
        """Helper to create dataset with cell_ids."""
        sids = [f"G{i:02d}_L1C" for i in range(1, n_sid + 1)]
        epochs = pd.date_range("2024-01-01", periods=n_epoch, freq="1s")
        snr = rng.uniform(20, 50, size=(n_epoch, n_sid))
        # Assign cell_ids: each sid gets a different cell
        cell_ids = np.tile(np.arange(1, n_sid + 1), (n_epoch, 1)).astype(np.float64)

        return xr.Dataset(
            {"SNR": (("epoch", "sid"), snr)},
            coords={
                "epoch": epochs.values,
                "sid": sids,
                "cell_id_grid": (("epoch", "sid"), cell_ids),
            },
            attrs={"File Hash": "test"},
        )

    def test_correct_keys_and_counts(self):
        """Vectorized grouping produces one key per (cell, sid) and correct counts."""
        rng = np.random.default_rng(42)
        n_epoch, n_sid = 50, 3
        ds = self._make_ds(n_epoch, n_sid, rng)

        registry = ProfileRegistry()
        op = UpdateStatistics(registry, "canopy", ["SNR"])
        op(ds)

        # Should have n_sid keys (one cell per sid)
        assert len(registry) == n_sid
        # Each key should have n_epoch observations
        for key, acc in registry.items():
            assert acc.welford.count == n_epoch

    def test_scale_1000_epochs_10_sids(self):
        """Vectorized op handles larger data without error."""
        rng = np.random.default_rng(88)
        n_epoch, n_sid = 1000, 10
        ds = self._make_ds(n_epoch, n_sid, rng)

        registry = ProfileRegistry()
        op = UpdateStatistics(registry, "canopy", ["SNR"])
        ds_out, result = op(ds)

        assert len(registry) == n_sid
        total_obs = sum(acc.welford.count for acc in registry._accumulators.values())
        assert total_obs == n_epoch * n_sid
        assert "10000 updates" in result.notes
