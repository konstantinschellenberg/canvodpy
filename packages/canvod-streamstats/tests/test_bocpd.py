"""Tests for Bayesian Online Changepoint Detection (BOCPD)."""

import numpy as np
import pytest

from canvod.streamstats.bayesian.bocpd import BOCPDAccumulator, BOCPDResult


class TestBOCPDEmpty:
    def test_empty_count(self):
        acc = BOCPDAccumulator()
        assert acc.count == 0

    def test_empty_result_defaults(self):
        acc = BOCPDAccumulator()
        r = acc.result
        assert r.map_run_length == 0
        assert r.changepoint_prob == 0.0
        assert np.isnan(r.predictive_mean)
        assert np.isnan(r.predictive_std)
        assert r.n_observations == 0

    def test_empty_changepoint_prob(self):
        acc = BOCPDAccumulator()
        assert acc.changepoint_prob == 0.0

    def test_empty_map_run_length(self):
        acc = BOCPDAccumulator()
        assert acc.map_run_length == 0


class TestBOCPDConstantSignal:
    def test_run_length_grows(self):
        """Constant signal: run length should grow toward N."""
        rng = np.random.default_rng(42)
        acc = BOCPDAccumulator(hazard_lambda=100.0)
        # Feed constant values with tiny noise
        for _ in range(50):
            acc.update(5.0 + rng.normal(0, 0.01))

        assert acc.count == 50
        # MAP run length should be near the number of observations
        assert acc.map_run_length >= 30

    def test_low_changepoint_prob(self):
        """Constant signal should have low changepoint probability."""
        acc = BOCPDAccumulator(hazard_lambda=100.0)
        for _ in range(50):
            acc.update(5.0)
        assert acc.changepoint_prob < 0.1


class TestBOCPDMeanShift:
    def test_changepoint_detected(self):
        """Abrupt mean shift N(0,1) → N(5,1): changepoint should be detected."""
        rng = np.random.default_rng(123)
        acc = BOCPDAccumulator(hazard_lambda=30.0, kappa0=0.1)

        # Phase 1: N(0, 0.5)
        for _ in range(100):
            acc.update(rng.normal(0, 0.5))

        # Record run length before shift
        rl_before = acc.map_run_length

        # Phase 2: N(5, 0.5) — large mean shift
        for _ in range(20):
            acc.update(rng.normal(5, 0.5))

        # After the shift, the run length should have reset (be small)
        assert acc.map_run_length < rl_before
        assert acc.count == 120

    def test_changepoint_prob_spikes(self):
        """Changepoint probability should spike around the mean shift."""
        rng = np.random.default_rng(456)
        acc = BOCPDAccumulator(hazard_lambda=50.0, kappa0=0.1)

        # Phase 1: stable — build up long run length
        for _ in range(150):
            acc.update(rng.normal(0, 0.3))

        cp_before = acc.changepoint_prob

        # Phase 2: very large shift to ensure detection
        for _ in range(15):
            acc.update(rng.normal(20, 0.3))

        cp_after = acc.changepoint_prob
        # Changepoint prob should clearly increase after the shift
        assert cp_after > cp_before


class TestBOCPDNaN:
    def test_nan_skipped(self):
        acc = BOCPDAccumulator()
        acc.update(1.0)
        acc.update(float("nan"))
        acc.update(2.0)
        assert acc.count == 2

    def test_nan_batch(self):
        acc = BOCPDAccumulator()
        data = np.array([1.0, float("nan"), 2.0, float("nan"), 3.0])
        acc.update_batch(data)
        assert acc.count == 3


class TestBOCPDUpdateBatch:
    def test_batch_matches_sequential(self):
        """update_batch should produce same result as sequential updates."""
        rng = np.random.default_rng(789)
        data = rng.normal(0, 1, 50)

        acc_seq = BOCPDAccumulator(hazard_lambda=20.0)
        for x in data:
            acc_seq.update(x)

        acc_batch = BOCPDAccumulator(hazard_lambda=20.0)
        acc_batch.update_batch(data)

        assert acc_seq.count == acc_batch.count
        np.testing.assert_allclose(
            acc_seq.run_length_distribution,
            acc_batch.run_length_distribution,
        )


class TestBOCPDSerialization:
    def test_to_array_length(self):
        """Serialized array length = 7 + 5*(R+1)."""
        R = 100
        acc = BOCPDAccumulator(max_run_length=R)
        arr = acc.to_array()
        assert len(arr) == 7 + 5 * (R + 1)

    def test_roundtrip(self):
        """to_array / from_array roundtrip preserves state."""
        rng = np.random.default_rng(101)
        acc = BOCPDAccumulator(max_run_length=50, hazard_lambda=15.0)
        for _ in range(30):
            acc.update(rng.normal(0, 1))

        arr = acc.to_array()
        restored = BOCPDAccumulator.from_array(arr)

        assert restored.count == acc.count
        assert restored.map_run_length == acc.map_run_length
        np.testing.assert_allclose(restored.changepoint_prob, acc.changepoint_prob)
        np.testing.assert_allclose(
            restored.run_length_distribution,
            acc.run_length_distribution,
        )

    def test_roundtrip_preserves_result(self):
        rng = np.random.default_rng(202)
        acc = BOCPDAccumulator(max_run_length=50)
        for _ in range(20):
            acc.update(rng.normal(0, 1))

        r1 = acc.result
        restored = BOCPDAccumulator.from_array(acc.to_array())
        r2 = restored.result

        assert r1.map_run_length == r2.map_run_length
        np.testing.assert_allclose(r1.changepoint_prob, r2.changepoint_prob)
        np.testing.assert_allclose(r1.predictive_mean, r2.predictive_mean)


class TestBOCPDMerge:
    def test_merge_raises(self):
        acc1 = BOCPDAccumulator()
        acc2 = BOCPDAccumulator()
        with pytest.raises(NotImplementedError, match="sequential"):
            acc1.merge(acc2)


class TestBOCPDHazardSensitivity:
    def test_short_lambda_more_sensitive(self):
        """Shorter hazard timescale should detect changes more readily."""
        rng = np.random.default_rng(303)
        data = np.concatenate([rng.normal(0, 0.5, 50), rng.normal(5, 0.5, 20)])

        acc_short = BOCPDAccumulator(hazard_lambda=7.0, kappa0=0.1)
        acc_long = BOCPDAccumulator(hazard_lambda=50.0, kappa0=0.1)

        acc_short.update_batch(data)
        acc_long.update_batch(data)

        # Short λ should have shorter MAP run length (detected change sooner)
        assert acc_short.map_run_length <= acc_long.map_run_length


class TestBOCPDResult:
    def test_result_is_frozen(self):
        r = BOCPDResult(
            map_run_length=5,
            changepoint_prob=0.1,
            predictive_mean=0.0,
            predictive_std=1.0,
            n_observations=10,
        )
        with pytest.raises(AttributeError):
            r.map_run_length = 10  # type: ignore[misc]
