"""Tests for CircularAccumulator."""

import numpy as np
import pytest

from canvod.streamstats.accumulators.circular import CircularAccumulator


class TestCircularAccumulatorEmpty:
    def test_empty_returns_nan(self):
        acc = CircularAccumulator()
        assert acc.count == 0
        assert np.isnan(acc.circular_mean)
        assert np.isnan(acc.mean_resultant_length)
        assert np.isnan(acc.circular_variance)
        assert np.isnan(acc.von_mises_kappa)
        assert np.isnan(acc.rayleigh_statistic)
        assert np.isnan(acc.rayleigh_p_value)


class TestCircularAccumulatorUniform:
    def test_uniform_angles_low_concentration(self):
        """Uniformly spaced angles → R-bar ≈ 0, V ≈ 1."""
        acc = CircularAccumulator()
        for angle in [0, np.pi / 2, np.pi, 3 * np.pi / 2]:
            acc.update(angle)
        assert acc.count == 4
        assert acc.mean_resultant_length == pytest.approx(0.0, abs=1e-10)
        assert acc.circular_variance == pytest.approx(1.0, abs=1e-10)


class TestCircularAccumulatorConcentrated:
    def test_von_mises_concentrated(self):
        """Concentrated von Mises samples → high R-bar, low variance."""
        rng = np.random.default_rng(42)
        # Approximate von Mises with kappa=10 via rejection sampling
        angles = rng.vonmises(mu=0.0, kappa=10.0, size=500)
        acc = CircularAccumulator()
        acc.update_batch(angles)
        assert acc.count == 500
        assert acc.mean_resultant_length > 0.9
        assert acc.circular_variance < 0.1


class TestCircularAccumulatorKnownMean:
    def test_known_circular_mean(self):
        """Angles π/4 and 3π/4 → circular mean = π/2."""
        acc = CircularAccumulator()
        acc.update(np.pi / 4)
        acc.update(3 * np.pi / 4)
        assert acc.circular_mean == pytest.approx(np.pi / 2, abs=1e-10)


class TestCircularAccumulatorBatchVsSequential:
    def test_batch_matches_sequential(self):
        rng = np.random.default_rng(99)
        angles = rng.uniform(-np.pi, np.pi, size=100)

        seq = CircularAccumulator()
        for a in angles:
            seq.update(a)

        batch = CircularAccumulator()
        batch.update_batch(angles)

        assert seq.count == batch.count
        assert seq.circular_mean == pytest.approx(batch.circular_mean, abs=1e-12)
        assert seq.mean_resultant_length == pytest.approx(
            batch.mean_resultant_length, abs=1e-12
        )


class TestCircularAccumulatorNaN:
    def test_nan_handling(self):
        acc = CircularAccumulator()
        acc.update(np.nan)
        assert acc.count == 0

        acc.update_batch(np.array([np.nan, 0.0, np.nan, np.pi]))
        assert acc.count == 2


class TestCircularAccumulatorSerialization:
    def test_roundtrip(self):
        rng = np.random.default_rng(7)
        acc = CircularAccumulator()
        acc.update_batch(rng.uniform(-np.pi, np.pi, size=50))

        arr = acc.to_array()
        assert arr.shape == (3,)

        restored = CircularAccumulator.from_array(arr)
        assert restored.count == acc.count
        assert restored.circular_mean == pytest.approx(acc.circular_mean, abs=1e-14)
        assert restored.mean_resultant_length == pytest.approx(
            acc.mean_resultant_length, abs=1e-14
        )


class TestCircularAccumulatorMerge:
    def test_merge_sums_state(self):
        rng = np.random.default_rng(11)
        angles = rng.uniform(-np.pi, np.pi, size=100)

        full = CircularAccumulator()
        full.update_batch(angles)

        a = CircularAccumulator()
        a.update_batch(angles[:40])
        b = CircularAccumulator()
        b.update_batch(angles[40:])
        a.merge(b)

        assert a.count == full.count
        assert a.circular_mean == pytest.approx(full.circular_mean, abs=1e-12)


class TestCircularAccumulatorRayleigh:
    def test_uniform_high_p_value(self):
        """Uniformly distributed angles → Rayleigh p-value should be high."""
        rng = np.random.default_rng(123)
        angles = rng.uniform(-np.pi, np.pi, size=200)
        acc = CircularAccumulator()
        acc.update_batch(angles)
        # For uniform data, p-value should not reject uniformity
        assert acc.rayleigh_p_value > 0.05

    def test_concentrated_low_p_value(self):
        """Concentrated data → Rayleigh p-value should be low."""
        rng = np.random.default_rng(456)
        angles = rng.vonmises(mu=0.0, kappa=5.0, size=200)
        acc = CircularAccumulator()
        acc.update_batch(angles)
        assert acc.rayleigh_p_value < 0.01
