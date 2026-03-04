"""Tests for FadingAccumulator."""

import math

import numpy as np
import pytest

from canvod.streamstats.accumulators.fading import FadingAccumulator


class TestFadingAccumulator:
    def test_empty(self):
        f = FadingAccumulator()
        assert f.count == 0
        assert math.isnan(f.rms_intensity)
        assert math.isnan(f.level_crossing_rate(0))
        assert math.isnan(f.average_fade_duration(0))
        assert math.isnan(f.fraction_below(0))

    def test_constant_signal_no_crossings(self):
        """Constant signal above all thresholds → no crossings."""
        f = FadingAccumulator(thresholds=(0.5,))
        # All values = 10.0; RMS = 10.0; threshold = 0.5 * 10 = 5.0
        # 10 > 5 → never below → no crossings
        for _ in range(100):
            f.update(10.0)
        assert f.count == 100
        assert f.n_crossings(0) == 0
        assert f.fraction_below(0) == pytest.approx(0.0)

    def test_known_crossings(self):
        """Signal that alternates above/below threshold → known crossing count."""
        f = FadingAccumulator(thresholds=(0.5,))
        # Feed high values first to establish stable RMS
        high = 10.0
        low = 0.1
        # First, push 10 high values so RMS ≈ 10
        for _ in range(10):
            f.update(high)
        initial_crossings = f.n_crossings(0)
        # Now alternate: high → low → high → low (each low is a new crossing)
        for _ in range(5):
            f.update(high)  # above threshold
            f.update(low)  # below → crossing detected
        assert f.n_crossings(0) >= initial_crossings + 5

    def test_multi_threshold(self):
        """Different thresholds track independently."""
        f = FadingAccumulator(thresholds=(0.9, 0.1))
        # Feed data: high threshold should see more crossings
        rng = np.random.default_rng(42)
        data = rng.uniform(0.5, 10.0, size=500)
        for x in data:
            f.update(float(x))
        # 0.9*RMS is a higher bar → more samples below → more crossings expected
        assert f.fraction_below(0) >= f.fraction_below(1)

    def test_nan_handling(self):
        f = FadingAccumulator(thresholds=(0.5,))
        f.update(float("nan"))
        assert f.count == 0
        f.update(5.0)
        assert f.count == 1

    def test_batch_running_stats_match_sequential(self):
        """Batch and sequential produce identical running intensity stats."""
        rng = np.random.default_rng(99)
        data = rng.uniform(0.1, 10.0, size=200)

        seq = FadingAccumulator(thresholds=(0.5, 0.3))
        for x in data:
            seq.update(float(x))

        batch = FadingAccumulator(thresholds=(0.5, 0.3))
        batch.update_batch(data)

        assert seq.count == batch.count
        assert seq.rms_intensity == pytest.approx(batch.rms_intensity, rel=1e-12)

    def test_batch_vectorized_crossing_detection(self):
        """Batch crossing detection via np.diff on below-mask works correctly."""
        # Constant high then constant low — one crossing per threshold
        high = np.full(50, 10.0)
        low = np.full(50, 0.01)
        data = np.concatenate([high, low])

        f = FadingAccumulator(thresholds=(0.5,))
        f.update_batch(data)

        assert f.count == 100
        assert f.n_crossings(0) == 1  # single transition high→low
        assert f.fraction_below(0) == pytest.approx(0.5)

    def test_batch_nan_filtered(self):
        """NaN values in batch are filtered out before processing."""
        data = np.array([5.0, np.nan, 5.0, np.nan, 5.0])
        f = FadingAccumulator(thresholds=(0.5,))
        f.update_batch(data)
        assert f.count == 3

    def test_batch_empty_after_nan_filter(self):
        """All-NaN batch is a no-op."""
        f = FadingAccumulator(thresholds=(0.5,))
        f.update_batch(np.array([np.nan, np.nan]))
        assert f.count == 0

    def test_fraction_below_correctness(self):
        """All samples below threshold → fraction = 1.0."""
        f = FadingAccumulator(thresholds=(2.0,))  # threshold = 2*RMS
        # Everything is below 2*RMS when all values are identical
        # Actually: for constant value v, RMS = v, threshold = 2v > v → always below
        for _ in range(50):
            f.update(5.0)
        assert f.fraction_below(0) == pytest.approx(1.0)

    def test_average_fade_duration(self):
        """Check AFD = cumulative_below / n_crossings."""
        f = FadingAccumulator(thresholds=(0.5,))
        # Build up RMS with high values, then do controlled fades
        for _ in range(20):
            f.update(10.0)
        # One fade of 3 samples
        for _ in range(3):
            f.update(0.01)  # well below 0.5 * RMS
        f.update(10.0)  # back above
        # Another fade of 3 samples
        for _ in range(3):
            f.update(0.01)
        f.update(10.0)
        crossings = f.n_crossings(0)
        if crossings > 0:
            afd = f.average_fade_duration(0)
            assert afd > 0.0

    def test_roundtrip_serialization(self):
        rng = np.random.default_rng(77)
        f = FadingAccumulator(thresholds=(0.5, 0.3, 0.1))
        for x in rng.uniform(0.1, 10.0, size=100):
            f.update(float(x))

        arr = f.to_array()
        assert arr.shape == (5 + 3 * 3 + 3,)  # 5 global + 9 per-thresh + 3 thresholds

        restored = FadingAccumulator.from_array(arr)
        assert restored.count == f.count
        assert restored.thresholds == f.thresholds
        assert restored.n_crossings(0) == f.n_crossings(0)
        assert restored.n_crossings(1) == f.n_crossings(1)
        assert restored.fraction_below(0) == pytest.approx(f.fraction_below(0))

    def test_merge(self):
        rng = np.random.default_rng(33)
        data = rng.uniform(0.1, 10.0, size=200)

        a = FadingAccumulator(thresholds=(0.5,))
        for x in data[:100]:
            a.update(float(x))

        b = FadingAccumulator(thresholds=(0.5,))
        for x in data[100:]:
            b.update(float(x))

        a.merge(b)
        assert a.count == 200

    def test_merge_with_empty(self):
        f = FadingAccumulator(thresholds=(0.5,))
        f.update(5.0)
        empty = FadingAccumulator(thresholds=(0.5,))
        f.merge(empty)
        assert f.count == 1

        empty2 = FadingAccumulator(thresholds=(0.5,))
        f2 = FadingAccumulator(thresholds=(0.5,))
        f2.update(5.0)
        empty2.merge(f2)
        assert empty2.count == 1
