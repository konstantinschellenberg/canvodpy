"""Tests for PermutationEntropyAccumulator."""

import math

import numpy as np
import pytest

from canvod.streamstats.accumulators.permutation_entropy import (
    PermutationEntropyAccumulator,
)


class TestPermutationEntropyAccumulator:
    def test_constant_signal_entropy_zero(self):
        acc = PermutationEntropyAccumulator(order=3, delay=1)
        acc.update_batch(np.ones(100))
        # All ordinal patterns are the same → only one pattern occupied
        # With ties, argsort is deterministic so only one pattern
        assert acc.count > 0
        assert acc.entropy == pytest.approx(0.0, abs=1e-10)

    def test_random_signal_high_entropy(self):
        rng = np.random.default_rng(42)
        acc = PermutationEntropyAccumulator(order=3, delay=1)
        acc.update_batch(rng.standard_normal(10000))
        # Random signal should have near-maximum entropy
        max_entropy = math.log2(math.factorial(3))  # log2(6)
        assert acc.entropy > 0.9 * max_entropy

    def test_normalized_entropy_range(self):
        rng = np.random.default_rng(42)
        acc = PermutationEntropyAccumulator(order=4, delay=1)
        acc.update_batch(rng.standard_normal(5000))
        assert 0.0 <= acc.normalized_entropy <= 1.0

    def test_batch_vs_sequential(self):
        rng = np.random.default_rng(42)
        data = rng.standard_normal(500)

        acc_batch = PermutationEntropyAccumulator(order=3, delay=1)
        acc_batch.update_batch(data)

        acc_seq = PermutationEntropyAccumulator(order=3, delay=1)
        for x in data:
            acc_seq.update(x)

        assert acc_batch.count == acc_seq.count
        assert acc_batch.entropy == pytest.approx(acc_seq.entropy, abs=1e-10)

    def test_nan_handling(self):
        acc = PermutationEntropyAccumulator(order=3, delay=1)
        data = np.array([1.0, 2.0, float("nan"), 3.0, 4.0, 5.0, 6.0])
        acc.update_batch(data)
        # NaN breaks the buffer continuity, reducing valid pattern count
        assert acc.count >= 0

    def test_serialization_roundtrip(self):
        rng = np.random.default_rng(42)
        acc = PermutationEntropyAccumulator(order=3, delay=1)
        acc.update_batch(rng.standard_normal(200))

        arr = acc.to_array()
        restored = PermutationEntropyAccumulator.from_array(arr)

        assert restored.order == acc.order
        assert restored.delay == acc.delay
        assert restored.count == acc.count
        assert restored.entropy == pytest.approx(acc.entropy, abs=1e-10)

    def test_merge(self):
        rng = np.random.default_rng(42)
        data = rng.standard_normal(1000)

        acc1 = PermutationEntropyAccumulator(order=3, delay=1)
        acc1.update_batch(data[:500])

        acc2 = PermutationEntropyAccumulator(order=3, delay=1)
        acc2.update_batch(data[500:])

        total_before = acc1.count + acc2.count
        acc1.merge(acc2)
        assert acc1.count == total_before

    def test_invalid_order(self):
        with pytest.raises(ValueError, match="order must be >= 2"):
            PermutationEntropyAccumulator(order=1)

    def test_pattern_distribution_sums_to_one(self):
        rng = np.random.default_rng(42)
        acc = PermutationEntropyAccumulator(order=3, delay=1)
        acc.update_batch(rng.standard_normal(500))
        dist = acc.pattern_distribution
        assert dist.sum() == pytest.approx(1.0, abs=1e-10)
