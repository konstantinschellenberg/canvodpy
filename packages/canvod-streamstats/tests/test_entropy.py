"""Tests for Shannon, joint, and conditional entropy."""

import math

import numpy as np
import pytest

from canvod.streamstats.accumulators.histogram import StreamingHistogram
from canvod.streamstats.information.entropy import (
    conditional_entropy,
    joint_entropy,
    shannon_entropy,
    shannon_entropy_from_histogram,
)


class TestShannonEntropy:
    def test_uniform_distribution(self):
        # K bins with equal counts → H = log2(K)
        counts = np.ones(8, dtype=np.int64) * 100
        h = shannon_entropy(counts)
        assert h == pytest.approx(math.log2(8), abs=1e-10)

    def test_single_bin(self):
        # All mass in one bin → H = 0
        counts = np.array([100, 0, 0, 0])
        h = shannon_entropy(counts)
        assert h == pytest.approx(0.0, abs=1e-10)

    def test_known_distribution(self):
        # p = (1/2, 1/4, 1/4) → H = 1.5 bits
        counts = np.array([200, 100, 100])
        h = shannon_entropy(counts)
        assert h == pytest.approx(1.5, abs=1e-10)

    def test_empty_counts(self):
        counts = np.zeros(5, dtype=np.int64)
        h = shannon_entropy(counts)
        assert h == 0.0

    def test_from_histogram(self):
        hist = StreamingHistogram(0.0, 10.0, 10)
        rng = np.random.default_rng(42)
        hist.update_batch(rng.uniform(0.0, 10.0, 1000))
        h = shannon_entropy_from_histogram(hist)
        max_h = math.log2(10)
        assert 0 < h <= max_h


class TestJointEntropy:
    def test_independent_uniform(self):
        # 4x4 uniform → H(X,Y) = log2(16) = 4.0
        counts = np.ones((4, 4), dtype=np.int64) * 100
        h = joint_entropy(counts)
        assert h == pytest.approx(4.0, abs=1e-10)

    def test_concentrated(self):
        # All mass in one cell → H = 0
        counts = np.zeros((4, 4), dtype=np.int64)
        counts[0, 0] = 100
        h = joint_entropy(counts)
        assert h == pytest.approx(0.0, abs=1e-10)


class TestConditionalEntropy:
    def test_consistency(self):
        # H(Y|X) = H(X,Y) - H(X) and H(X,Y) >= H(X) always
        rng = np.random.default_rng(42)
        counts = rng.integers(0, 100, size=(5, 5))
        h_cond = conditional_entropy(counts)
        h_xy = joint_entropy(counts)
        h_x = shannon_entropy(counts.sum(axis=1))
        assert h_cond == pytest.approx(h_xy - h_x, abs=1e-10)
        assert h_xy >= h_x - 1e-10  # H(X,Y) >= H(X)

    def test_deterministic_y(self):
        # If Y is fully determined by X, H(Y|X) = 0
        # Each row has all mass in one column
        counts = np.zeros((3, 3), dtype=np.int64)
        counts[0, 0] = 100
        counts[1, 1] = 100
        counts[2, 2] = 100
        h_cond = conditional_entropy(counts)
        assert h_cond == pytest.approx(0.0, abs=1e-10)
