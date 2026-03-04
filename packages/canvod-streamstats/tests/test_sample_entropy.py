"""Tests for sample entropy."""

import numpy as np
import pytest

from canvod.streamstats.information.sample_entropy import sample_entropy


class TestSampleEntropy:
    def test_constant_signal(self):
        # Constant → very regular (SampEn = 0 or near zero)
        result = sample_entropy(np.ones(200), m=2)
        # All templates match → -log(A/B) should be 0 when A==B
        assert result.sample_entropy == pytest.approx(0.0, abs=0.02)
        assert result.complexity == "regular"

    def test_random_noise_higher(self):
        rng = np.random.default_rng(42)
        result = sample_entropy(rng.standard_normal(500), m=2)
        assert result.sample_entropy > 0.5

    def test_periodic_signal(self):
        t = np.linspace(0, 10 * np.pi, 500)
        result = sample_entropy(np.sin(t), m=2)
        # Periodic signal → low entropy (regular)
        assert result.sample_entropy < 1.0

    def test_short_input(self):
        result = sample_entropy(np.array([1.0, 2.0]), m=2)
        assert np.isnan(result.sample_entropy)
        assert result.complexity == "insufficient_data"

    def test_tolerance_effect(self):
        rng = np.random.default_rng(42)
        data = rng.standard_normal(300)
        r_small = sample_entropy(data, m=2, r=0.05)
        r_large = sample_entropy(data, m=2, r=1.0)
        # Larger tolerance → more matches → lower SampEn
        assert r_large.sample_entropy < r_small.sample_entropy
