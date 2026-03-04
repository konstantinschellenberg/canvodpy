"""Tests for S4Accumulator and scintillation helper functions."""

import math

import numpy as np
import pytest

from canvod.streamstats.accumulators.scintillation import (
    S4Accumulator,
    lognormal_params,
    nakagami_m_from_s4,
    sigma_phi,
)
from canvod.streamstats.accumulators.welford import WelfordAccumulator


class TestS4Accumulator:
    def test_empty(self):
        s = S4Accumulator()
        assert s.count == 0
        assert math.isnan(s.s4)
        assert math.isnan(s.nakagami_m)
        assert math.isnan(s.mean_intensity)
        assert s.scintillation_regime == "unknown"

    def test_constant_signal(self):
        """Constant SNR → zero variance → S4 = 0."""
        s = S4Accumulator()
        for _ in range(100):
            s.update(30.0)  # 30 dB constant
        assert s.count == 100
        assert s.s4 == pytest.approx(0.0, abs=1e-10)
        assert s.scintillation_regime == "weak"

    def test_known_fluctuating_signal(self):
        """Alternating SNR → non-zero S4."""
        s = S4Accumulator()
        # Alternate between 20 dB and 40 dB
        for _ in range(500):
            s.update(20.0)
            s.update(40.0)
        assert s.count == 1000
        assert s.s4 > 0.0
        # Intensity values: 10^2=100 and 10^4=10000
        # mean = 5050, var ≈ (100-5050)² + (10000-5050)² ... large S4
        assert s.scintillation_regime == "strong"

    def test_batch_equals_sequential(self):
        rng = np.random.default_rng(42)
        data = rng.uniform(20.0, 50.0, size=200)

        seq = S4Accumulator()
        for x in data:
            seq.update(float(x))

        batch = S4Accumulator()
        batch.update_batch(data)

        assert seq.count == batch.count
        assert seq.s4 == pytest.approx(batch.s4, rel=1e-10)
        assert seq.mean_intensity == pytest.approx(batch.mean_intensity, rel=1e-10)

    def test_nan_handling(self):
        s = S4Accumulator()
        s.update(float("nan"))
        s.update(30.0)
        s.update(float("nan"))
        s.update(30.0)
        assert s.count == 2
        assert s.n_nan == 2

    def test_roundtrip_serialization(self):
        rng = np.random.default_rng(123)
        s = S4Accumulator()
        s.update_batch(rng.uniform(10.0, 50.0, size=100))

        arr = s.to_array()
        assert arr.shape == (8,)

        restored = S4Accumulator.from_array(arr)
        assert restored.count == s.count
        assert restored.s4 == pytest.approx(s.s4)
        assert restored.mean_intensity == pytest.approx(s.mean_intensity)

    def test_scintillation_regime_moderate(self):
        """Construct an accumulator with S4 in moderate range (0.3-0.6)."""
        # We'll directly build from a Welford on intensity with known moments
        # mean²/var ratio gives S4² = var/mean²
        # For S4 = 0.45 → var/mean² = 0.2025
        # mean = 100, var = 2025 → M2 = var*(n-1) for n samples
        s = S4Accumulator()
        # Generate data with controlled coefficient of variation
        rng = np.random.default_rng(99)
        # CV ≈ 0.45 for lognormal with sigma_ln ≈ 0.43
        ln_data = rng.normal(loc=4.0, scale=0.43, size=10000)
        intensity = np.exp(ln_data)
        snr_db = 10.0 * np.log10(intensity)
        s.update_batch(snr_db)
        # Check regime is moderate (0.3-0.6)
        assert s.scintillation_regime == "moderate"

    def test_nakagami_m_property(self):
        s = S4Accumulator()
        rng = np.random.default_rng(7)
        s.update_batch(rng.uniform(20.0, 40.0, size=500))
        s4_val = s.s4
        assert s.nakagami_m == pytest.approx(1.0 / (s4_val * s4_val))

    def test_merge(self):
        rng = np.random.default_rng(55)
        data = rng.uniform(15.0, 45.0, size=200)

        full = S4Accumulator()
        full.update_batch(data)

        a = S4Accumulator()
        a.update_batch(data[:100])
        b = S4Accumulator()
        b.update_batch(data[100:])
        a.merge(b)

        assert a.count == full.count
        assert a.s4 == pytest.approx(full.s4, rel=1e-10)

    def test_single_value(self):
        s = S4Accumulator()
        s.update(25.0)
        assert s.count == 1
        assert math.isnan(s.s4)  # n < 2


class TestSigmaPhi:
    def test_basic(self):
        assert sigma_phi(4.0) == pytest.approx(2.0)

    def test_nan_input(self):
        assert math.isnan(sigma_phi(float("nan")))

    def test_negative_variance(self):
        assert math.isnan(sigma_phi(-1.0))

    def test_zero(self):
        assert sigma_phi(0.0) == pytest.approx(0.0)


class TestNakagamiMFromS4:
    def test_basic(self):
        # S4 = 0.5 → m = 1/0.25 = 4.0
        assert nakagami_m_from_s4(0.5) == pytest.approx(4.0)

    def test_s4_one(self):
        # S4 = 1.0 → m = 1.0 (Rayleigh)
        assert nakagami_m_from_s4(1.0) == pytest.approx(1.0)

    def test_nan(self):
        assert math.isnan(nakagami_m_from_s4(float("nan")))

    def test_zero(self):
        assert math.isnan(nakagami_m_from_s4(0.0))

    def test_negative(self):
        assert math.isnan(nakagami_m_from_s4(-0.5))


class TestLognormalParams:
    def test_basic(self):
        w = WelfordAccumulator()
        for x in [1.0, 2.0, 3.0, 4.0, 5.0]:
            w.update(x)
        mu, sigma = lognormal_params(w)
        assert mu == pytest.approx(w.mean)
        assert sigma == pytest.approx(w.std)
