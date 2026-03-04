"""Tests for GoertzelAccumulator."""

import numpy as np

from canvod.streamstats.accumulators.goertzel import GoertzelAccumulator


class TestGoertzelAccumulator:
    """GoertzelAccumulator tests."""

    def test_empty(self):
        acc = GoertzelAccumulator()
        assert acc.count == 0
        assert np.isnan(acc.power(0))

    def test_known_sine_frequency_recovery(self):
        """A pure sine at a target frequency should produce high power there."""
        freq = 0.1  # Hz
        fs = 1.0  # sample rate
        N = 100  # window size
        t = np.arange(N)
        signal = np.sin(2 * np.pi * freq * t / fs)

        acc = GoertzelAccumulator(frequencies=(freq,), sample_rate=fs, window_size=N)
        acc.update_batch(signal)

        # After one full window, power should be computed
        assert acc.count == N
        p = acc.power(0)
        assert not np.isnan(p)
        assert p > 0

    def test_multi_frequency(self):
        """Monitor two frequencies; power should be higher at the matching one."""
        f_target = 0.05
        f_other = 0.2
        fs = 1.0
        N = 200
        t = np.arange(N)
        signal = np.sin(2 * np.pi * f_target * t / fs)

        acc = GoertzelAccumulator(
            frequencies=(f_target, f_other), sample_rate=fs, window_size=N
        )
        acc.update_batch(signal)

        assert acc.power(0) > acc.power(1)

    def test_batch_vs_sequential(self):
        """Batch and sequential update should give the same result."""
        freq = 0.1
        fs = 1.0
        N = 50
        t = np.arange(N)
        signal = np.sin(2 * np.pi * freq * t / fs)

        acc_batch = GoertzelAccumulator(
            frequencies=(freq,), sample_rate=fs, window_size=N
        )
        acc_batch.update_batch(signal)

        acc_seq = GoertzelAccumulator(
            frequencies=(freq,), sample_rate=fs, window_size=N
        )
        for s in signal:
            acc_seq.update(s)

        np.testing.assert_allclose(acc_batch.power(0), acc_seq.power(0), rtol=1e-10)

    def test_nan_handling(self):
        """NaN values are skipped."""
        acc = GoertzelAccumulator(frequencies=(0.1,), sample_rate=1.0, window_size=10)
        data = np.ones(10)
        data[3] = np.nan
        data[7] = np.nan
        acc.update_batch(data)
        # 8 valid samples, window is 10, so window not yet complete
        assert acc.count == 8
        assert np.isnan(acc.power(0))

    def test_nan_scalar_skipped(self):
        acc = GoertzelAccumulator(frequencies=(0.1,), sample_rate=1.0, window_size=5)
        acc.update(float("nan"))
        assert acc.count == 0

    def test_roundtrip_serialization(self):
        """to_array / from_array round-trip preserves state."""
        freq = 0.1
        fs = 1.0
        N = 20
        acc = GoertzelAccumulator(
            frequencies=(freq, 0.2), sample_rate=fs, window_size=N
        )
        signal = np.sin(2 * np.pi * freq * np.arange(N) / fs)
        acc.update_batch(signal)

        arr = acc.to_array()
        restored = GoertzelAccumulator.from_array(arr)

        assert restored.count == acc.count
        assert restored.window_size == acc.window_size
        np.testing.assert_allclose(restored.power(0), acc.power(0), rtol=1e-10)
        np.testing.assert_allclose(restored.power(1), acc.power(1), rtol=1e-10)
        assert len(restored.frequencies) == 2
        np.testing.assert_allclose(restored.frequencies[0], freq, rtol=1e-6)

    def test_merge_right_biased(self):
        """Merge takes the accumulator with more observations."""
        freq = 0.1
        N = 20
        acc1 = GoertzelAccumulator(frequencies=(freq,), window_size=N)
        acc2 = GoertzelAccumulator(frequencies=(freq,), window_size=N)

        signal = np.sin(2 * np.pi * freq * np.arange(N))
        acc2.update_batch(signal)

        acc1.merge(acc2)
        assert acc1.count == N

    def test_merge_empty_other(self):
        """Merging an empty accumulator is a no-op."""
        acc = GoertzelAccumulator(frequencies=(0.1,), window_size=10)
        signal = np.ones(10)
        acc.update_batch(signal)
        count_before = acc.count
        acc.merge(GoertzelAccumulator(frequencies=(0.1,), window_size=10))
        assert acc.count == count_before

    def test_amplitude(self):
        """Amplitude is sqrt(power)."""
        freq = 0.1
        N = 100
        acc = GoertzelAccumulator(frequencies=(freq,), window_size=N)
        signal = np.sin(2 * np.pi * freq * np.arange(N))
        acc.update_batch(signal)

        p = acc.power(0)
        a = acc.amplitude(0)
        assert not np.isnan(a)
        np.testing.assert_allclose(a, np.sqrt(p), rtol=1e-12)

    def test_amplitude_nan_before_window(self):
        acc = GoertzelAccumulator(frequencies=(0.1,), window_size=100)
        acc.update(1.0)
        assert np.isnan(acc.amplitude(0))
