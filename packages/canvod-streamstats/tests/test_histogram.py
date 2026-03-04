"""Tests for StreamingHistogram."""

import numpy as np
import pytest

from canvod.streamstats.accumulators.histogram import StreamingHistogram


class TestStreamingHistogram:
    def test_empty(self):
        h = StreamingHistogram(0.0, 10.0, 10)
        assert h.total == 0
        assert h.underflow == 0
        assert h.overflow == 0
        np.testing.assert_array_equal(h.counts, np.zeros(10, dtype=np.int64))

    def test_single_bin(self):
        h = StreamingHistogram(0.0, 10.0, 10)
        h.update(0.5)
        assert h.total == 1
        assert h.counts[0] == 1
        assert h.underflow == 0
        assert h.overflow == 0

    def test_underflow_overflow(self):
        h = StreamingHistogram(0.0, 10.0, 10)
        h.update(-1.0)  # underflow
        h.update(10.0)  # overflow (>= high)
        h.update(15.0)  # overflow
        assert h.underflow == 1
        assert h.overflow == 2
        assert h.total == 3

    def test_batch(self):
        h = StreamingHistogram(0.0, 10.0, 10)
        data = np.array([0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5])
        h.update_batch(data)
        assert h.total == 10
        # Each value falls in a different bin
        np.testing.assert_array_equal(h.counts, np.ones(10, dtype=np.int64))

    def test_batch_with_nan(self):
        h = StreamingHistogram(0.0, 10.0, 10)
        data = np.array([0.5, float("nan"), 5.5, float("nan")])
        h.update_batch(data)
        assert h.total == 2

    def test_merge(self):
        h1 = StreamingHistogram(0.0, 10.0, 5)
        h2 = StreamingHistogram(0.0, 10.0, 5)
        h1.update_batch(np.array([1.0, 3.0, 5.0]))
        h2.update_batch(np.array([2.0, 4.0, -1.0]))

        h1.merge(h2)
        assert h1.total == 6
        assert h1.underflow == 1  # -1.0 from h2

    def test_merge_incompatible(self):
        h1 = StreamingHistogram(0.0, 10.0, 5)
        h2 = StreamingHistogram(0.0, 20.0, 5)
        with pytest.raises(ValueError, match="different bin specs"):
            h1.merge(h2)

    def test_factory(self):
        h = StreamingHistogram.for_variable("SNR")
        assert h.low == 0.0
        assert h.high == 60.0
        assert h.n_bins == 120

    def test_factory_unknown(self):
        with pytest.raises(KeyError, match="Unknown variable"):
            StreamingHistogram.for_variable("nonexistent")

    def test_roundtrip(self):
        h = StreamingHistogram(0.0, 10.0, 5)
        h.update_batch(np.array([-1.0, 1.0, 3.0, 5.0, 7.0, 9.0, 11.0]))

        arr = h.to_array()
        h2 = StreamingHistogram.from_array(arr, 0.0, 10.0, 5)

        assert h2.underflow == h.underflow
        assert h2.overflow == h.overflow
        np.testing.assert_array_equal(h2.counts, h.counts)

    def test_bin_edges(self):
        h = StreamingHistogram(0.0, 10.0, 5)
        edges = h.bin_edges
        np.testing.assert_allclose(edges, [0.0, 2.0, 4.0, 6.0, 8.0, 10.0])
