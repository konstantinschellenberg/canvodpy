"""Tests for AccumulatorSet and ProfileRegistry."""

import numpy as np
import pytest

from canvod.ops.statistics.profile import AccumulatorSet, ProfileRegistry
from canvod.streamstats import CellSignalKey


class TestAccumulatorSet:
    def test_update_propagates(self):
        acc = AccumulatorSet()
        acc.welford  # ensure default factory works
        # No histogram by default
        assert acc.histogram is None

        acc.update(5.0)
        assert acc.welford.count == 1
        assert acc.gk.count == 1

    def test_batch_update(self):
        from canvod.streamstats import StreamingHistogram

        acc = AccumulatorSet(histogram=StreamingHistogram(0.0, 10.0, 10))
        data = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        acc.update_batch(data)

        assert acc.welford.count == 5
        assert acc.gk.count == 5
        assert acc.histogram.total == 5

    def test_merge(self):
        a = AccumulatorSet()
        b = AccumulatorSet()
        a.update(1.0)
        a.update(2.0)
        b.update(3.0)
        b.update(4.0)

        a.merge(b)
        assert a.welford.count == 4
        assert a.gk.count == 4


class TestProfileRegistry:
    def test_get_or_create(self):
        reg = ProfileRegistry()
        key = CellSignalKey(1, "G01_L1C", "SNR", "canopy")
        acc = reg.get_or_create(key)

        assert acc.welford.count == 0
        assert acc.histogram is not None  # SNR has default bins
        assert len(reg) == 1

        # Same key returns same object
        assert reg.get_or_create(key) is acc

    def test_update(self):
        reg = ProfileRegistry()
        key = CellSignalKey(1, "G01_L1C", "SNR", "canopy")
        reg.update(key, 25.0)
        reg.update(key, 30.0)

        assert reg[key].welford.count == 2
        assert reg[key].welford.mean == pytest.approx(27.5)

    def test_merge_registries(self):
        reg1 = ProfileRegistry()
        reg2 = ProfileRegistry()

        key_a = CellSignalKey(1, "G01_L1C", "SNR", "canopy")
        key_b = CellSignalKey(2, "G02_L1C", "SNR", "canopy")

        reg1.update(key_a, 10.0)
        reg2.update(key_a, 20.0)
        reg2.update(key_b, 30.0)

        reg1.merge(reg2)

        assert len(reg1) == 2
        assert reg1[key_a].welford.count == 2
        assert key_b in reg1

    def test_summary(self):
        reg = ProfileRegistry(gk_epsilon=0.02)
        key = CellSignalKey(1, "G01_L1C", "SNR", "canopy")
        reg.update(key, 25.0)

        s = reg.summary()
        assert s["n_keys"] == 1
        assert s["total_observations"] == 1
        assert s["variables"] == ["SNR"]
        assert s["n_cells"] == 1
        assert s["gk_epsilon"] == 0.02

    def test_summary_empty(self):
        reg = ProfileRegistry()
        s = reg.summary()
        assert s["n_keys"] == 0
