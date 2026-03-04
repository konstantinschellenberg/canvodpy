"""Tests for ClimatologyGrid."""

from __future__ import annotations

import math

import numpy as np
import pytest

from canvod.streamstats.climatology.grid import ClimatologyGrid


class TestClimatologyGridEmpty:
    """Empty grid returns NaN mean/std and zero count."""

    def test_empty_grid_nan_mean_std(self) -> None:
        grid = ClimatologyGrid()
        mean, std, count = grid.climatology_at(doy=100, hour=12.0)
        assert math.isnan(mean)
        assert math.isnan(std)
        assert count == 0


class TestClimatologyGridSingleObs:
    """Single observation routes to the correct bin."""

    def test_single_observation(self) -> None:
        grid = ClimatologyGrid(doy_window=15, tod_window=1)
        grid.update(doy=50, hour=10.5, value=3.0)

        mean, std, count = grid.climatology_at(doy=50, hour=10.5)
        assert mean == pytest.approx(3.0)
        assert count == 1
        # std is NaN for a single observation (variance = M2/0)
        assert std == pytest.approx(0.0) or math.isnan(std)

    def test_different_bin_empty(self) -> None:
        grid = ClimatologyGrid(doy_window=15, tod_window=1)
        grid.update(doy=50, hour=10.5, value=3.0)

        # Different TOD bin should be empty
        _, _, count = grid.climatology_at(doy=50, hour=5.0)
        assert count == 0


class TestClimatologyGridDOYWrap:
    """DOY circular wrapping: DOY 1 and 366 should map based on modulo."""

    def test_doy_1_and_366_adjacent(self) -> None:
        grid = ClimatologyGrid(doy_window=15, tod_window=24)
        # DOY 1 → bin 0, DOY 366 → bin (365 % 366) // 15 = 24
        grid.update(doy=1, hour=0.0, value=1.0)
        grid.update(doy=366, hour=0.0, value=2.0)

        _, _, c1 = grid.climatology_at(doy=1, hour=0.0)
        _, _, c366 = grid.climatology_at(doy=366, hour=0.0)

        # DOY 1 → bin 0 ((1-1)%366 // 15 = 0)
        # DOY 366 → bin 24 ((366-1)%366 // 15 = 365//15 = 24)
        # They should be in different bins
        assert c1 == 1
        assert c366 == 1

    def test_same_bin_wrap(self) -> None:
        # With window=366, everything maps to bin 0
        grid = ClimatologyGrid(doy_window=366, tod_window=24)
        grid.update(doy=1, hour=0.0, value=1.0)
        grid.update(doy=366, hour=0.0, value=2.0)
        _, _, count = grid.climatology_at(doy=1, hour=0.0)
        assert count == 2


class TestClimatologyGridTODBoundary:
    """TOD boundary mapping."""

    def test_hour_0_maps_to_bin_0(self) -> None:
        grid = ClimatologyGrid(doy_window=366, tod_window=1)
        grid.update(doy=1, hour=0.0, value=1.0)
        assert grid.count(0, 0) == 1

    def test_hour_23_9_maps_to_bin_23(self) -> None:
        grid = ClimatologyGrid(doy_window=366, tod_window=1)
        grid.update(doy=1, hour=23.9, value=1.0)
        assert grid.count(0, 23) == 1


class TestClimatologyGridBatch:
    """Batch update matches sequential."""

    def test_batch_matches_sequential(self) -> None:
        doys = np.array([10, 10, 200, 200])
        hours = np.array([5.0, 5.0, 18.0, 18.0])
        values = np.array([1.0, 3.0, 10.0, 20.0])

        # Sequential
        g_seq = ClimatologyGrid(doy_window=30, tod_window=6)
        for d, h, v in zip(doys, hours, values):
            g_seq.update(int(d), float(h), float(v))

        # Batch
        g_batch = ClimatologyGrid(doy_window=30, tod_window=6)
        g_batch.update_batch(doys, hours, values)

        for d, h in [(10, 5.0), (200, 18.0)]:
            m_s, s_s, c_s = g_seq.climatology_at(d, h)
            m_b, s_b, c_b = g_batch.climatology_at(d, h)
            assert c_s == c_b
            assert m_s == pytest.approx(m_b)
            assert s_s == pytest.approx(s_b)


class TestClimatologyGridKnownStats:
    """Known statistics in one bin."""

    def test_known_mean_std(self) -> None:
        grid = ClimatologyGrid(doy_window=366, tod_window=24)
        # All observations go to the same bin
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]:
            grid.update(doy=1, hour=0.0, value=v)

        mean, std, count = grid.climatology_at(doy=1, hour=0.0)
        assert count == 8
        assert mean == pytest.approx(5.0)
        # Sample std (N-1 denominator): sqrt(32/7) ≈ 2.138
        assert std == pytest.approx(2.138089935299395, rel=1e-6)


class TestClimatologyGridSerialization:
    """Serialisation roundtrip."""

    def test_roundtrip(self) -> None:
        grid = ClimatologyGrid(doy_window=30, tod_window=6)
        grid.update(doy=50, hour=10.0, value=3.0)
        grid.update(doy=50, hour=10.0, value=7.0)
        grid.update(doy=200, hour=20.0, value=100.0)

        arr = grid.to_array()
        restored = ClimatologyGrid.from_array(arr)

        assert restored.doy_window == grid.doy_window
        assert restored.tod_window == grid.tod_window
        assert restored.shape == grid.shape

        m1, s1, c1 = grid.climatology_at(50, 10.0)
        m2, s2, c2 = restored.climatology_at(50, 10.0)
        assert c1 == c2
        assert m1 == pytest.approx(m2)
        assert s1 == pytest.approx(s2)


class TestClimatologyGridMerge:
    """Merge two grids with matching dimensions."""

    def test_merge(self) -> None:
        g1 = ClimatologyGrid(doy_window=366, tod_window=24)
        g2 = ClimatologyGrid(doy_window=366, tod_window=24)

        g1.update(doy=1, hour=0.0, value=2.0)
        g1.update(doy=1, hour=0.0, value=4.0)
        g2.update(doy=1, hour=0.0, value=6.0)
        g2.update(doy=1, hour=0.0, value=8.0)

        g1.merge(g2)
        mean, std, count = g1.climatology_at(doy=1, hour=0.0)
        assert count == 4
        assert mean == pytest.approx(5.0)

    def test_merge_dimension_mismatch_raises(self) -> None:
        g1 = ClimatologyGrid(doy_window=15, tod_window=1)
        g2 = ClimatologyGrid(doy_window=30, tod_window=1)
        with pytest.raises(ValueError, match="Cannot merge"):
            g1.merge(g2)


class TestClimatologyGridProperties:
    """Shape and property accessors."""

    def test_shape(self) -> None:
        grid = ClimatologyGrid(doy_window=15, tod_window=1)
        assert grid.n_doy_bins == 25  # ceil(366/15) = 25
        assert grid.n_tod_bins == 24
        assert grid.shape == (25, 24)

    def test_direct_bin_access(self) -> None:
        grid = ClimatologyGrid(doy_window=366, tod_window=24)
        grid.update(doy=1, hour=0.0, value=5.0)
        assert grid.mean(0, 0) == pytest.approx(5.0)
        assert grid.count(0, 0) == 1
