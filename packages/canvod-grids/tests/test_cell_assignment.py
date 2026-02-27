"""Tests for cell assignment operations.

Tests the core functionality of assigning VOD observations to grid cells
using KDTree-based nearest-neighbor lookups.

Tests cover:
- add_cell_ids_to_vod_fast() - KDTree-based assignment
- add_cell_ids_to_ds_fast() - Dask variant
- KDTree construction and functionality
- Edge case coordinates (zenith, horizon, wrapping)
"""

from __future__ import annotations

import numpy as np
import pytest
import xarray as xr
from canvod.grids import create_hemigrid
from canvod.grids.operations import (
    _build_kdtree,
    add_cell_ids_to_ds_fast,
    add_cell_ids_to_vod_fast,
)


@pytest.fixture
def sample_grid():
    """Create sample grid for testing."""
    return create_hemigrid("equal_area", angular_resolution=10.0)


@pytest.fixture
def sample_vod_dataset():
    """Create sample VOD xarray Dataset."""
    np.random.seed(42)
    n_epochs = 10
    n_sids = 10

    # Random points on hemisphere
    phi = np.random.uniform(0, 2 * np.pi, (n_epochs, n_sids))
    theta = np.random.uniform(0, np.pi / 2, (n_epochs, n_sids))
    vod = np.random.uniform(0, 1, (n_epochs, n_sids))

    return xr.Dataset(
        {
            "VOD": (["epoch", "sid"], vod),
        },
        coords={
            "phi": (["epoch", "sid"], phi),
            "theta": (["epoch", "sid"], theta),
            "epoch": np.arange(n_epochs),
            "sid": np.arange(n_sids),
        },
    )


class TestCellAssignmentBasic:
    """Test basic cell assignment functionality."""

    def test_add_cell_ids_to_vod_fast(self, sample_grid, sample_vod_dataset) -> None:
        """Test KDTree-based cell assignment."""
        result = add_cell_ids_to_vod_fast(sample_vod_dataset, sample_grid, "test_grid")

        # Should return Dataset with cell_id variable
        assert isinstance(result, xr.Dataset)
        assert "cell_id_test_grid" in result.variables

        # All observations should have assigned IDs
        cell_ids = result["cell_id_test_grid"].values
        assert cell_ids.shape == sample_vod_dataset["VOD"].shape

        # Valid cell IDs should be within grid range
        valid_ids = cell_ids[np.isfinite(cell_ids)]
        assert np.all(valid_ids >= 0)
        assert np.all(valid_ids < sample_grid.ncells)

    def test_cell_assignment_preserves_original_data(
        self, sample_grid, sample_vod_dataset
    ) -> None:
        """Verify original data is preserved."""
        result = add_cell_ids_to_vod_fast(sample_vod_dataset, sample_grid, "test_grid")

        # Original variables should be present
        assert "VOD" in result.variables
        assert "phi" in result.coords
        assert "theta" in result.coords

        # Values should match original
        np.testing.assert_array_equal(
            result["VOD"].values, sample_vod_dataset["VOD"].values
        )


class TestKDTreeBuild:
    """Test KDTree construction."""

    def test_kdtree_build(self, sample_grid) -> None:
        """Test KDTree construction from grid."""
        tree = _build_kdtree(sample_grid)

        # Tree should have correct number of points
        assert tree.n == sample_grid.ncells

    def test_kdtree_query(self, sample_grid) -> None:
        """Test KDTree query functionality."""
        tree = _build_kdtree(sample_grid)

        # Query point at zenith
        query_point = np.array([[0.0, 0.0, 1.0]])  # North pole
        dist, idx = tree.query(query_point)

        assert len(dist) == 1
        assert len(idx) == 1
        assert 0 <= idx[0] < sample_grid.ncells


class TestAddCellIdsToDsFast:
    """Test Dask variant for xarray Datasets."""

    def test_add_cell_ids_to_ds_fast(self, sample_grid) -> None:
        """Test Dask variant with (epoch, sid) dask-backed dataset."""
        import dask.array as da

        np.random.seed(42)
        n_epochs = 5
        n_sids = 10

        phi = np.random.uniform(0, 2 * np.pi, (n_epochs, n_sids))
        theta = np.random.uniform(0, np.pi / 2, (n_epochs, n_sids))
        vod = np.random.uniform(0, 1, (n_epochs, n_sids))

        ds = xr.Dataset(
            {
                "vod": (["epoch", "sid"], da.from_array(vod, chunks=(5, 10))),
            },
            coords={
                "phi": (["epoch", "sid"], da.from_array(phi, chunks=(5, 10))),
                "theta": (["epoch", "sid"], da.from_array(theta, chunks=(5, 10))),
                "epoch": np.arange(n_epochs),
                "sid": np.arange(n_sids),
            },
        )

        result = add_cell_ids_to_ds_fast(ds, sample_grid, "test_grid", data_var="vod")

        # Should have cell_id variable
        assert "cell_id_test_grid" in result.variables

        # Should preserve original data
        assert "vod" in result.variables
        assert "phi" in result.coords
        assert "theta" in result.coords

        # Cell IDs should be computable and valid
        cell_ids = result["cell_id_test_grid"].values
        assert cell_ids.shape == (n_epochs, n_sids)
        valid = cell_ids[np.isfinite(cell_ids)]
        assert np.all(valid >= 0)
        assert np.all(valid < sample_grid.ncells)


class TestEdgeCaseCoordinates:
    """Test edge cases and boundary coordinates."""

    def test_edge_case_zenith(self, sample_grid) -> None:
        """Test point at zenith (θ=0)."""
        zenith_ds = xr.Dataset(
            {"VOD": (["epoch", "sid"], [[0.5]])},
            coords={
                "phi": (["epoch", "sid"], [[0.0]]),
                "theta": (["epoch", "sid"], [[0.0]]),
                "epoch": [0],
                "sid": [0],
            },
        )

        result = add_cell_ids_to_vod_fast(zenith_ds, sample_grid, "test")

        # Should be assigned to valid cell
        cell_id = result["cell_id_test"].values[0, 0]
        assert np.isfinite(cell_id)
        assert 0 <= cell_id < sample_grid.ncells

    def test_edge_case_horizon(self, sample_grid) -> None:
        """Test points near horizon (θ≈π/2)."""
        horizon_ds = xr.Dataset(
            {"VOD": (["epoch", "sid"], [[0.5, 0.6, 0.7, 0.8]])},
            coords={
                "phi": (["epoch", "sid"], [[0.0, np.pi / 2, np.pi, 3 * np.pi / 2]]),
                "theta": (
                    ["epoch", "sid"],
                    [[np.pi / 2, np.pi / 2, np.pi / 2, np.pi / 2]],
                ),
                "epoch": [0],
                "sid": [0, 1, 2, 3],
            },
        )

        result = add_cell_ids_to_vod_fast(horizon_ds, sample_grid, "test")

        # All should be assigned
        cell_ids = result["cell_id_test"].values[0, :]
        assert len(cell_ids) == 4

        # All IDs should be valid
        assert np.all(np.isfinite(cell_ids))
        assert np.all(cell_ids >= 0)
        assert np.all(cell_ids < sample_grid.ncells)

    def test_edge_case_phi_wrapping(self, sample_grid) -> None:
        """Test points near phi=0/2π boundary."""
        wrap_ds = xr.Dataset(
            {"VOD": (["epoch", "sid"], [[0.5, 0.6, 0.7]])},
            coords={
                "phi": (["epoch", "sid"], [[0.0, 0.01, 2 * np.pi - 0.01]]),
                "theta": (["epoch", "sid"], [[np.pi / 4, np.pi / 4, np.pi / 4]]),
                "epoch": [0],
                "sid": [0, 1, 2],
            },
        )

        result = add_cell_ids_to_vod_fast(wrap_ds, sample_grid, "test")

        # Should handle wrapping correctly
        cell_ids = result["cell_id_test"].values[0, :]
        assert len(cell_ids) == 3

        # All should be valid
        assert np.all(np.isfinite(cell_ids))
        assert np.all(cell_ids >= 0)
        assert np.all(cell_ids < sample_grid.ncells)

    def test_nan_coordinates(self, sample_grid) -> None:
        """Test handling of NaN coordinates."""
        nan_ds = xr.Dataset(
            {"VOD": (["epoch", "sid"], [[0.5, 0.6, 0.7]])},
            coords={
                "phi": (["epoch", "sid"], [[0.5, np.nan, 1.0]]),
                "theta": (["epoch", "sid"], [[0.5, 0.6, np.nan]]),
                "epoch": [0],
                "sid": [0, 1, 2],
            },
        )

        result = add_cell_ids_to_vod_fast(nan_ds, sample_grid, "test")

        cell_ids = result["cell_id_test"].values[0, :]

        # First should be assigned, others should be NaN
        assert np.isfinite(cell_ids[0])
        assert np.isnan(cell_ids[1])
        assert np.isnan(cell_ids[2])
