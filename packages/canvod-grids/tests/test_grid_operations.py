"""Tests for grid conversion and extraction operations.

Tests cover:
- extract_grid_vertices() - 3D vertex extraction
- grid_to_dataset() - xarray Dataset conversion
- Metadata preservation in conversion
- Vertex coordinate validity
- Geometric consistency
"""

from __future__ import annotations

import numpy as np
import pytest

from canvod.grids import create_hemigrid, extract_grid_vertices, grid_to_dataset


@pytest.fixture
def equal_area_grid():
    """Create equal-area grid for testing."""
    return create_hemigrid("equal_area", angular_resolution=15.0)


@pytest.fixture
def htm_grid():
    """Create HTM grid for testing."""
    return create_hemigrid("htm", angular_resolution=15.0)


class TestExtractGridVertices:
    """Test vertex extraction for 3D visualization."""

    def test_extract_vertices_equal_area(self, equal_area_grid) -> None:
        """Extract vertices from equal-area grid."""
        x, y, z = extract_grid_vertices(equal_area_grid)

        # Should return three arrays
        assert isinstance(x, np.ndarray)
        assert isinstance(y, np.ndarray)
        assert isinstance(z, np.ndarray)

        # All arrays same length
        assert len(x) == len(y) == len(z)

        # Should have vertices (4 per cell for rectangular)
        assert len(x) > 0

    def test_extract_vertices_htm(self, htm_grid) -> None:
        """Extract vertices from HTM grid."""
        x, y, z = extract_grid_vertices(htm_grid)

        # Should have vertices (3 per cell for triangular)
        assert len(x) > 0
        assert len(x) == len(y) == len(z)

    def test_vertices_on_unit_sphere(self, equal_area_grid) -> None:
        """Verify vertices lie on unit sphere."""
        x, y, z = extract_grid_vertices(equal_area_grid)

        # Sample vertices and check radius
        for i in range(0, len(x), 10):
            r = np.sqrt(x[i] ** 2 + y[i] ** 2 + z[i] ** 2)
            assert np.isclose(r, 1.0, rtol=1e-5)


class TestGridToDataset:
    """Test conversion to xarray Dataset."""

    def test_grid_to_dataset_equal_area(self, equal_area_grid) -> None:
        """Convert equal-area grid to xarray Dataset."""
        ds = grid_to_dataset(equal_area_grid)

        # Check Dataset structure
        assert "cell_phi" in ds.data_vars
        assert "cell_theta" in ds.data_vars
        assert "vertices_phi" in ds.data_vars
        assert "vertices_theta" in ds.data_vars
        assert "n_vertices" in ds.data_vars

        # Check dimensions
        assert len(ds["cell_phi"]) == equal_area_grid.ncells

    def test_grid_to_dataset_htm(self, htm_grid) -> None:
        """Convert HTM grid to xarray Dataset."""
        ds = grid_to_dataset(htm_grid)

        # Should have required variables
        assert "cell_phi" in ds.data_vars
        assert "cell_theta" in ds.data_vars
        assert "vertices_phi" in ds.data_vars
        assert "vertices_theta" in ds.data_vars

        # Check cell count
        assert len(ds["cell_phi"]) == htm_grid.ncells

    def test_grid_to_dataset_has_solid_angles(self, equal_area_grid) -> None:
        """Verify Dataset includes solid angles."""
        ds = grid_to_dataset(equal_area_grid)

        # Should have solid_angle variable
        assert "solid_angle" in ds.data_vars

        # Should have same length as cells
        assert len(ds["solid_angle"]) == equal_area_grid.ncells

        # All solid angles should be positive
        assert np.all(ds["solid_angle"].values > 0)


class TestDatasetMetadata:
    """Test metadata preservation in Dataset conversion."""

    def test_dataset_has_grid_type(self, equal_area_grid) -> None:
        """Verify grid_type stored in Dataset attributes."""
        ds = grid_to_dataset(equal_area_grid)

        # Should have grid_type attribute
        assert "grid_type" in ds.attrs
        assert ds.attrs["grid_type"] == "equal_area"

    def test_dataset_has_resolution(self, equal_area_grid) -> None:
        """Verify angular_resolution_deg stored in attributes."""
        ds = grid_to_dataset(equal_area_grid)

        # Should have angular_resolution_deg
        assert "angular_resolution_deg" in ds.attrs
        # Should be numeric
        assert isinstance(ds.attrs["angular_resolution_deg"], (int, float))

    def test_dataset_preserves_ncells(self, equal_area_grid) -> None:
        """Verify cell count preserved in conversion."""
        ds = grid_to_dataset(equal_area_grid)

        # Should have n_cells attribute (not ncells)
        assert "n_cells" in ds.attrs
        assert ds.attrs["n_cells"] == equal_area_grid.ncells


class TestVertexCoordinates:
    """Test validity of vertex coordinates."""

    def test_vertex_coords_valid_ranges(self, equal_area_grid) -> None:
        """Verify vertex coordinates in valid ranges."""
        ds = grid_to_dataset(equal_area_grid)

        vertices_phi = ds["vertices_phi"].values
        vertices_theta = ds["vertices_theta"].values

        # Phi in [0, 2π]
        assert np.all(vertices_phi >= 0.0)
        assert np.all(vertices_phi <= 2 * np.pi + 1e-10)  # Allow tiny overshoot

        # Theta in [0, π/2] for hemisphere
        assert np.all(vertices_theta >= 0.0)
        assert np.all(vertices_theta <= np.pi / 2 + 1e-10)

    def test_n_vertices_correct(self, equal_area_grid) -> None:
        """Verify n_vertices matches actual vertex count."""
        ds = grid_to_dataset(equal_area_grid)

        n_vertices = ds["n_vertices"].values
        vertices_phi = ds["vertices_phi"].values

        # Each cell should have consistent vertex count
        for i, n in enumerate(n_vertices):
            # Equal-area cells should have 4 vertices
            assert n == 4

            # Check that we have actual coordinates
            cell_vertices_phi = vertices_phi[i, :n]
            assert np.all(np.isfinite(cell_vertices_phi))


class TestGeometricConsistency:
    """Test geometric consistency of grid representations."""

    def test_cell_centers_match_grid(self, equal_area_grid) -> None:
        """Verify Dataset cell centers match grid DataFrame."""
        ds = grid_to_dataset(equal_area_grid)

        ds_phi = ds["cell_phi"].values
        ds_theta = ds["cell_theta"].values

        grid_phi = equal_area_grid.grid["phi"].to_numpy()
        grid_theta = equal_area_grid.grid["theta"].to_numpy()

        # Should match exactly
        np.testing.assert_allclose(ds_phi, grid_phi, rtol=1e-10)
        np.testing.assert_allclose(ds_theta, grid_theta, rtol=1e-10)

    def test_vertices_form_closed_cells(self, equal_area_grid) -> None:
        """Verify vertices form geometrically valid cells."""
        ds = grid_to_dataset(equal_area_grid)

        vertices_phi = ds["vertices_phi"].values
        vertices_theta = ds["vertices_theta"].values

        # Sample first cell
        cell_v_phi = vertices_phi[0, :]
        cell_v_theta = vertices_theta[0, :]

        # Should have 4 vertices for rectangular cell
        assert len(cell_v_phi) >= 4

        # Vertices should form rectangle (opposite sides equal)
        # This is a basic check - exact geometry depends on projection
        assert np.all(np.isfinite(cell_v_phi[:4]))
        assert np.all(np.isfinite(cell_v_theta[:4]))


class TestDifferentGridTypes:
    """Test operations work with different grid types."""

    def test_equirectangular_grid_to_dataset(self) -> None:
        """Test conversion of equirectangular grid."""
        grid = create_hemigrid("equirectangular", angular_resolution=20.0)
        ds = grid_to_dataset(grid)

        assert "cell_phi" in ds.data_vars
        assert ds.attrs["grid_type"] == "equirectangular"
        assert len(ds["cell_phi"]) == grid.ncells

    def test_equal_angle_grid_to_dataset(self) -> None:
        """Test conversion of equal-angle grid."""
        grid = create_hemigrid("equal_angle", angular_resolution=20.0)
        ds = grid_to_dataset(grid)

        assert "cell_phi" in ds.data_vars
        assert ds.attrs["grid_type"] == "equal_angle"
        assert len(ds["cell_phi"]) == grid.ncells
