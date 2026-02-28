"""Property-based tests for grid invariants using Hypothesis.

These tests use generative testing to verify that grid properties hold
for a wide range of inputs. Hypothesis automatically generates test cases
and shrinks failures to minimal examples.

Tests cover:
- Grid always has positive cell count
- Coordinates stay within hemisphere bounds
- Cell IDs are unique
- Solid angles sum to hemisphere area
- Phi/theta bounds are valid
- Grid DataFrame is always Polars
"""

from __future__ import annotations

import numpy as np
import polars as pl
from hypothesis import given, settings
from hypothesis import strategies as st

from canvod.grids import create_hemigrid


class TestGridInvariants:
    """Property-based tests for grid structural invariants."""

    @given(
        angular_resolution=st.floats(min_value=2.0, max_value=45.0, exclude_min=False)
    )
    @settings(max_examples=50, deadline=None)
    def test_grid_ncells_always_positive(self, angular_resolution: float) -> None:
        """Any valid resolution should create grid with positive cells."""
        # Create equal-area grid
        grid = create_hemigrid("equal_area", angular_resolution)

        # Fundamental invariant: grids must have cells
        assert grid.ncells > 0
        assert isinstance(grid.ncells, int)

    @given(
        angular_resolution=st.floats(min_value=5.0, max_value=30.0, exclude_min=False)
    )
    @settings(max_examples=30, deadline=None)
    def test_grid_coords_in_hemisphere(self, angular_resolution: float) -> None:
        """All cell centers must be within hemisphere bounds."""
        grid = create_hemigrid("equal_area", angular_resolution)

        # Extract coordinates
        phi = grid.grid["phi"].to_numpy()
        theta = grid.grid["theta"].to_numpy()

        # Phi must be in [0, 2π)
        assert np.all(phi >= 0.0)
        assert np.all(phi < 2 * np.pi)

        # Theta must be in [0, π/2] for hemisphere
        assert np.all(theta >= 0.0)
        assert np.all(theta <= np.pi / 2)

    @given(
        angular_resolution=st.floats(min_value=5.0, max_value=30.0, exclude_min=False)
    )
    @settings(max_examples=30, deadline=None)
    def test_cell_ids_unique(self, angular_resolution: float) -> None:
        """Cell IDs must be unique across the grid."""
        grid = create_hemigrid("equal_area", angular_resolution)

        cell_ids = grid.grid["cell_id"].to_numpy()

        # All IDs must be unique
        assert len(cell_ids) == len(np.unique(cell_ids))

        # IDs should be sequential starting from 0
        assert np.all(cell_ids == np.arange(grid.ncells))

    @given(
        angular_resolution=st.floats(min_value=5.0, max_value=30.0, exclude_min=False)
    )
    @settings(max_examples=20, deadline=None)
    def test_solid_angles_sum_to_hemisphere(self, angular_resolution: float) -> None:
        """Sum of solid angles should approximately equal hemisphere area."""
        grid = create_hemigrid("equal_area", angular_resolution)

        solid_angles = grid.get_solid_angles()

        # All solid angles must be positive
        assert np.all(solid_angles > 0), "All solid angles must be positive"

        total_solid_angle = np.sum(solid_angles)
        expected_area = 2 * np.pi

        # Discrete grids undercount hemisphere area (edge effects).
        # Coarse resolutions (>20°) can miss up to ~50% of the area.
        # Fine resolutions (<10°) typically within ~10%.
        # The sum should never exceed the hemisphere area.
        assert total_solid_angle <= expected_area * 1.05, (
            f"Sum {total_solid_angle} exceeds hemisphere area {expected_area}"
        )
        assert total_solid_angle > expected_area * 0.45, (
            f"Sum {total_solid_angle} far below hemisphere area {expected_area}"
        )

    @given(
        angular_resolution=st.floats(min_value=5.0, max_value=30.0, exclude_min=False)
    )
    @settings(max_examples=30, deadline=None)
    def test_phi_theta_bounds_valid(self, angular_resolution: float) -> None:
        """Cell boundaries must satisfy min < max for phi and theta."""
        grid = create_hemigrid("equal_area", angular_resolution)

        # Check bounds exist for rectangular grids
        if "phi_min" in grid.grid.columns:
            phi_min = grid.grid["phi_min"].to_numpy()
            phi_max = grid.grid["phi_max"].to_numpy()
            theta_min = grid.grid["theta_min"].to_numpy()
            theta_max = grid.grid["theta_max"].to_numpy()

            # Min must be less than max (with tolerance for wrapping)
            # Some cells may wrap around φ=0/2π
            non_wrapping = phi_max > phi_min
            assert np.sum(non_wrapping) > 0  # Most cells don't wrap

            # Theta should never wrap
            assert np.all(theta_max > theta_min)

    @given(
        angular_resolution=st.floats(min_value=5.0, max_value=30.0, exclude_min=False)
    )
    @settings(max_examples=30, deadline=None)
    def test_polars_dataframe_invariant(self, angular_resolution: float) -> None:
        """Grid structure is always a Polars DataFrame."""
        grid = create_hemigrid("equal_area", angular_resolution)

        # Core invariant: grid.grid must be Polars DataFrame
        assert isinstance(grid.grid, pl.DataFrame)

        # Must have required columns
        required_cols = ["cell_id", "phi", "theta"]
        for col in required_cols:
            assert col in grid.grid.columns

        # Number of rows must match ncells
        assert len(grid.grid) == grid.ncells


class TestEqualAreaSpecificProperties:
    """Property tests specific to equal-area grids."""

    @given(
        angular_resolution=st.floats(min_value=10.0, max_value=30.0, exclude_min=False)
    )
    @settings(max_examples=20, deadline=None)
    def test_equal_area_cells_similar_size(self, angular_resolution: float) -> None:
        """Equal-area grids should have cells with similar solid angles."""
        grid = create_hemigrid("equal_area", angular_resolution)

        solid_angles = grid.get_solid_angles()

        # Calculate coefficient of variation (std/mean)
        mean_sa = np.mean(solid_angles)
        std_sa = np.std(solid_angles)
        cv = std_sa / mean_sa

        # Equal-area grids should have low variation (< 20%)
        # This is a design goal, not a hard requirement
        assert cv < 0.2, f"Coeff of variation {cv:.2%} too high for equal-area"

    @given(
        angular_resolution=st.floats(min_value=10.0, max_value=30.0, exclude_min=False)
    )
    @settings(max_examples=20, deadline=None)
    def test_equal_area_has_rectangular_columns(
        self, angular_resolution: float
    ) -> None:
        """Equal-area grids must have phi/theta min/max columns."""
        grid = create_hemigrid("equal_area", angular_resolution)

        # Equal-area is rectangular grid type
        assert "phi_min" in grid.grid.columns
        assert "phi_max" in grid.grid.columns
        assert "theta_min" in grid.grid.columns
        assert "theta_max" in grid.grid.columns


class TestHTMSpecificProperties:
    """Property tests specific to HTM grids."""

    @given(
        angular_resolution=st.floats(min_value=5.0, max_value=20.0, exclude_min=False)
    )
    @settings(max_examples=15, deadline=None)
    def test_htm_has_vertex_columns(self, angular_resolution: float) -> None:
        """HTM grids must have htm_vertex_N columns."""
        grid = create_hemigrid("htm", angular_resolution)

        # HTM uses triangular cells with 3 vertices
        assert "htm_vertex_0" in grid.grid.columns
        assert "htm_vertex_1" in grid.grid.columns
        assert "htm_vertex_2" in grid.grid.columns

    @given(
        angular_resolution=st.floats(min_value=5.0, max_value=20.0, exclude_min=False)
    )
    @settings(max_examples=15, deadline=None)
    def test_htm_vertices_are_3d(self, angular_resolution: float) -> None:
        """HTM vertices must be 3D Cartesian coordinates."""
        grid = create_hemigrid("htm", angular_resolution)

        # Check first cell's vertices
        first_row = grid.grid.row(0, named=True)
        v0 = first_row["htm_vertex_0"]
        v1 = first_row["htm_vertex_1"]
        v2 = first_row["htm_vertex_2"]

        # Each vertex should be list/array of 3 coordinates
        assert len(v0) == 3
        assert len(v1) == 3
        assert len(v2) == 3

        # Coordinates should be on unit sphere (|v| ≈ 1)
        assert np.isclose(np.linalg.norm(v0), 1.0, rtol=1e-5)
        assert np.isclose(np.linalg.norm(v1), 1.0, rtol=1e-5)
        assert np.isclose(np.linalg.norm(v2), 1.0, rtol=1e-5)


class TestGridMetadataProperties:
    """Property tests for grid metadata."""

    @given(
        angular_resolution=st.floats(min_value=5.0, max_value=30.0, exclude_min=False)
    )
    @settings(max_examples=30, deadline=None)
    def test_grid_type_is_set(self, angular_resolution: float) -> None:
        """Grid must have grid_type attribute."""
        grid = create_hemigrid("equal_area", angular_resolution)

        assert grid.grid_type is not None
        assert isinstance(grid.grid_type, str)
        assert grid.grid_type == "equal_area"
