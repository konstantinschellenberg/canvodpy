# """
# Grid storage and loading for icechunk/zarr.

# Handles serialization of HemiGrid structures with:
# - Cell centers and properties
# - Ragged vertex arrays
# - Neighbor adjacency
# - KDTree caching for fast spatial lookups
# """

# import hashlib
# import json
# from pathlib import Path
# from typing import Optional, Dict, Any
# import warnings

# import numpy as np
# import polars as pl
# from scipy.spatial import cKDTree
# import zarr
# from pydantic import BaseModel, Field, field_validator

# from gnssvodpy.logging.context import get_logger

# # ==============================================================================
# # PYDANTIC MODELS FOR VALIDATION
# # ==============================================================================

# class CoordinateSystem(BaseModel):
#     """Coordinate system documentation."""
#     phi_description: str = Field(
#         default="azimuth [0, 2π) radians, 0=North, increases clockwise",
#         description="Azimuthal angle convention")
#     theta_description: str = Field(
#         default="polar angle [0, π/2] radians from zenith",
#         description="Polar angle convention")
#     reference: str = Field(default="unit sphere, radius = 1",
#                            description="Coordinate reference")

# class GridMetadata(BaseModel):
#     """Grid-level metadata for validation."""
#     grid_type: str = Field(...,
#                            description="Grid type (htm, equal_area, geodesic)")
#     angular_resolution: float = Field(
#         ..., gt=0, description="Angular resolution in degrees")
#     cutoff_theta: float = Field(...,
#                                 ge=0,
#                                 le=np.pi / 2,
#                                 description="Cutoff angle in radians")
#     ncells: int = Field(..., gt=0, description="Number of cells")
#     coordinate_system: CoordinateSystem = Field(
#         default_factory=CoordinateSystem)
#     creation_timestamp: str = Field(..., description="ISO 8601 timestamp")
#     creation_software: str = Field(..., description="Software version")
#     grid_hash: str = Field(..., description="SHA256 hash of grid definition")
#     version: str = Field(default="1.0", description="Grid format version")
#     immutable: bool = Field(default=True, description="Grid is immutable")

#     @field_validator('grid_type')
#     @classmethod
#     def validate_grid_type(cls, v):
#         allowed = [
#             'htm', 'equal_area', 'geodesic', 'equirectangular', 'equal_angle'
#         ]
#         if v not in allowed:
#             raise ValueError(f"grid_type must be one of {allowed}")
#         return v

# class GridSpecificMetadata(BaseModel):
#     """Grid-type-specific metadata."""
#     htm_level: Optional[int] = Field(None,
#                                      ge=0,
#                                      description="HTM subdivision level")
#     base_triangles: Optional[int] = Field(
#         None, description="Number of base triangles")
#     n_theta_bands: Optional[int] = Field(
#         None, ge=1, description="Number of latitude bands")
#     cells_per_band: Optional[list[int]] = Field(
#         None, description="Cells per latitude band")
#     subdivision_frequency: Optional[int] = Field(
#         None, ge=1, description="Geodesic subdivision")
#     base_icosahedron: Optional[bool] = Field(
#         None, description="Based on icosahedron")

# # ==============================================================================
# # GRID HASH COMPUTATION
# # ==============================================================================

# def compute_grid_hash(phi: np.ndarray, theta: np.ndarray, grid_type: str,
#                       angular_resolution: float) -> str:
#     """
#     Compute SHA256 hash of grid definition.

#     Used for cache invalidation and grid verification.

#     Parameters
#     ----------
#     phi : np.ndarray
#         Cell center azimuths
#     theta : np.ndarray
#         Cell center polar angles
#     grid_type : str
#         Grid type
#     angular_resolution : float
#         Angular resolution

#     Returns
#     -------
#     str
#         SHA256 hash (hex digest)
#     """
#     hasher = hashlib.sha256()

#     # Hash cell centers (deterministic ordering)
#     hasher.update(phi.tobytes())
#     hasher.update(theta.tobytes())

#     # Hash metadata
#     hasher.update(grid_type.encode())
#     hasher.update(str(angular_resolution).encode())

#     return hasher.hexdigest()

# # ==============================================================================
# # KDTREE CACHING
# # ==============================================================================

# class KDTreeCache:
#     """Manages local caching of KDTrees for fast grid loading."""

#     def __init__(self, cache_dir: Optional[Path] = None):
#         """
#         Initialize KDTree cache.

#         Parameters
#         ----------
#         cache_dir : Path, optional
#             Cache directory. Defaults to ~/.cache/gnssvodpy/grids/
#         """
#         if cache_dir is None:
#             cache_dir = Path.home() / '.cache' / 'gnssvodpy' / 'grids'

#         self.cache_dir = cache_dir
#         self.cache_dir.mkdir(parents=True, exist_ok=True)
#         self._logger = get_logger()

#     def get_cache_path(self, grid_name: str, grid_hash: str) -> Path:
#         """Get cache file path for a grid."""
#         return self.cache_dir / f"{grid_name}_{grid_hash[:16]}.kdtree.pkl"

#     def load(self, grid_name: str, grid_hash: str) -> Optional[cKDTree]:
#         """
#         Load KDTree from cache.

#         Returns None if not found or corrupted.
#         """
#         cache_path = self.get_cache_path(grid_name, grid_hash)

#         if not cache_path.exists():
#             self._logger.debug(f"KDTree cache miss: {cache_path.name}")
#             return None

#         try:
#             import pickle
#             with open(cache_path, 'rb') as f:
#                 tree = pickle.load(f)

#             self._logger.debug(f"KDTree loaded from cache: {cache_path.name}")
#             return tree

#         except Exception as e:
#             self._logger.warning(f"Failed to load cached KDTree: {e}")
#             # Delete corrupted cache
#             cache_path.unlink(missing_ok=True)
#             return None

#     def save(self, tree: cKDTree, grid_name: str, grid_hash: str) -> None:
#         """Save KDTree to cache."""
#         cache_path = self.get_cache_path(grid_name, grid_hash)

#         try:
#             import pickle
#             with open(cache_path, 'wb') as f:
#                 pickle.dump(tree, f, protocol=pickle.HIGHEST_PROTOCOL)

#             self._logger.debug(f"KDTree cached: {cache_path.name}")

#         except Exception as e:
#             self._logger.warning(f"Failed to cache KDTree: {e}")

#     def clear(self, grid_name: Optional[str] = None) -> int:
#         """
#         Clear cache files.

#         Parameters
#         ----------
#         grid_name : str, optional
#             If provided, only clear this grid. Otherwise clear all.

#         Returns
#         -------
#         int
#             Number of files deleted
#         """
#         if grid_name:
#             pattern = f"{grid_name}_*.kdtree.pkl"
#         else:
#             pattern = "*.kdtree.pkl"

#         deleted = 0
#         for cache_file in self.cache_dir.glob(pattern):
#             cache_file.unlink()
#             deleted += 1

#         self._logger.info(f"Cleared {deleted} cached KDTree(s)")
#         return deleted

# # ==============================================================================
# # GRID WRITER
# # ==============================================================================

# def write_grid_to_zarr(zarr_group: zarr.Group,
#                        grid_name: str,
#                        df_cells: pl.DataFrame,
#                        df_vertices: Optional[pl.DataFrame] = None,
#                        df_neighbors: Optional[pl.DataFrame] = None,
#                        metadata: Optional[Dict[str, Any]] = None,
#                        specific_metadata: Optional[Dict[str, Any]] = None,
#                        compressor: Optional[zarr.Compressor] = None,
#                        ) -> str:
#     """
#     Write grid to zarr group.

#     Parameters
#     ----------
#     zarr_group : zarr.Group
#         Parent zarr group (e.g., store root)
#     grid_name : str
#         Grid identifier (e.g., 'htm_10deg')
#     df_cells : pl.DataFrame
#         Cell data with columns: cell_id, phi, theta, x, y, z, solid_angle,
#         is_boundary, phi_min, phi_max, theta_min, theta_max
#     df_vertices : pl.DataFrame, optional
#         Vertex data with columns: cell_id, vertex_idx, phi, theta, x, y, z
#     df_neighbors : pl.DataFrame, optional
#         Neighbor data with columns: cell_id, neighbor_id
#     metadata : Dict, optional
#         Grid metadata (will be validated against GridMetadata)
#     specific_metadata : Dict, optional
#         Grid-specific metadata
#     compressor : zarr.Compressor, optional
#         Compressor (default: zstd level 5)

#     Returns
#     -------
#     str
#         Grid hash for verification
#     """
#     logger = get_logger()

#     if compressor is None:
#         compressor = zarr.Blosc(cname='zstd', clevel=5, shuffle=2)

#     logger.info(f"Writing grid '{grid_name}' to zarr...")

#     # Create grid group
#     grid_group = zarr_group.require_group(f'grids/{grid_name}')

#     # Compute grid hash
#     grid_hash = compute_grid_hash(df_cells['phi'].to_numpy(),
#                                   df_cells['theta'].to_numpy(),
#                                   metadata['grid_type'],
#                                   metadata['angular_resolution'])

#     # Add hash to metadata
#     metadata['grid_hash'] = grid_hash

#     # Validate metadata
#     try:
#         grid_meta = GridMetadata(**metadata)
#         grid_group.attrs.update(grid_meta.model_dump())
#     except Exception as e:
#         logger.error(f"Metadata validation failed: {e}")
#         raise

#     # Write cells
#     logger.debug("  Writing cell data...")
#     cells_group = grid_group.require_group('cells')

#     for col in df_cells.columns:
#         data = df_cells[col].to_numpy()

#         # Determine dtype
#         if col == 'cell_id':
#             dtype = np.int32
#         elif col == 'is_boundary':
#             dtype = np.uint8
#         else:
#             dtype = np.float32

#         cells_group.array(
#             name=col,
#             data=data.astype(dtype),
#             chunks=(len(data), ),  # Single chunk
#             compressor=compressor,
#             overwrite=True)

#     logger.debug(f"  ✓ Wrote {len(df_cells)} cells")

#     # Write vertices (ragged)
#     if df_vertices is not None:
#         logger.debug("  Writing vertex data...")
#         vertices_group = grid_group.require_group('vertices')

#         # Group by cell_id to create ragged structure
#         grouped = df_vertices.group_by('cell_id', maintain_order=True)

#         # Flatten all vertices
#         for col in ['phi', 'theta', 'x', 'y', 'z']:
#             if col in df_vertices.columns:
#                 vertices_group.array(name=col,
#                                      data=df_vertices[col].to_numpy().astype(
#                                          np.float32),
#                                      chunks=(10000, ),
#                                      compressor=compressor,
#                                      overwrite=True)

#         # Create offsets array
#         offsets = [0]
#         for cell_id, group_df in grouped:
#             offsets.append(offsets[-1] + len(group_df))

#         vertices_group.array(name='offsets',
#                              data=np.array(offsets, dtype=np.int32),
#                              chunks=(len(offsets), ),
#                              compressor=compressor,
#                              overwrite=True)

#         logger.debug(f"  ✓ Wrote {len(df_vertices)} vertices")

#     # Write neighbors (ragged)
#     if df_neighbors is not None:
#         logger.debug("  Writing neighbor data...")
#         neighbors_group = grid_group.require_group('neighbors')

#         # Group by cell_id
#         grouped = df_neighbors.group_by('cell_id', maintain_order=True)

#         # Flatten all neighbor IDs
#         neighbors_group.array(
#             name='cell_ids',
#             data=df_neighbors['neighbor_id'].to_numpy().astype(np.int32),
#             chunks=(10000, ),
#             compressor=compressor,
#             overwrite=True)

#         # Create offsets
#         offsets = [0]
#         for cell_id, group_df in grouped:
#             offsets.append(offsets[-1] + len(group_df))

#         neighbors_group.array(name='offsets',
#                               data=np.array(offsets, dtype=np.int32),
#                               chunks=(len(offsets), ),
#                               compressor=compressor,
#                               overwrite=True)

#         logger.debug(f"  ✓ Wrote {len(df_neighbors)} neighbor relationships")

#     # Write grid-specific metadata
#     if specific_metadata:
#         metadata_group = grid_group.require_group('metadata')
#         try:
#             spec_meta = GridSpecificMetadata(**specific_metadata)
#             metadata_group.attrs.update(
#                 spec_meta.model_dump(exclude_none=True))
#         except Exception as e:
#             logger.warning(f"Grid-specific metadata validation failed: {e}")
#             metadata_group.attrs.update(specific_metadata)

#     logger.info(f"✓ Grid '{grid_name}' written successfully")
#     logger.info(f"  Hash: {grid_hash[:16]}...")

#     return grid_hash

# # ==============================================================================
# # GRID LOADER
# # ==============================================================================

# class LoadedGrid:
#     """
#     Container for loaded grid data with lazy KDTree construction.

#     Attributes
#     ----------
#     grid_name : str
#         Grid identifier
#     cells : pl.DataFrame
#         Cell data
#     vertices : pl.DataFrame, optional
#         Vertex data (reconstructed from ragged)
#     neighbors : pl.DataFrame, optional
#         Neighbor data (reconstructed from ragged)
#     metadata : GridMetadata
#         Validated metadata
#     specific_metadata : dict
#         Grid-specific metadata
#     kdtree : cKDTree
#         Spatial index (lazy loaded)
#     """

#     def __init__(self,
#                  grid_name: str,
#                  cells: pl.DataFrame,
#                  metadata: GridMetadata,
#                  vertices: Optional[pl.DataFrame] = None,
#                  neighbors: Optional[pl.DataFrame] = None,
#                  specific_metadata: Optional[Dict] = None):
#         self.grid_name = grid_name
#         self.cells = cells
#         self.vertices = vertices
#         self.neighbors = neighbors
#         self.metadata = metadata
#         self.specific_metadata = specific_metadata or {}

#         self._kdtree = None
#         self._kdtree_cache = KDTreeCache()
#         self._logger = get_logger()

#     @property
#     def kdtree(self) -> cKDTree:
#         """Get KDTree, building/loading from cache if needed."""
#         if self._kdtree is None:
#             # Try cache first
#             self._kdtree = self._kdtree_cache.load(self.grid_name,
#                                                    self.metadata.grid_hash)

#             if self._kdtree is None:
#                 # Build from scratch
#                 self._logger.info(f"Building KDTree for '{self.grid_name}'...")
#                 xyz = np.column_stack([
#                     self.cells['x'].to_numpy(), self.cells['y'].to_numpy(),
#                     self.cells['z'].to_numpy()
#                 ])
#                 self._kdtree = cKDTree(xyz)

#                 # Cache it
#                 self._kdtree_cache.save(self._kdtree, self.grid_name,
#                                         self.metadata.grid_hash)
#                 self._logger.info("  ✓ KDTree built and cached")

#         return self._kdtree

#     def query_point(self, phi: float, theta: float) -> int:
#         """
#         Find cell ID for a point.

#         Parameters
#         ----------
#         phi : float
#             Azimuth angle
#         theta : float
#             Polar angle

#         Returns
#         -------
#         int
#             Cell ID
#         """
#         # Convert to Cartesian
#         x = np.sin(theta) * np.cos(phi)
#         y = np.sin(theta) * np.sin(phi)
#         z = np.cos(theta)

#         # Query KDTree
#         _, idx = self.kdtree.query([x, y, z])

#         return int(self.cells['cell_id'][idx])

#     def query_points(self, phi: np.ndarray, theta: np.ndarray) -> np.ndarray:
#         """
#         Find cell IDs for multiple points (vectorized).

#         Parameters
#         ----------
#         phi : np.ndarray
#             Azimuth angles
#         theta : np.ndarray
#             Polar angles

#         Returns
#         -------
#         np.ndarray
#             Cell IDs
#         """
#         # Convert to Cartesian
#         x = np.sin(theta) * np.cos(phi)
#         y = np.sin(theta) * np.sin(phi)
#         z = np.cos(theta)
#         xyz = np.column_stack([x, y, z])

#         # Query KDTree
#         _, indices = self.kdtree.query(xyz, workers=-1)

#         return self.cells['cell_id'][indices].to_numpy()

#     def __repr__(self) -> str:
#         return (f"LoadedGrid(name='{self.grid_name}', "
#                 f"type='{self.metadata.grid_type}', "
#                 f"ncells={self.metadata.ncells})")

# def load_grid_from_zarr(zarr_group: zarr.Group,
#                         grid_name: str,
#                         load_vertices: bool = True,
#                         load_neighbors: bool = True) -> LoadedGrid:
#     """
#     Load grid from zarr group.

#     Parameters
#     ----------
#     zarr_group : zarr.Group
#         Parent zarr group
#     grid_name : str
#         Grid identifier
#     load_vertices : bool
#         Load vertex data
#     load_neighbors : bool
#         Load neighbor data

#     Returns
#     -------
#     LoadedGrid
#         Loaded grid with lazy KDTree
#     """
#     logger = get_logger()
#     logger.info(f"Loading grid '{grid_name}' from zarr...")

#     grid_group = zarr_group[f'grids/{grid_name}']

#     # Load and validate metadata
#     metadata = GridMetadata(**dict(grid_group.attrs))

#     # Load cells
#     cells_group = grid_group['cells']
#     cell_data = {
#         name: cells_group[name][:]
#         for name in cells_group.array_keys()
#     }
#     df_cells = pl.DataFrame(cell_data)

#     logger.debug(f"  ✓ Loaded {len(df_cells)} cells")

#     # Load vertices if requested
#     df_vertices = None
#     if load_vertices and 'vertices' in grid_group:
#         vertices_group = grid_group['vertices']
#         offsets = vertices_group['offsets'][:]

#         # Reconstruct vertex DataFrame from ragged arrays
#         vertex_rows = []
#         for cell_idx in range(len(offsets) - 1):
#             start = offsets[cell_idx]
#             end = offsets[cell_idx + 1]

#             for vert_idx, i in enumerate(range(start, end)):
#                 vertex_rows.append({
#                     'cell_id': df_cells['cell_id'][cell_idx],
#                     'vertex_idx': vert_idx,
#                     'phi': vertices_group['phi'][i],
#                     'theta': vertices_group['theta'][i],
#                     'x': vertices_group['x'][i],
#                     'y': vertices_group['y'][i],
#                     'z': vertices_group['z'][i],
#                 })

#         df_vertices = pl.DataFrame(vertex_rows)
#         logger.debug(f"  ✓ Loaded {len(df_vertices)} vertices")

#     # Load neighbors if requested
#     df_neighbors = None
#     if load_neighbors and 'neighbors' in grid_group:
#         neighbors_group = grid_group['neighbors']
#         offsets = neighbors_group['offsets'][:]
#         neighbor_ids = neighbors_group['cell_ids'][:]

#         # Reconstruct neighbor DataFrame
#         neighbor_rows = []
#         for cell_idx in range(len(offsets) - 1):
#             start = offsets[cell_idx]
#             end = offsets[cell_idx + 1]

#             for i in range(start, end):
#                 neighbor_rows.append({
#                     'cell_id': df_cells['cell_id'][cell_idx],
#                     'neighbor_id': neighbor_ids[i]
#                 })

#         df_neighbors = pl.DataFrame(neighbor_rows)
#         logger.debug(f"  ✓ Loaded {len(df_neighbors)} neighbor relationships")

#     # Load grid-specific metadata
#     specific_metadata = {}
#     if 'metadata' in grid_group:
#         specific_metadata = dict(grid_group['metadata'].attrs)

#     logger.info(f"✓ Grid '{grid_name}' loaded successfully")

#     return LoadedGrid(grid_name=grid_name,
#                       cells=df_cells,
#                       metadata=metadata,
#                       vertices=df_vertices,
#                       neighbors=df_neighbors,
#                       specific_metadata=specific_metadata)
"""
Grid storage and loading for icechunk/zarr.

Handles serialization of HemiGrid structures with:
- Cell centers and properties
- Ragged vertex arrays
- Neighbor adjacency
- KDTree caching for fast spatial lookups
"""

import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import xarray as xr
import zarr
from pydantic import BaseModel, Field, field_validator
from scipy.spatial import cKDTree

# Icechunk integration
try:
    from icechunk.xarray import to_icechunk
except ImportError:  # pragma: no cover - optional dependency for documentation builds
    to_icechunk = None  # type: ignore[assignment]

# Try different import paths for Blosc compressor
try:
    from zarr.codecs import Blosc
except ImportError:
    try:
        from zarr import Blosc
    except ImportError:
        # Fallback for older zarr versions
        try:
            from numcodecs import Blosc
        except ImportError:
            # If all fails, create a simple wrapper
            class Blosc:
                """Fallback Blosc codec wrapper for older zarr versions."""

                def __init__(
                    self,
                    cname: str = "zstd",
                    clevel: int = 5,
                    shuffle: int = 2,
                ) -> None:
                    """Initialize the codec configuration.

                    Parameters
                    ----------
                    cname : str, default "zstd"
                        Compression algorithm name.
                    clevel : int, default 5
                        Compression level.
                    shuffle : int, default 2
                        Shuffle mode.
                    """
                    self.cname = cname
                    self.clevel = clevel
                    self.shuffle = shuffle


from gnssvodpy.logging.context import get_logger

try:  # Polars dtype helpers (0.20+)
    from polars.datatypes import (  # type: ignore[attr-defined]
        is_boolean_dtype,
        is_numeric_dtype,
    )
except (ImportError, AttributeError):  # pragma: no cover - legacy Polars

    def is_numeric_dtype(dtype: Any) -> bool:
        """Return True if the dtype is numeric.

        Parameters
        ----------
        dtype : Any
            Polars dtype to check.

        Returns
        -------
        bool
            True if dtype is numeric.
        """
        numeric_dtypes = {
            pl.Int8,
            pl.Int16,
            pl.Int32,
            pl.Int64,
            pl.UInt8,
            pl.UInt16,
            pl.UInt32,
            pl.UInt64,
            pl.Float32,
            pl.Float64,
        }
        return dtype in numeric_dtypes

    def is_boolean_dtype(dtype: Any) -> bool:
        """Return True if the dtype is boolean.

        Parameters
        ----------
        dtype : Any
            Polars dtype to check.

        Returns
        -------
        bool
            True if dtype is boolean.
        """
        return dtype == pl.Boolean

# ==============================================================================
# PYDANTIC MODELS FOR VALIDATION
# ==============================================================================


class CoordinateSystem(BaseModel):
    """Coordinate system metadata for grid validation.

    Notes
    -----
    This is a Pydantic model used to validate serialized metadata fields.
    """

    phi_description: str = Field(
        default="azimuth [0, 2π) radians, 0=North, increases clockwise",
        description="Azimuthal angle convention",
    )
    theta_description: str = Field(
        default="polar angle [0, π/2] radians from zenith",
        description="Polar angle convention",
    )
    reference: str = Field(
        default="unit sphere, radius = 1", description="Coordinate reference"
    )


class GridMetadata(BaseModel):
    """Grid-level metadata for validation.

    Notes
    -----
    This is a Pydantic model used to validate serialized metadata fields.
    """

    grid_type: str = Field(..., description="Grid type (htm, equal_area, geodesic)")
    angular_resolution: float = Field(
        ..., gt=0, description="Angular resolution in degrees"
    )
    cutoff_theta: float = Field(
        ..., ge=0, le=np.pi / 2, description="Cutoff angle in radians"
    )
    ncells: int = Field(..., gt=0, description="Number of cells")
    coordinate_system: CoordinateSystem = Field(default_factory=CoordinateSystem)
    creation_timestamp: str = Field(..., description="ISO 8601 timestamp")
    creation_software: str = Field(..., description="Software version")
    grid_hash: str = Field(..., description="SHA256 hash of grid definition")
    version: str = Field(default="1.0", description="Grid format version")
    immutable: bool = Field(default=True, description="Grid is immutable")

    @field_validator("grid_type")
    @classmethod
    def validate_grid_type(cls, v: str) -> str:
        """Validate grid type.

        Parameters
        ----------
        v : str
            Grid type string.

        Returns
        -------
        str
            Validated grid type.
        """
        allowed = [
            "htm",
            "equal_area",
            "geodesic",
            "equirectangular",
            "equal_angle",
            "healpix",
            "fibonacci",
        ]
        if v not in allowed:
            raise ValueError(f"grid_type must be one of {allowed}")
        return v


class GridSpecificMetadata(BaseModel):
    """Grid-type-specific metadata for validation.

    Notes
    -----
    This is a Pydantic model used to validate serialized metadata fields.
    """

    htm_level: int | None = Field(None, ge=0, description="HTM subdivision level")
    base_triangles: int | None = Field(None, description="Number of base triangles")
    n_theta_bands: int | None = Field(
        None, ge=1, description="Number of latitude bands"
    )
    cells_per_band: list[int] | None = Field(
        None, description="Cells per latitude band"
    )
    subdivision_frequency: int | None = Field(
        None, ge=1, description="Geodesic subdivision"
    )
    base_icosahedron: bool | None = Field(None, description="Based on icosahedron")
    healpix_nside: int | None = Field(None, ge=1, description="HEALPix nside parameter")
    fibonacci_n_points: int | None = Field(
        None, ge=1, description="Number of Fibonacci spiral points"
    )


# ==============================================================================
# COMPRESSION UTILITIES
# ==============================================================================


def get_default_compressor() -> Blosc | dict | None:
    """
    Get default compressor configuration.

    Returns
    -------
    Union[Blosc, dict, None]
        Default compressor instance or configuration
    """
    try:
        return Blosc(cname="zstd", clevel=5, shuffle=2)
    except Exception:
        # Fallback to dict configuration
        return {"id": "blosc", "cname": "zstd", "clevel": 5, "shuffle": 2}


def configure_compressor(compressor: Blosc | dict | str | None) -> Blosc | dict | None:
    """
    Configure compressor from various input types.

    Parameters
    ----------
    compressor : Blosc, dict, str, optional
        Compressor configuration

    Returns
    -------
    Union[Blosc, dict, None]
        Configured compressor
    """
    if compressor is None:
        return get_default_compressor()
    elif isinstance(compressor, str):
        # Simple string like 'zstd' -> default config
        try:
            return Blosc(cname=compressor, clevel=5, shuffle=2)
        except Exception:
            return {"id": "blosc", "cname": compressor, "clevel": 5, "shuffle": 2}
    elif isinstance(compressor, dict):
        # Dict config -> create Blosc instance if possible
        try:
            return Blosc(**compressor)
        except Exception:
            # Return dict as-is for zarr to handle
            return compressor
    else:
        # Assume it's already a proper compressor instance
        return compressor


# ==============================================================================
# GRID HASH COMPUTATION
# ==============================================================================


def compute_grid_hash(
    phi: np.ndarray, theta: np.ndarray, grid_type: str, angular_resolution: float
) -> str:
    """
    Compute SHA256 hash of grid definition.

    Used for cache invalidation and grid verification.

    Parameters
    ----------
    phi : np.ndarray
        Cell center azimuths
    theta : np.ndarray
        Cell center polar angles
    grid_type : str
        Grid type
    angular_resolution : float
        Angular resolution

    Returns
    -------
    str
        SHA256 hash (hex digest)
    """
    hasher = hashlib.sha256()

    # Hash cell centers (deterministic ordering)
    hasher.update(phi.tobytes())
    hasher.update(theta.tobytes())

    # Hash metadata
    hasher.update(grid_type.encode())
    hasher.update(str(angular_resolution).encode())

    return hasher.hexdigest()


# ==============================================================================
# KDTREE CACHING
# ==============================================================================


class KDTreeCache:
    """
    Manage local caching of KDTrees for fast grid loading.

    Parameters
    ----------
    cache_dir : Path, optional
        Cache directory. Defaults to ~/.cache/gnssvodpy/grids/.
    """

    def __init__(self, cache_dir: Path | None = None) -> None:
        """Initialize the cache.

        Parameters
        ----------
        cache_dir : Path | None, optional
            Cache directory. Defaults to ~/.cache/gnssvodpy/grids/.
        """
        if cache_dir is None:
            cache_dir = Path.home() / ".cache" / "gnssvodpy" / "grids"

        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._logger = get_logger()

    def get_cache_path(self, grid_name: str, grid_hash: str) -> Path:
        """Get cache file path for a grid."""
        return self.cache_dir / f"{grid_name}_{grid_hash[:16]}.kdtree.pkl"

    def load(self, grid_name: str, grid_hash: str) -> cKDTree | None:
        """
        Load KDTree from cache.

        Returns None if not found or corrupted.
        """
        cache_path = self.get_cache_path(grid_name, grid_hash)

        if not cache_path.exists():
            self._logger.debug(f"KDTree cache miss: {cache_path.name}")
            return None

        try:
            import pickle

            with open(cache_path, "rb") as f:
                tree = pickle.load(f)

            self._logger.debug(f"KDTree loaded from cache: {cache_path.name}")
            return tree

        except Exception as e:
            self._logger.warning(f"Failed to load cached KDTree: {e}")
            # Delete corrupted cache
            cache_path.unlink(missing_ok=True)
            return None

    def save(self, tree: cKDTree, grid_name: str, grid_hash: str) -> None:
        """Save KDTree to cache.

        Parameters
        ----------
        tree : cKDTree
            KDTree to serialize.
        grid_name : str
            Grid identifier.
        grid_hash : str
            Grid hash for cache keying.

        Returns
        -------
        None
        """
        cache_path = self.get_cache_path(grid_name, grid_hash)

        try:
            import pickle

            with open(cache_path, "wb") as f:
                pickle.dump(tree, f, protocol=pickle.HIGHEST_PROTOCOL)

            self._logger.debug(f"KDTree cached: {cache_path.name}")

        except Exception as e:
            self._logger.warning(f"Failed to cache KDTree: {e}")

    def clear(self, grid_name: str | None = None) -> int:
        """
        Clear cache files.

        Parameters
        ----------
        grid_name : str, optional
            If provided, only clear this grid. Otherwise clear all.

        Returns
        -------
        int
            Number of files deleted
        """
        if grid_name:
            pattern = f"{grid_name}_*.kdtree.pkl"
        else:
            pattern = "*.kdtree.pkl"

        deleted = 0
        for cache_file in self.cache_dir.glob(pattern):
            cache_file.unlink()
            deleted += 1

        self._logger.info(f"Cleared {deleted} cached KDTree(s)")
        return deleted


# ==============================================================================
# GRID LOADER
# ==============================================================================


class LoadedGrid:
    """
    Container for loaded grid data with lazy KDTree construction.

    Attributes
    ----------
    grid_name : str
        Grid identifier
    cells : pl.DataFrame
        Cell data
    vertices : pl.DataFrame, optional
        Vertex data (reconstructed from ragged)
    neighbors : pl.DataFrame, optional
        Neighbor data (reconstructed from ragged)
    metadata : GridMetadata
        Validated metadata
    specific_metadata : dict
        Grid-specific metadata
    kdtree : cKDTree
        Spatial index (lazy loaded)
    """

    def __init__(
        self,
        grid_name: str,
        cells: pl.DataFrame,
        metadata: GridMetadata,
        vertices: pl.DataFrame | None = None,
        neighbors: pl.DataFrame | None = None,
        specific_metadata: dict | None = None,
    ) -> None:
        """Initialize a loaded grid container.

        Parameters
        ----------
        grid_name : str
            Grid identifier.
        cells : pl.DataFrame
            Cells table.
        metadata : GridMetadata
            Validated grid metadata.
        vertices : pl.DataFrame | None, optional
            Vertices table if available.
        neighbors : pl.DataFrame | None, optional
            Neighbors table if available.
        specific_metadata : dict | None, optional
            Grid-type-specific metadata.
        """
        self.grid_name = grid_name
        self.cells = cells
        self.vertices = vertices
        self.neighbors = neighbors
        self.metadata = metadata
        self.specific_metadata = specific_metadata or {}

        self._kdtree = None
        self._kdtree_cache = KDTreeCache()
        self._logger = get_logger()

    @property
    def kdtree(self) -> cKDTree:
        """Get KDTree, building/loading from cache if needed."""
        if self._kdtree is None:
            # Try cache first
            self._kdtree = self._kdtree_cache.load(
                self.grid_name,
                self.metadata.grid_hash,
            )

            if self._kdtree is None:
                # Build from scratch
                self._logger.info(f"Building KDTree for '{self.grid_name}'...")
                xyz = np.column_stack(
                    [
                        self.cells["x"].to_numpy(),
                        self.cells["y"].to_numpy(),
                        self.cells["z"].to_numpy(),
                    ]
                )
                self._kdtree = cKDTree(xyz)

                # Cache it
                self._kdtree_cache.save(
                    self._kdtree,
                    self.grid_name,
                    self.metadata.grid_hash,
                )
                self._logger.info("  ✓ KDTree built and cached")

        return self._kdtree

    def query_point(self, phi: float, theta: float) -> int:
        """
        Find cell ID for a point.

        Parameters
        ----------
        phi : float
            Azimuth angle
        theta : float
            Polar angle

        Returns
        -------
        int
            Cell ID
        """
        # Convert to Cartesian
        x = np.sin(theta) * np.cos(phi)
        y = np.sin(theta) * np.sin(phi)
        z = np.cos(theta)

        # Query KDTree
        _, idx = self.kdtree.query([x, y, z])

        return int(self.cells["cell_id"][idx])

    def query_points(self, phi: np.ndarray, theta: np.ndarray) -> np.ndarray:
        """
        Find cell IDs for multiple points (vectorized).

        Parameters
        ----------
        phi : np.ndarray
            Azimuth angles
        theta : np.ndarray
            Polar angles

        Returns
        -------
        np.ndarray
            Cell IDs
        """
        # Convert to Cartesian
        x = np.sin(theta) * np.cos(phi)
        y = np.sin(theta) * np.sin(phi)
        z = np.cos(theta)
        xyz = np.column_stack([x, y, z])

        # Query KDTree
        _, indices = self.kdtree.query(xyz, workers=-1)

        return self.cells["cell_id"][indices].to_numpy()

    def get_cell_boundaries(self, cell_id: int) -> dict[str, Any] | None:
        """
        Get cell boundary information.

        Parameters
        ----------
        cell_id : int
            Cell identifier

        Returns
        -------
        dict or None
            Cell boundary data (phi_min, phi_max, theta_min, theta_max, etc.)
        """
        try:
            cell_row = self.cells.filter(pl.col("cell_id") == cell_id)
            if len(cell_row) == 0:
                return None
            return cell_row.to_dicts()[0]
        except Exception:
            return None

    def get_cell_vertices(self, cell_id: int) -> pl.DataFrame | None:
        """
        Get vertices for a specific cell.

        Parameters
        ----------
        cell_id : int
            Cell identifier

        Returns
        -------
        pl.DataFrame or None
            Vertex data for the cell
        """
        if self.vertices is None:
            return None

        try:
            return self.vertices.filter(pl.col("cell_id") == cell_id)
        except Exception:
            return None

    def get_cell_neighbors(self, cell_id: int) -> list | None:
        """
        Get neighbor cell IDs for a specific cell.

        Parameters
        ----------
        cell_id : int
            Cell identifier

        Returns
        -------
        list or None
            List of neighbor cell IDs
        """
        if self.neighbors is None:
            return None

        try:
            neighbors = self.neighbors.filter(pl.col("cell_id") == cell_id)
            return neighbors["neighbor_id"].to_list()
        except Exception:
            return None

    def __repr__(self) -> str:
        """Return the developer-facing representation.

        Returns
        -------
        str
            Representation string.
        """
        return (
            f"LoadedGrid(name='{self.grid_name}', "
            f"type='{self.metadata.grid_type}', "
            f"ncells={self.metadata.ncells})"
        )


def load_grid_from_zarr(
    zarr_group: zarr.Group,
    grid_name: str,
    load_vertices: bool = True,
    load_neighbors: bool = True,
) -> LoadedGrid:
    """
    Load grid from zarr group.

    Parameters
    ----------
    zarr_group : zarr.Group
        Parent zarr group
    grid_name : str
        Grid identifier
    load_vertices : bool
        Load vertex data
    load_neighbors : bool
        Load neighbor data

    Returns
    -------
    LoadedGrid
        Loaded grid with lazy KDTree
    """
    logger = get_logger()
    logger.info(f"Loading grid '{grid_name}' from zarr...")

    grid_group = zarr_group[f"grids/{grid_name}"]

    # Load and validate metadata
    metadata = GridMetadata(**dict(grid_group.attrs))

    # Load cells
    cells_group = grid_group["cells"]
    cell_data = {name: cells_group[name][:] for name in cells_group.array_keys()}
    df_cells = pl.DataFrame(cell_data)

    logger.debug(f"  ✓ Loaded {len(df_cells)} cells")

    # Load vertices if requested
    df_vertices = None
    if load_vertices and "vertices" in grid_group:
        vertices_group = grid_group["vertices"]
        offsets = vertices_group["offsets"][:]

        # Reconstruct vertex DataFrame from ragged arrays
        vertex_rows = []
        for cell_idx in range(len(offsets) - 1):
            start = offsets[cell_idx]
            end = offsets[cell_idx + 1]

            for vert_idx, i in enumerate(range(start, end)):
                vertex_rows.append(
                    {
                        "cell_id": df_cells["cell_id"][cell_idx],
                        "vertex_idx": vert_idx,
                        "phi": vertices_group["phi"][i],
                        "theta": vertices_group["theta"][i],
                        "x": vertices_group["x"][i],
                        "y": vertices_group["y"][i],
                        "z": vertices_group["z"][i],
                    }
                )

        df_vertices = pl.DataFrame(vertex_rows)
        logger.debug(f"  ✓ Loaded {len(df_vertices)} vertices")

    # Load neighbors if requested
    df_neighbors = None
    if load_neighbors and "neighbors" in grid_group:
        neighbors_group = grid_group["neighbors"]
        offsets = neighbors_group["offsets"][:]
        neighbor_ids = neighbors_group["cell_ids"][:]

        # Reconstruct neighbor DataFrame
        neighbor_rows = []
        for cell_idx in range(len(offsets) - 1):
            start = offsets[cell_idx]
            end = offsets[cell_idx + 1]

            for i in range(start, end):
                neighbor_rows.append(
                    {
                        "cell_id": df_cells["cell_id"][cell_idx],
                        "neighbor_id": neighbor_ids[i],
                    }
                )

        df_neighbors = pl.DataFrame(neighbor_rows)
        logger.debug(f"  ✓ Loaded {len(df_neighbors)} neighbor relationships")

    # Load grid-specific metadata
    specific_metadata = {}
    if "metadata" in grid_group:
        specific_metadata = dict(grid_group["metadata"].attrs)

    logger.info(f"✓ Grid '{grid_name}' loaded successfully")

    return LoadedGrid(
        grid_name=grid_name,
        cells=df_cells,
        metadata=metadata,
        vertices=df_vertices,
        neighbors=df_neighbors,
        specific_metadata=specific_metadata,
    )


# ==============================================================================
# GRID WRITER
# ==============================================================================


def write_grid_to_zarr(
    zarr_group: zarr.Group,
    grid_name: str,
    df_cells: pl.DataFrame,
    df_vertices: pl.DataFrame | None = None,
    df_neighbors: pl.DataFrame | None = None,
    metadata: dict[str, Any] | None = None,
    specific_metadata: dict[str, Any] | None = None,
    compressor: Blosc | dict | str | None = None,
) -> str:
    """
    Write grid to zarr group.

    Parameters
    ----------
    zarr_group : zarr.Group
        Parent zarr group (e.g., store root)
    grid_name : str
        Grid identifier (e.g., 'htm_10deg')
    df_cells : pl.DataFrame
        Cell data with columns: cell_id, phi, theta, x, y, z, solid_angle,
        is_boundary, phi_min, phi_max, theta_min, theta_max
    df_vertices : pl.DataFrame, optional
        Vertex data with columns: cell_id, vertex_idx, phi, theta, x, y, z
    df_neighbors : pl.DataFrame, optional
        Neighbor data with columns: cell_id, neighbor_id
    metadata : Dict, optional
        Grid metadata (will be validated against GridMetadata)
    specific_metadata : Dict, optional
        Grid-specific metadata
    compressor : Blosc, dict, str, optional
        Compressor configuration. Can be:
        - Blosc instance
        - dict with compressor config
        - string like 'zstd' for default config
        Default: Blosc with zstd level 5

    Returns
    -------
    str
        Grid hash for verification
    """
    logger = get_logger()

    # Configure compressor
    compressor = configure_compressor(compressor)

    logger.info(f"Writing grid '{grid_name}' to zarr...")

    # Create grid group
    grid_group = zarr_group.require_group(f"grids/{grid_name}")

    # Compute grid hash
    grid_hash = compute_grid_hash(
        df_cells["phi"].to_numpy(),
        df_cells["theta"].to_numpy(),
        metadata["grid_type"],
        metadata["angular_resolution"],
    )

    # Add hash to metadata
    metadata = metadata.copy() if metadata else {}
    metadata["grid_hash"] = grid_hash

    # Validate metadata
    try:
        grid_meta = GridMetadata(**metadata)
        grid_group.attrs.update(grid_meta.model_dump())
    except Exception as e:
        logger.error(f"Metadata validation failed: {e}")
        raise

    # Write cells
    logger.debug("  Writing cell data...")
    cells_group = grid_group.require_group("cells")

    for col in df_cells.columns:
        data = df_cells[col].to_numpy()

        # Determine dtype
        if col == "cell_id":
            dtype = np.int32
        elif col == "is_boundary":
            dtype = np.uint8
        else:
            dtype = np.float32

        cells_group.create_array(
            name=col,
            data=data.astype(dtype),
            chunks=(len(data),),  # Single chunk
            compressor=compressor,
            overwrite=True,
        )

    logger.debug(f"  ✓ Wrote {len(df_cells)} cells")

    # Write vertices (ragged)
    if df_vertices is not None:
        logger.debug("  Writing vertex data...")
        vertices_group = grid_group.require_group("vertices")

        # Group by cell_id to create ragged structure
        grouped = df_vertices.group_by("cell_id", maintain_order=True)

        # Flatten all vertices
        for col in ["phi", "theta", "x", "y", "z"]:
            if col in df_vertices.columns:
                vertices_group.array(
                    name=col,
                    data=df_vertices[col].to_numpy().astype(np.float32),
                    chunks=(10000,),
                    compressor=compressor,
                    overwrite=True,
                )

        # Create offsets array
        offsets = [0]
        for cell_id, group_df in grouped:
            offsets.append(offsets[-1] + len(group_df))

        vertices_group.create_array(
            name="offsets",
            data=np.array(offsets, dtype=np.int32),
            chunks=(len(offsets),),
            compressor=compressor,
            overwrite=True,
        )

        logger.debug(f"  ✓ Wrote {len(df_vertices)} vertices")

    # Write neighbors (ragged)
    if df_neighbors is not None:
        logger.debug("  Writing neighbor data...")
        neighbors_group = grid_group.require_group("neighbors")

        # Group by cell_id
        grouped = df_neighbors.group_by("cell_id", maintain_order=True)

        # Flatten all neighbor IDs
        neighbors_group.create_array(
            name="cell_ids",
            data=df_neighbors["neighbor_id"].to_numpy().astype(np.int32),
            chunks=(10000,),
            compressor=compressor,
            overwrite=True,
        )

        # Create offsets
        offsets = [0]
        for cell_id, group_df in grouped:
            offsets.append(offsets[-1] + len(group_df))

        neighbors_group.create_array(
            name="offsets",
            data=np.array(offsets, dtype=np.int32),
            chunks=(len(offsets),),
            compressor=compressor,
            overwrite=True,
        )

        logger.debug(f"  ✓ Wrote {len(df_neighbors)} neighbor relationships")

    # Write grid-specific metadata
    if specific_metadata:
        metadata_group = grid_group.require_group("metadata")
        try:
            spec_meta = GridSpecificMetadata(**specific_metadata)
            metadata_group.attrs.update(spec_meta.model_dump(exclude_none=True))
        except Exception as e:
            logger.warning(f"Grid-specific metadata validation failed: {e}")
            metadata_group.attrs.update(specific_metadata)

    logger.info(f"✓ Grid '{grid_name}' written successfully")
    logger.info(f"  Hash: {grid_hash[:16]}...")

    return grid_hash


def _compute_offsets(df_cells: pl.DataFrame, counts_df: pl.DataFrame) -> np.ndarray:
    """Compute ragged offsets aligned with cell ordering."""
    cell_ids = df_cells["cell_id"].to_numpy()
    offsets = np.zeros(len(cell_ids) + 1, dtype=np.int32)

    counts_map = {
        int(row["cell_id"]): int(row["len"]) for row in counts_df.iter_rows(named=True)
    }

    total = 0
    for idx, cell_id in enumerate(cell_ids):
        offsets[idx] = total
        total += counts_map.get(int(cell_id), 0)
    offsets[-1] = total
    return offsets


def _build_cells_dataset(df_cells: pl.DataFrame) -> xr.Dataset:
    """Convert cell dataframe to an xarray.Dataset suitable for Icechunk."""
    if df_cells.is_empty():
        raise ValueError("Grid must contain at least one cell.")

    logger = get_logger()

    dtype_overrides: dict[str, Any] = {
        "cell_id": np.int32,
        "is_boundary": np.bool_,
        "phi": np.float32,
        "theta": np.float32,
        "x": np.float32,
        "y": np.float32,
        "z": np.float32,
        "solid_angle": np.float32,
        "phi_min": np.float32,
        "phi_max": np.float32,
        "theta_min": np.float32,
        "theta_max": np.float32,
    }

    data_vars = {}
    for col, dtype in df_cells.schema.items():
        if not (is_numeric_dtype(dtype) or is_boolean_dtype(dtype)):
            logger.debug(f"Skipping non-scalar cell column '{col}' (dtype={dtype})")
            continue

        arr = df_cells[col].to_numpy()
        target_dtype = dtype_overrides.get(col)
        if target_dtype is not None:
            arr = arr.astype(target_dtype)
        data_vars[col] = ("cell", arr)

    coords = {"cell": ("cell", np.arange(df_cells.height, dtype=np.int32))}
    return xr.Dataset(data_vars=data_vars, coords=coords)


def _build_vertices_dataset(
    df_cells: pl.DataFrame, df_vertices: pl.DataFrame | None
) -> xr.Dataset | None:
    """Convert vertices dataframe to dataset with ragged representation."""
    if df_vertices is None or df_vertices.is_empty():
        return None

    df = df_vertices

    if "vertex_idx" not in df.columns:
        df = df.sort("cell_id").with_columns(
            pl.col("cell_id").cumcount().over("cell_id").alias("vertex_idx")
        )

    order_map = {
        int(cell_id): idx for idx, cell_id in enumerate(df_cells["cell_id"].to_numpy())
    }

    order_df = pl.DataFrame(
        {"cell_id": list(order_map.keys()), "_cell_order": list(order_map.values())}
    )

    df = (
        df.join(order_df, on="cell_id", how="left")
        .with_columns(pl.col("_cell_order").fill_null(-1))
        .sort(["_cell_order", "vertex_idx"])
        .drop("_cell_order")
    )

    counts_df = df.group_by("cell_id", maintain_order=True).len()
    offsets = _compute_offsets(df_cells, counts_df)

    dtype_overrides: dict[str, Any] = {
        "cell_id": np.int32,
        "vertex_idx": np.int32,
        "phi": np.float32,
        "theta": np.float32,
        "x": np.float32,
        "y": np.float32,
        "z": np.float32,
    }

    data_vars = {}
    for col, dtype in dtype_overrides.items():
        if col in df.columns:
            data_vars[col] = ("vertex", df[col].to_numpy().astype(dtype))

    coords = {"vertex": ("vertex", np.arange(df.height, dtype=np.int32))}
    ds = xr.Dataset(data_vars=data_vars, coords=coords)
    ds["offsets"] = ("offset", offsets.astype(np.int32))
    return ds


def _build_neighbors_dataset(
    df_cells: pl.DataFrame, df_neighbors: pl.DataFrame | None
) -> xr.Dataset | None:
    """Convert neighbor dataframe to dataset with ragged representation."""
    if df_neighbors is None or df_neighbors.is_empty():
        return None

    df = df_neighbors.sort(["cell_id", "neighbor_id"])
    counts_df = df.group_by("cell_id", maintain_order=True).len()
    offsets = _compute_offsets(df_cells, counts_df)

    data_vars = {
        "cell_id": ("neighbor", df["cell_id"].to_numpy().astype(np.int32)),
        "neighbor_id": ("neighbor", df["neighbor_id"].to_numpy().astype(np.int32)),
    }

    coords = {"neighbor": ("neighbor", np.arange(df.height, dtype=np.int32))}
    ds = xr.Dataset(data_vars=data_vars, coords=coords)
    ds["offsets"] = ("offset", offsets.astype(np.int32))
    return ds


def write_grid_to_icechunk(
    session: Any,
    grid_name: str,
    df_cells: pl.DataFrame,
    df_vertices: pl.DataFrame | None = None,
    df_neighbors: pl.DataFrame | None = None,
    metadata: dict[str, Any] | None = None,
    specific_metadata: dict[str, Any] | None = None,
    overwrite: bool = False,
) -> str:
    """Write a grid definition to an Icechunk repository.

    Parameters
    ----------
    session : Any
        Icechunk session or store handle.
    grid_name : str
        Grid identifier.
    df_cells : pl.DataFrame
        Cell metadata table.
    df_vertices : pl.DataFrame | None, optional
        Vertex metadata table.
    df_neighbors : pl.DataFrame | None, optional
        Neighbor metadata table.
    metadata : dict[str, Any] | None, optional
        Grid metadata values.
    specific_metadata : dict[str, Any] | None, optional
        Grid-type-specific metadata values.
    overwrite : bool, default False
        Whether to overwrite existing grid data.

    Returns
    -------
    str
        Grid hash.
    """
    if to_icechunk is None:  # pragma: no cover - handled at runtime
        raise ImportError(
            "icechunk is required to write grids. "
            "Install the `icechunk` package to enable this functionality.",
        )

    logger = get_logger()
    logger.info(f"Writing grid '{grid_name}' to icechunk...")

    store = getattr(session, "store", session)
    zroot = zarr.open_group(store, mode="a")
    grids_group = zroot.require_group("grids")

    if grid_name in grids_group:
        if not overwrite:
            raise ValueError(
                f"Grid 'grids/{grid_name}' already exists. "
                "Use overwrite=True to replace."
            )
        del grids_group[grid_name]

    grid_path = f"grids/{grid_name}"

    metadata = metadata.copy() if metadata else {}
    grid_hash = compute_grid_hash(
        df_cells["phi"].to_numpy(),
        df_cells["theta"].to_numpy(),
        metadata["grid_type"],
        metadata["angular_resolution"],
    )
    metadata["grid_hash"] = grid_hash

    grid_meta = GridMetadata(**metadata)
    cells_ds = _build_cells_dataset(df_cells)

    to_icechunk(cells_ds, session, group=f"{grid_path}/cells", mode="w")

    vertices_ds = _build_vertices_dataset(df_cells, df_vertices)
    if vertices_ds is not None:
        to_icechunk(vertices_ds, session, group=f"{grid_path}/vertices", mode="w")

    neighbors_ds = _build_neighbors_dataset(df_cells, df_neighbors)
    if neighbors_ds is not None:
        to_icechunk(neighbors_ds, session, group=f"{grid_path}/neighbors", mode="w")

    meta_ds = xr.Dataset()
    meta_ds.attrs["grid_metadata"] = json.dumps(grid_meta.model_dump(exclude_none=True))

    if specific_metadata:
        spec_meta = GridSpecificMetadata(**specific_metadata)
        meta_ds.attrs["grid_specific_metadata"] = json.dumps(
            spec_meta.model_dump(exclude_none=True)
        )

    meta_ds.attrs["format"] = "gnssvodpy.grid.v1"
    to_icechunk(meta_ds, session, group=f"{grid_path}/metadata", mode="w")

    logger.info(f"✓ Grid '{grid_name}' written successfully via Icechunk")
    logger.info(f"  Hash: {grid_hash[:16]}...")

    return grid_hash


def load_grid_from_icechunk(
    session: Any,
    grid_name: str,
    load_vertices: bool = True,
    load_neighbors: bool = True,
) -> LoadedGrid:
    """Load a grid definition from an Icechunk repository.

    Parameters
    ----------
    session : Any
        Icechunk session or store handle.
    grid_name : str
        Grid identifier.
    load_vertices : bool, default True
        Whether to load vertex data.
    load_neighbors : bool, default True
        Whether to load neighbor data.

    Returns
    -------
    LoadedGrid
        Loaded grid container.
    """
    logger = get_logger()
    logger.info(f"Loading grid '{grid_name}' from icechunk...")

    store = getattr(session, "store", session)
    grid_path = f"grids/{grid_name}"

    try:
        meta_ds = xr.open_zarr(
            store=store,
            group=f"{grid_path}/metadata",
            consolidated=False,
        )
    except Exception as exc:
        raise KeyError(f"Grid '{grid_name}' not found in store") from exc

    metadata_json = meta_ds.attrs.get("grid_metadata")
    if metadata_json is None:
        raise ValueError(
            f"Grid '{grid_name}' metadata missing 'grid_metadata' attribute"
        )

    metadata = GridMetadata(**json.loads(metadata_json))

    specific_metadata = {}
    specific_json = meta_ds.attrs.get("grid_specific_metadata")
    if specific_json:
        specific_metadata = json.loads(specific_json)

    cells_ds = xr.open_zarr(
        store=store,
        group=f"{grid_path}/cells",
        consolidated=False,
    )
    cell_data = {name: cells_ds[name].values for name in cells_ds.data_vars}
    df_cells = pl.DataFrame(cell_data)
    logger.debug(f"  ✓ Loaded {len(df_cells)} cells")

    df_vertices = None
    if load_vertices:
        try:
            vertices_ds = xr.open_zarr(
                store=store, group=f"{grid_path}/vertices", consolidated=False
            )
            vertex_data = {
                name: vertices_ds[name].values
                for name in vertices_ds.data_vars
                if name != "offsets"
            }
            if vertex_data:
                df_vertices = pl.DataFrame(vertex_data)
                if "vertex_idx" not in df_vertices.columns:
                    df_vertices = df_vertices.sort("cell_id").with_columns(
                        pl.col("cell_id").cumcount().over("cell_id").alias("vertex_idx")
                    )
            vertex_count = 0 if df_vertices is None else df_vertices.height
            logger.debug(f"  ✓ Loaded {vertex_count} vertices")
        except (KeyError, FileNotFoundError):
            df_vertices = None

    df_neighbors = None
    if load_neighbors:
        try:
            neighbors_ds = xr.open_zarr(
                store=store, group=f"{grid_path}/neighbors", consolidated=False
            )
            neighbor_data = {
                name: neighbors_ds[name].values
                for name in neighbors_ds.data_vars
                if name != "offsets"
            }
            if neighbor_data:
                df_neighbors = pl.DataFrame(neighbor_data)
            neighbor_count = 0 if df_neighbors is None else df_neighbors.height
            logger.debug(f"  ✓ Loaded {neighbor_count} neighbor relationships")
        except (KeyError, FileNotFoundError):
            df_neighbors = None

    # Reconstruct grid-type specific columns that rely on nested data
    if metadata.grid_type == "htm" and df_vertices is not None:
        vertex_map: dict[int, list[list[float]]] = {}

        for row in df_vertices.sort(["cell_id", "vertex_idx"]).iter_rows(named=True):
            cell_id = int(row["cell_id"])
            # Convert to floats; fallback to NaN if missing
            vertex = [
                float(row.get("x", float("nan"))),
                float(row.get("y", float("nan"))),
                float(row.get("z", float("nan"))),
            ]
            vertex_map.setdefault(cell_id, [])
            if len(vertex_map[cell_id]) < 3:
                vertex_map[cell_id].append(vertex)

        default_vertex = [float("nan"), float("nan"), float("nan")]
        vertex_columns = {0: [], 1: [], 2: []}

        for cell_id in df_cells["cell_id"]:
            vertices = vertex_map.get(int(cell_id), [])
            for idx in range(3):
                if idx < len(vertices):
                    vertex_columns[idx].append(vertices[idx])
                else:
                    vertex_columns[idx].append(default_vertex)

        df_cells = df_cells.with_columns(
            [
                pl.Series("htm_vertex_0", vertex_columns[0]),
                pl.Series("htm_vertex_1", vertex_columns[1]),
                pl.Series("htm_vertex_2", vertex_columns[2]),
            ]
        )

    logger.info(f"✓ Grid '{grid_name}' loaded successfully")

    return LoadedGrid(
        grid_name=grid_name,
        cells=df_cells,
        metadata=metadata,
        vertices=df_vertices,
        neighbors=df_neighbors,
        specific_metadata=specific_metadata,
    )


# ==============================================================================
# UTILITY FUNCTIONS
# ==============================================================================


def list_available_grids(zarr_group: zarr.Group) -> dict[str, dict[str, Any]]:
    """
    List all available grids in zarr store.

    Parameters
    ----------
    zarr_group : zarr.Group
        Parent zarr group

    Returns
    -------
    dict
        Dictionary mapping grid names to their metadata
    """
    if "grids" not in zarr_group:
        return {}

    grids_group = zarr_group["grids"]
    available_grids = {}

    for grid_name in grids_group.group_keys():
        try:
            grid_group = grids_group[grid_name]
            if "metadata" in grid_group:
                metadata_group = grid_group["metadata"]
                metadata_json = metadata_group.attrs.get("grid_metadata")
                if metadata_json:
                    metadata = json.loads(metadata_json)
                else:
                    metadata = dict(metadata_group.attrs)
            else:
                metadata = dict(grid_group.attrs)
            available_grids[grid_name] = metadata
        except Exception as e:
            available_grids[grid_name] = {"error": str(e)}

    return available_grids


def delete_grid(zarr_group: zarr.Group, grid_name: str) -> bool:
    """
    Delete a grid from zarr store.

    Parameters
    ----------
    zarr_group : zarr.Group
        Parent zarr group
    grid_name : str
        Grid identifier

    Returns
    -------
    bool
        True if deleted successfully
    """
    logger = get_logger()

    try:
        if "grids" in zarr_group and grid_name in zarr_group["grids"]:
            del zarr_group["grids"][grid_name]
            logger.info(f"✓ Deleted grid '{grid_name}'")
            return True
        else:
            logger.warning(f"Grid '{grid_name}' not found")
            return False
    except Exception as e:
        logger.error(f"Failed to delete grid '{grid_name}': {e}")
        return False


def validate_grid_integrity(loaded_grid: LoadedGrid) -> dict[str, Any]:
    """
    Validate grid integrity and compute diagnostics.

    Parameters
    ----------
    loaded_grid : LoadedGrid
        Loaded grid to validate

    Returns
    -------
    dict
        Validation results and diagnostics
    """
    logger = get_logger()

    results = {
        "grid_name": loaded_grid.grid_name,
        "valid": True,
        "errors": [],
        "warnings": [],
        "statistics": {},
    }

    try:
        # Check cell count consistency
        expected_ncells = loaded_grid.metadata.ncells
        actual_ncells = len(loaded_grid.cells)

        if expected_ncells != actual_ncells:
            results["errors"].append(
                f"Cell count mismatch: expected {expected_ncells}, got {actual_ncells}"
            )
            results["valid"] = False

        # Check coordinate ranges
        phi = loaded_grid.cells["phi"].to_numpy()
        theta = loaded_grid.cells["theta"].to_numpy()

        if not (np.all(phi >= 0) and np.all(phi <= 2 * np.pi)):
            results["errors"].append("Phi coordinates out of range [0, 2π]")
            results["valid"] = False

        if not (np.all(theta >= 0) and np.all(theta <= np.pi / 2)):
            results["warnings"].append(
                "Theta coordinates outside expected hemisphere range [0, π/2]"
            )

        # Check solid angles
        if "solid_angle" in loaded_grid.cells.columns:
            solid_angles = loaded_grid.cells["solid_angle"].to_numpy()
            if not np.all(solid_angles > 0):
                results["errors"].append("Non-positive solid angles found")
                results["valid"] = False

            # Statistics
            results["statistics"] = {
                "solid_angle_mean": float(solid_angles.mean()),
                "solid_angle_std": float(solid_angles.std()),
                "solid_angle_cv": float(solid_angles.std() / solid_angles.mean() * 100),
                "total_solid_angle": float(solid_angles.sum()),
            }

        # Verify grid hash
        computed_hash = compute_grid_hash(
            phi,
            theta,
            loaded_grid.metadata.grid_type,
            loaded_grid.metadata.angular_resolution,
        )

        if computed_hash != loaded_grid.metadata.grid_hash:
            results["errors"].append("Grid hash mismatch - data may be corrupted")
            results["valid"] = False

        logger.info(
            f"Grid validation: {'✓ PASSED' if results['valid'] else '✗ FAILED'}"
        )

    except Exception as e:
        results["valid"] = False
        results["errors"].append(f"Validation failed: {e!s}")
        logger.error(f"Grid validation error: {e}")

    return results
