"""Position and coordinate transformations for GNSS data.

Provides ECEF/geodetic position representations and spherical coordinate
computation for satellite-receiver geometry analysis.
"""

from .position import ECEFPosition, GeodeticPosition
from .spherical_coords import (
    add_broadcast_spherical_coords_to_dataset,
    add_spherical_coords_to_dataset,
    compute_spherical_coordinates,
)

__all__ = [
    "ECEFPosition",
    "GeodeticPosition",
    "add_broadcast_spherical_coords_to_dataset",
    "add_spherical_coords_to_dataset",
    "compute_spherical_coordinates",
]
