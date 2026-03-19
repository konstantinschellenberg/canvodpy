"""GNSS specifications and core characteristics.

This module contains fundamental GNSS definitions including:
- Constants: Unit registry, physical constants, RINEX parameters
- Exceptions: GNSS-specific error types
- Metadata: CF-compliant metadata for coordinates and observables
- Models: Pydantic validation models for RINEX data structures
- Signals: Signal ID mapping and band properties
- Utils: File hashing, version extraction, data type checks

These components are used across all GNSS reader implementations.
"""

from canvod.readers.gnss_specs.satellite_catalog import SatelliteCatalog

__all__ = ["SatelliteCatalog"]
