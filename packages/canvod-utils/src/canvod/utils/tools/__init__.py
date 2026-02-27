"""
Utility tools for canVODpy packages.

This module provides common utilities used across all canVODpy packages:
- Version management
- Date/time utilities for GNSS data
- Validation helpers
- File hashing

Examples
--------
>>> from canvod.utils.tools import get_version_from_pyproject
>>> version = get_version_from_pyproject()

>>> from canvod.utils.tools import YYYYDOY
>>> date = YYYYDOY.from_str("2025024")
>>> print(date.to_datetime())

>>> from canvod.utils.tools import gpsweekday
>>> week, day = gpsweekday("2025-01-15")

>>> from canvod.utils.tools import isfloat
>>> isfloat("3.14")  # True
"""

from .date_utils import YYDOY, YYYYDOY, get_gps_week_from_filename
from .hashing import rinex_file_hash
from .validation import isfloat
from .version import get_version_from_pyproject

# Backwards compatibility: gpsweekday is now a static method on YYYYDOY
# Provide as standalone function for backwards compatibility
gpsweekday = YYYYDOY.gpsweekday

__all__ = [
    "YYDOY",
    # Date/time
    "YYYYDOY",
    "get_gps_week_from_filename",
    # Version
    "get_version_from_pyproject",
    "gpsweekday",
    # Validation
    "isfloat",
    # Hashing
    "rinex_file_hash",
]
