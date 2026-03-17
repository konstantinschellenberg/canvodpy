"""Utility functions and configuration for canvodpy."""

from ._meta import __version__

# Submodules are imported on demand to avoid circular imports
# Use: from canvod.utils.config import load_config
# Use: from canvod.utils.tools import YYYYDOY, get_version_from_pyproject

__all__ = [
    "__version__",
    "config",
    "diagnostics",
    "tools",
]
