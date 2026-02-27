"""canvod-vod: VOD calculation for GNSS vegetation analysis.

This package provides VOD calculation algorithms based on the Tau-Omega model.
"""

from canvod.vod.calculator import TauOmegaZerothOrder, VODCalculator

__version__ = "0.1.0"

__all__ = [
    "TauOmegaZerothOrder",
    "VODCalculator",
]
