"""NMEA format readers.

Supports:
- NMEA v4.00 (GSV satellite-in-view with SNR)
"""

from canvod.readers.nmea.v4_00 import NmeaObs

__all__ = [
    "NmeaObs",
]
