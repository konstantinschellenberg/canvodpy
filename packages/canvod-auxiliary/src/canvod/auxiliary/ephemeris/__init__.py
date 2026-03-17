"""Ephemeris (satellite orbit) data handling.

This module provides tools for reading, parsing, and validating satellite
ephemeris data from SP3 format files, as well as the EphemerisProvider ABC
for augmenting GNSS datasets with angular coordinates.
"""

from canvod.auxiliary.ephemeris.parser import Sp3Parser
from canvod.auxiliary.ephemeris.provider import (
    AgencyEphemerisProvider,
    EphemerisProvider,
    SbfBroadcastProvider,
)
from canvod.auxiliary.ephemeris.reader import Sp3File
from canvod.auxiliary.ephemeris.validator import Sp3Validator

__all__ = [
    "AgencyEphemerisProvider",
    "EphemerisProvider",
    "SbfBroadcastProvider",
    "Sp3File",
    "Sp3Parser",
    "Sp3Validator",
]
