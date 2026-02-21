"""Constants for RINEX readers.

This module contains only true physical/technical constants that should
never change. User-configurable settings have been moved to the configuration
system (canvod.utils.config).

Removed and moved to config:
- KEEP_RNX_VARS → processing.keep_rnx_vars
- COMPRESSION → compression.{zlib, complevel}
- TIME_AGGR → processing.time_aggregation_seconds
- AGGREGATE_GLONASS_FDMA → processing.aggregate_glonass_fdma
- AUTHOR, EMAIL, etc. → metadata.{author, email, ...}
- SOFTWARE → canvod.utils._meta.SOFTWARE_ATTRS
"""

import re

import pint

# Initialize unit registry - use application registry to avoid redefinition warnings
# in multiprocessing contexts
UREG: pint.UnitRegistry = pint.get_application_registry()

# Define custom units only if not already defined (idempotent)
# Note: 'dB' (decibel) already exists in pint by default, so we don't redefine it
if "dBHz" not in UREG:
    UREG.define("dBHz = 10 * log10(hertz)")

# Glob patterns for discovering GNSS observation files on disk
# Used by PairDataDirMatcher to detect which date directories contain data.
RINEX_OBS_GLOB_PATTERNS: tuple[str, ...] = (
    "*.[0-9][0-9]o",  # RINEX v2/v3 short-name: .24o, .25o, etc.
    "*.O",            # Uppercase generic RINEX obs suffix
    "*.rnx",          # RINEX v3 long-name format
    "*.[0-9][0-9]_",  # Septentrio SBF binary: .25_, .24_, etc.
)

# Regex pattern for validating a single RINEX observation file suffix
RINEX_OBS_SUFFIX_RE = re.compile(r"\.(\d{2}o|O|rnx)$")

# Physical constants
SPEEDOFLIGHT: pint.Quantity = 299792458 * UREG.meter / UREG.second

# RINEX parsing
EPOCH_RECORD_INDICATOR: str = ">"

# GNSS frequency unit
FREQ_UNIT: pint.Unit = UREG.MHz

# Septentrio receiver sampling intervals (hardware capabilities)
SEPTENTRIO_SAMPLING_INTERVALS: list[pint.Quantity] = [
    100 * UREG.millisecond,
    200 * UREG.millisecond,
    500 * UREG.millisecond,
    1 * UREG.second,
    2 * UREG.second,
    5 * UREG.second,
    10 * UREG.second,
    15 * UREG.second,
    30 * UREG.second,
    60 * UREG.second,
    2 * UREG.minute,
    5 * UREG.minute,
    10 * UREG.minute,
    15 * UREG.minute,
    30 * UREG.minute,
    60 * UREG.minute,
]

# IGS RINEX dump intervals (data availability)
IGS_RNX_DUMP_INTERVALS: list[pint.Quantity] = [
    15 * UREG.minute,
    1 * UREG.hour,
    6 * UREG.hour,
    24 * UREG.hour,
]
