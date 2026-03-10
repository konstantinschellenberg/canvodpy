"""Infrastructure: store round-trip audit.

Reads each group from the store, writes to NetCDF, reads back, and
verifies bit-identical results.

Stores used
-----------
canvodpy RINEX : same as ``run_tier0_vs_gnssvodpy.py``

Results (2026-03-10)
--------------------
- canopy_01: PASS
- reference_01_canopy_01: PASS
"""

from __future__ import annotations

from canvod.audit.runners import audit_store_round_trip
from canvod.audit.runners.common import open_store

CANVODPY_RINEX = (
    "/Volumes/ExtremePro/canvod_audit_output"
    "/tier0_rinex_vs_gnssvodpy/Rosalia/canvodpy_RINEX_store"
)

store = open_store(CANVODPY_RINEX)
result = audit_store_round_trip(store)
print(result.summary())
