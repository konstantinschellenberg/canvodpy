"""canvod-audit: Audit, comparison, and regression verification for canvodpy.

Three tiers of verification:

1. **Internal consistency** — different paths through canvodpy produce
   equivalent results (SBF vs RINEX reader, broadcast vs agency ephemeris).

2. **Regression** — outputs are unchanged after code changes, verified
   against frozen reference checkpoints.

3. **External intercomparison** — canvodpy agrees with independent
   implementations (gnssvod by Humphrey et al.).

Quick start::

    from canvod.audit import compare_datasets, ComparisonResult
    from canvod.audit.tolerances import ToleranceTier

    result = compare_datasets(ds_canvod, ds_reference, tier=ToleranceTier.SCIENTIFIC)
    print(result.summary())
    result.to_polars()
"""

from canvod.audit._meta import __version__
from canvod.audit.core import AlignmentInfo, ComparisonResult, compare_datasets
from canvod.audit.stats import VariableStats, compute_variable_stats
from canvod.audit.tolerances import (
    SCIENTIFIC_DEFAULTS,
    TIER_DEFAULTS,
    Tolerance,
    ToleranceTier,
    get_tolerance,
)

__all__ = [
    "SCIENTIFIC_DEFAULTS",
    "TIER_DEFAULTS",
    "AlignmentInfo",
    "ComparisonResult",
    "Tolerance",
    "ToleranceTier",
    "VariableStats",
    "__version__",
    "compare_datasets",
    "compute_variable_stats",
    "get_tolerance",
]
