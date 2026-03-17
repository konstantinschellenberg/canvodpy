"""Internal consistency comparisons within canvodpy.

Compare outputs from different processing paths that should produce
equivalent results: SBF vs RINEX readers, broadcast vs agency ephemeris.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from canvod.audit.core import ComparisonResult, compare_datasets
from canvod.audit.tolerances import ToleranceTier

if TYPE_CHECKING:
    pass


def compare_sbf_vs_rinex(
    store_sbf: Any,
    store_rinex: Any,
    *,
    group: str,
    variables: list[str] | None = None,
) -> ComparisonResult:
    """Compare SBF and RINEX reader outputs from the same observation period.

    Parameters
    ----------
    store_sbf : MyIcechunkStore
        Store populated from SBF files.
    store_rinex : MyIcechunkStore
        Store populated from RINEX files.
    group : str
        Date group key (e.g. "2025001").
    variables : list[str], optional
        Variables to compare. Defaults to intersection.

    Returns
    -------
    ComparisonResult
        SCIENTIFIC tier — expects SNR quantization differences (0.25 dB)
        and potentially different satellite coverage.
    """
    ds_sbf = store_sbf.read_group(group)
    ds_rinex = store_rinex.read_group(group)

    return compare_datasets(
        ds_sbf,
        ds_rinex,
        variables=variables,
        tier=ToleranceTier.SCIENTIFIC,
        label=f"SBF vs RINEX — {group}",
        metadata={
            "comparison_type": "internal",
            "reader_a": "sbf",
            "reader_b": "rinex",
            "group": group,
        },
    )


def compare_ephemeris_sources(
    store_broadcast: Any,
    store_agency: Any,
    *,
    group: str,
    variables: list[str] | None = None,
) -> ComparisonResult:
    """Compare broadcast vs agency (SP3/CLK) ephemeris augmentation.

    Parameters
    ----------
    store_broadcast : MyIcechunkStore
        Store augmented with broadcast ephemeris (from SBF SatVisibility).
    store_agency : MyIcechunkStore
        Store augmented with agency products (SP3 + CLK).
    group : str
        Date group key.

    Returns
    -------
    ComparisonResult
        SCIENTIFIC tier — broadcast ephemeris has lower accuracy than
        precise products (~1-2m vs ~2cm orbit accuracy).
    """
    ds_broadcast = store_broadcast.read_group(group)
    ds_agency = store_agency.read_group(group)

    # Default to satellite coordinate variables
    if variables is None:
        coord_vars = ["sat_x", "sat_y", "sat_z", "phi", "theta"]
        variables = [
            v
            for v in coord_vars
            if v in ds_broadcast.data_vars and v in ds_agency.data_vars
        ]

    return compare_datasets(
        ds_broadcast,
        ds_agency,
        variables=variables,
        tier=ToleranceTier.SCIENTIFIC,
        label=f"Broadcast vs Agency ephemeris — {group}",
        metadata={
            "comparison_type": "internal",
            "ephemeris_a": "broadcast",
            "ephemeris_b": "agency",
            "group": group,
        },
    )
