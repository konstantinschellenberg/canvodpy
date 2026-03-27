"""External intercomparison with independent implementations.

Compare canvodpy outputs against gnssvod (Humphrey et al.) — the
established community GNSS-VOD processing tool.

This module provides the low-level ``compare_vs_gnssvod`` function.
For the full audit workflow (with ``AuditResult``), use
``canvod.audit.runners.vs_gnssvod.audit_vs_gnssvod`` instead.
"""

from __future__ import annotations

from typing import Any

from canvod.audit.core import ComparisonResult, compare_datasets
from canvod.audit.runners.vs_gnssvod import (
    GNSSVOD_TOLERANCES,  # type: ignore[unresolved-import]
    GnssvodAdapter,
    _wrap_aware_azimuth_diff,
    gnssvod_df_to_xarray,
)
from canvod.audit.tolerances import ToleranceTier


def compare_vs_gnssvod(
    ds_canvod: Any,
    ds_or_df_gnssvod: Any,
    *,
    band_filter: str = "L1|C",
    snr_col: str = "S1C",
    vod_col: str | None = "VOD1",
    variables: list[str] | None = None,
    label: str = "canvodpy vs gnssvod",
    gnssvod_epoch_col: str = "Epoch",
    gnssvod_sid_col: str = "SV",
) -> ComparisonResult:
    """Compare canvodpy output against gnssvod output for one band.

    Uses ``GnssvodAdapter`` to project canvodpy into gnssvod variable
    space before comparison, ensuring identical variable names, units,
    and conventions.

    Parameters
    ----------
    ds_canvod : xarray.Dataset
        canvodpy output (epoch, sid) dataset.
    ds_or_df_gnssvod : xarray.Dataset or pandas.DataFrame
        gnssvod output. If a pandas DataFrame, it is automatically
        converted to xarray.
    band_filter : str
        SID band suffix, e.g. ``"L1|C"`` or ``"L2|W"``.
    snr_col : str
        gnssvod SNR column name for this band.
    vod_col : str or None
        gnssvod VOD column name for this band.
    variables : list[str], optional
        Variables to compare. Defaults to all shared variables.
    label : str
        Comparison label.
    gnssvod_epoch_col, gnssvod_sid_col : str
        Column names in gnssvod DataFrame.

    Returns
    -------
    ComparisonResult
        SCIENTIFIC tier — two independent implementations.
    """
    import pandas as pd

    if isinstance(ds_or_df_gnssvod, pd.DataFrame):
        ds_gnssvod = gnssvod_df_to_xarray(
            ds_or_df_gnssvod,
            epoch_col=gnssvod_epoch_col,
            sid_col=gnssvod_sid_col,
        )
    else:
        ds_gnssvod = ds_or_df_gnssvod

    # Project canvodpy into gnssvod space
    adapter = GnssvodAdapter(
        ds_canvod,
        band_filter=band_filter,
        snr_col=snr_col,
        vod_col=vod_col,
    )
    ds_adapted = adapter.to_gnssvod_dataset()

    # Handle azimuth wrap-around
    ds_adapted = _wrap_aware_azimuth_diff(ds_adapted, ds_gnssvod)

    # Build tolerance overrides
    tol_overrides = {}
    if snr_col in ds_adapted.data_vars:
        tol_overrides[snr_col] = GNSSVOD_TOLERANCES["SNR"]
    if "Azimuth" in ds_adapted.data_vars:
        tol_overrides["Azimuth"] = GNSSVOD_TOLERANCES["Azimuth"]
    if "Elevation" in ds_adapted.data_vars:
        tol_overrides["Elevation"] = GNSSVOD_TOLERANCES["Elevation"]
    if vod_col and vod_col in ds_adapted.data_vars:
        tol_overrides[vod_col] = GNSSVOD_TOLERANCES["VOD"]

    return compare_datasets(
        ds_adapted,
        ds_gnssvod,
        variables=variables,
        tier=ToleranceTier.SCIENTIFIC,
        tolerance_overrides=tol_overrides,
        label=label,
        metadata={
            "comparison_type": "external",
            "implementation_a": "canvodpy",
            "implementation_b": "gnssvod",
            "band": band_filter,
            "snr_col": snr_col,
            "vod_col": vod_col,
        },
    )
