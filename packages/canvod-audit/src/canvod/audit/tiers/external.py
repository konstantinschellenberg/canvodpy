"""External intercomparison with independent implementations.

Compare canvodpy outputs against gnssvod (Humphrey et al.) — the
established community GNSS-VOD processing tool.
"""

from __future__ import annotations

from typing import Any

import numpy as np

from canvod.audit.core import ComparisonResult, compare_datasets
from canvod.audit.tolerances import ToleranceTier


def _gnssvod_df_to_xarray(
    df: Any,
    *,
    epoch_col: str = "epoch",
    sid_col: str = "sv",
    value_cols: list[str] | None = None,
) -> Any:
    """Convert a gnssvod pandas DataFrame to an xarray Dataset.

    gnssvod outputs pandas DataFrames with columns like epoch, sv, SNR, etc.
    This converts to the (epoch, sid) xarray format used by canvodpy.

    Parameters
    ----------
    df : pandas.DataFrame
        gnssvod output DataFrame.
    epoch_col : str
        Column name for epoch/time.
    sid_col : str
        Column name for satellite identifier.
    value_cols : list[str], optional
        Data columns to include. If None, uses all numeric columns.
    """
    import xarray as xr

    if value_cols is None:
        value_cols = [
            c
            for c in df.columns
            if c not in (epoch_col, sid_col) and np.issubdtype(df[c].dtype, np.number)
        ]

    # Pivot to (epoch, sid) structure
    epochs = sorted(df[epoch_col].unique())
    sids = sorted(df[sid_col].unique())

    data_vars = {}
    for col in value_cols:
        pivoted = df.pivot_table(index=epoch_col, columns=sid_col, values=col)
        # Reindex to ensure consistent shape
        pivoted = pivoted.reindex(index=epochs, columns=sids)
        data_vars[col] = (["epoch", "sid"], pivoted.values)

    return xr.Dataset(
        data_vars,
        coords={"epoch": epochs, "sid": [str(s) for s in sids]},
    )


def compare_vs_gnssvod(
    ds_canvod: Any,
    ds_or_df_gnssvod: Any,
    *,
    variables: list[str] | None = None,
    label: str = "canvodpy vs gnssvod",
    gnssvod_epoch_col: str = "epoch",
    gnssvod_sid_col: str = "sv",
) -> ComparisonResult:
    """Compare canvodpy output against gnssvod output.

    Parameters
    ----------
    ds_canvod : xarray.Dataset
        canvodpy output (epoch, sid) dataset.
    ds_or_df_gnssvod : xarray.Dataset or pandas.DataFrame
        gnssvod output. If a pandas DataFrame, it is automatically converted
        to xarray using ``_gnssvod_df_to_xarray``.
    variables : list[str], optional
        Variables to compare. Defaults to intersection.
    label : str
        Comparison label.
    gnssvod_epoch_col : str
        Column name for epoch in gnssvod DataFrame.
    gnssvod_sid_col : str
        Column name for satellite ID in gnssvod DataFrame.

    Returns
    -------
    ComparisonResult
        SCIENTIFIC tier — two independent implementations may differ in
        coordinate conventions, interpolation methods, etc.
    """
    import pandas as pd

    if isinstance(ds_or_df_gnssvod, pd.DataFrame):
        ds_gnssvod = _gnssvod_df_to_xarray(
            ds_or_df_gnssvod,
            epoch_col=gnssvod_epoch_col,
            sid_col=gnssvod_sid_col,
        )
    else:
        ds_gnssvod = ds_or_df_gnssvod

    return compare_datasets(
        ds_canvod,
        ds_gnssvod,
        variables=variables,
        tier=ToleranceTier.SCIENTIFIC,
        label=label,
        metadata={
            "comparison_type": "external",
            "implementation_a": "canvodpy",
            "implementation_b": "gnssvod",
        },
    )
