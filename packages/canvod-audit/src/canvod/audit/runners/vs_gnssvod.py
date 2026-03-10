"""Tier 3: Compare canvodpy against gnssvod (Humphrey et al.).

gnssvod is the established community tool for GNSS vegetation optical
depth, developed by Vincent Humphrey. It processes RINEX files into
VOD using an independent codebase.

Comparison strategy:

1. Start with a **trimmed RINEX file** (see ``canvod.audit.rinex_trimmer``)
   that has exactly one observation code per band per system. This
   eliminates signal selection ambiguity between the tools.

2. Feed the same trimmed RINEX to both canvodpy and gnssvod.

3. Compare the outputs. Since there is one code per band, canvodpy's
   SID (e.g. "G01|L1|C") maps 1:1 to gnssvod's PRN ("G01") for each
   band — no collapse logic needed.

gnssvod outputs pandas DataFrames with MultiIndex (Epoch, SV) and
columns like S1C, S2W, Azimuth, Elevation, VOD1, VOD2. This runner
handles the conversion to xarray automatically.

Usage::

    from canvod.audit.runners import audit_vs_gnssvod

    result = audit_vs_gnssvod(
        canvodpy_store="/path/to/store",
        gnssvod_dataframe=df,
        group="canopy_01",
    )

    print(result.summary())
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import xarray as xr

from canvod.audit.core import compare_datasets
from canvod.audit.runners.common import AuditResult, load_group, open_store
from canvod.audit.tolerances import Tolerance, ToleranceTier

# Tolerances for cross-implementation comparison
GNSSVOD_TOLERANCES = {
    "SNR": Tolerance(
        atol=0.01,
        rtol=0.0,
        nan_rate_atol=0.05,
        description="SNR should be near-identical — both read the same RINEX values.",
    ),
    "vod": Tolerance(
        atol=0.01,
        rtol=0.01,
        nan_rate_atol=0.05,
        description="VOD retrieval: sub-0.01 differences are below measurement noise.",
    ),
    "phi": Tolerance(
        atol=0.05,
        rtol=0.0,
        nan_rate_atol=0.05,
        description="Elevation angle: coordinate conversion differences between "
        "independent implementations.",
    ),
    "theta": Tolerance(
        atol=0.05,
        rtol=0.0,
        nan_rate_atol=0.05,
        description="Azimuth angle: coordinate conversion differences.",
    ),
}


def gnssvod_df_to_xarray(
    df,
    *,
    epoch_col="Epoch",
    sid_col="SV",
    value_cols=None,
):
    """Convert a gnssvod pandas DataFrame to an xarray Dataset.

    gnssvod outputs DataFrames with MultiIndex (Epoch, SV) and columns
    for each observation type (S1C, S2W, ...) plus Azimuth, Elevation,
    and VOD bands (VOD1, VOD2, ...).

    Parameters
    ----------
    df : pandas.DataFrame
        Output from gnssvod (with or without MultiIndex).
    epoch_col : str
        Column or index level name for the time axis.
    sid_col : str
        Column or index level name for the satellite identifier.
    value_cols : list of str, optional
        Data columns to include. If not given, uses all numeric columns.

    Returns
    -------
    xarray.Dataset
        With dimensions (epoch, sid).
    """
    import pandas as pd

    # Reset MultiIndex if present
    if isinstance(df.index, pd.MultiIndex):
        df = df.reset_index()

    if value_cols is None:
        value_cols = [
            c
            for c in df.columns
            if c not in (epoch_col, sid_col) and np.issubdtype(df[c].dtype, np.number)
        ]

    epochs = sorted(df[epoch_col].unique())
    sids = sorted(df[sid_col].unique())

    data_vars = {}
    for col in value_cols:
        pivoted = df.pivot_table(index=epoch_col, columns=sid_col, values=col)
        pivoted = pivoted.reindex(index=epochs, columns=sids)
        data_vars[col] = (["epoch", "sid"], pivoted.values)

    return xr.Dataset(
        data_vars,
        coords={"epoch": epochs, "sid": [str(s) for s in sids]},
    )


def _canvodpy_to_prn(ds, snr_var="SNR"):
    """Rename canvodpy SIDs from "G01|L1|C" to "G01" for direct comparison.

    Only works correctly when there is one observation code per PRN per
    band (i.e. the input RINEX was trimmed). Raises ValueError if
    duplicate PRNs would result.

    Parameters
    ----------
    ds : xarray.Dataset
        canvodpy dataset with sv, band, code coords along sid.
    snr_var : str
        Name of the SNR variable.

    Returns
    -------
    xarray.Dataset
        With sid values replaced by PRN strings.
    """
    prns = ds.sv.values.tolist()

    # Check for duplicates (would happen if >1 code per PRN per band)
    if len(prns) != len(set(prns)):
        from collections import Counter

        dupes = {k: v for k, v in Counter(prns).items() if v > 1}
        raise ValueError(
            f"Duplicate PRNs after SID→PRN mapping: {dupes}. "
            f"This means the input has multiple codes per satellite per band. "
            f"Use a trimmed RINEX file with one code per band "
            f"(see canvod.audit.rinex_trimmer)."
        )

    return ds.assign_coords(sid=prns)


def audit_vs_gnssvod(
    canvodpy_store,
    gnssvod_dataframe=None,
    gnssvod_file=None,
    *,
    group="canopy_01",
    variables=None,
    epoch_col="Epoch",
    sid_col="SV",
):
    """Compare canvodpy output against gnssvod output.

    Both tools must have been run on the same trimmed RINEX file
    (one observation code per band per system). See
    ``canvod.audit.rinex_trimmer`` for creating trimmed files.

    Provide either ``gnssvod_dataframe`` (a pandas DataFrame) or
    ``gnssvod_file`` (path to a CSV or Parquet file).

    Parameters
    ----------
    canvodpy_store : str or Path
        Path to the canvodpy Icechunk store.
    gnssvod_dataframe : pandas.DataFrame, optional
        gnssvod output as a DataFrame (MultiIndex or flat).
    gnssvod_file : str or Path, optional
        Path to a saved gnssvod output (CSV or Parquet).
    group : str
        Which store group to compare.
    variables : list of str, optional
        Which variables to compare. If not given, compares all shared
        numeric variables.
    epoch_col, sid_col : str
        Column/index names in the gnssvod DataFrame.

    Returns
    -------
    AuditResult
    """
    if gnssvod_dataframe is None and gnssvod_file is None:
        raise ValueError(
            "Provide either gnssvod_dataframe (a pandas DataFrame) "
            "or gnssvod_file (path to CSV/Parquet)."
        )

    # Load gnssvod data
    if gnssvod_dataframe is None:
        gnssvod_file = Path(gnssvod_file)
        print(f"Loading gnssvod data from {gnssvod_file} ...")

        import pandas as pd

        if gnssvod_file.suffix == ".parquet":
            gnssvod_dataframe = pd.read_parquet(gnssvod_file)
        else:
            gnssvod_dataframe = pd.read_csv(gnssvod_file)

    # Convert gnssvod → xarray
    ds_gnssvod = gnssvod_df_to_xarray(
        gnssvod_dataframe,
        epoch_col=epoch_col,
        sid_col=sid_col,
    )
    print(f"gnssvod: {dict(ds_gnssvod.dims)}, variables: {list(ds_gnssvod.data_vars)}")

    # Load canvodpy store
    s = open_store(canvodpy_store)
    print(f"Loading canvodpy group '{group}' ...")
    ds_canvod = load_group(s, group)
    print(
        f"canvodpy: {dict(ds_canvod.dims)}, "
        f"SID examples: {list(ds_canvod.sid.values[:5])}"
    )

    # Rename canvodpy SIDs to PRNs for direct comparison
    ds_canvod_prn = _canvodpy_to_prn(ds_canvod)
    print(f"canvodpy PRNs: {list(ds_canvod_prn.sid.values[:5])}")

    # Compare
    label = f"{group}: canvodpy vs gnssvod (Humphrey et al.)"
    r = compare_datasets(
        ds_canvod_prn,
        ds_gnssvod,
        variables=variables,
        tier=ToleranceTier.SCIENTIFIC,
        tolerance_overrides=GNSSVOD_TOLERANCES,
        label=label,
    )

    result = AuditResult()
    result.results[f"gnssvod_{group}"] = r

    print()
    print(result.summary())
    return result
