"""Tier 3: Compare canvodpy against gnssvod (Humphrey et al.).

gnssvod is the established community tool for GNSS vegetation optical
depth, developed by Vincent Humphrey. It processes RINEX files into
VOD using an independent codebase.

Comparison strategy:

1. Start with a **trimmed RINEX file** (see ``canvod.audit.rinex_trimmer``)
   that has exactly one observation code per band per system. This
   eliminates signal selection ambiguity between the tools.

2. Feed the same trimmed RINEX to both canvodpy and gnssvod.

3. Use ``GnssvodAdapter`` to project canvodpy's (epoch, sid) dataset into
   gnssvod's variable space — same variable names, same units, same
   conventions. Then compare two identically-shaped datasets.

Coordinate conventions
----------------------
canvodpy:
    theta = polar angle from Up, [0, π/2] rad (0=zenith, π/2=horizon)
    phi   = azimuth from North clockwise, [0, 2π) rad

gnssvod:
    Elevation = angle from horizon, [0, 90] degrees (0=horizon, 90=zenith)
    Azimuth   = angle from North clockwise, [0, 360) degrees

Mapping:
    Elevation = 90 - degrees(theta)
    Azimuth   = degrees(phi) mod 360

Usage::

    from canvod.audit.runners import audit_vs_gnssvod

    result = audit_vs_gnssvod(
        canvodpy_store="/path/to/store",
        gnssvod_file="/path/to/gnssvod_output.parquet",
        group="canopy_01",
    )

    print(result.summary())
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import xarray as xr

from canvod.audit.core import ComparisonResult, compare_datasets
from canvod.audit.runners.common import AuditResult, load_group, open_store
from canvod.audit.tolerances import Tolerance, ToleranceTier

# ---------------------------------------------------------------------------
# Tolerances — justified per-variable for cross-implementation comparison
# ---------------------------------------------------------------------------

GNSSVOD_TOLERANCES = {
    # Both tools read the same trimmed RINEX → SNR must agree.
    # canvodpy stores SNR as float32 (~7 sig digits), gnssvod uses float64.
    # RINEX SNR precision is ~0.001 dB (3 decimal places); float32
    # truncation introduces max ~2e-6 dB error — 1000x below measurement
    # resolution. Not a bug: float32 is deliberate (halves memory for
    # large (epoch, sid) arrays).
    "SNR": Tolerance(
        atol=1e-5,
        mae_atol=0.0,
        nan_rate_atol=0.05,
        description="SNR from same RINEX: differs only by float32 vs float64 "
        "dtype truncation (~1e-6 dB). RINEX precision is ~0.001 dB, so "
        "float32 error is 1000x below measurement resolution.",
    ),
    # VOD = -ln(T) * cos(theta). Both tools use the same SNR (identical
    # RINEX, same formula), so T is identical. VOD differs only because
    # theta differs — canvodpy uses scipy CubicHermiteSpline (piecewise
    # cubic, uses SP3 velocities), gnssvod uses numpy degree-16 polyfit
    # on 4-hour windows (no SP3 velocities). Different interpolation
    # methods → different satellite ECEF → different theta → different
    # cos(theta) → systematic VOD difference.
    "VOD": Tolerance(
        atol=0.05,
        mae_atol=0.01,
        nan_rate_atol=0.10,
        description="VOD differs because theta differs (different SP3 "
        "interpolation methods, not different SP3 files). "
        "0.05 is below typical measurement uncertainty "
        "(~0.1 for forest canopies).",
    ),
    # Azimuth/Elevation: systematic differences from fundamentally
    # different SP3 interpolation algorithms (same SP3 file):
    #   canvodpy: scipy CubicHermiteSpline (uses SP3 positions + velocities)
    #   gnssvod:  numpy degree-16 polyfit on 4h windows (positions only)
    # These are NOT floating-point noise — they are real, reproducible
    # differences between two valid interpolation approaches.
    # Wrap-around at 0°/360° handled by comparison adapter.
    "Azimuth": Tolerance(
        atol=0.5,
        mae_atol=0.01,
        nan_rate_atol=0.15,
        description="Azimuth (degrees): systematic difference from different "
        "SP3 interpolation methods (Hermite cubic vs degree-16 polyfit). "
        "0.5° well within one hemigrid cell (2°). "
        "NaN rate tolerance 15%: gnssvod drops elev <= -10°, "
        "canvodpy retains as NaN.",
    ),
    "Elevation": Tolerance(
        atol=0.5,
        mae_atol=0.01,
        nan_rate_atol=0.15,
        description="Elevation (degrees): same root cause as Azimuth — "
        "different SP3 interpolation methods. "
        "NaN rate tolerance 15% for gnssvod elevation cutoff difference.",
    ),
}


# ---------------------------------------------------------------------------
# Adapter: project canvodpy → gnssvod variable space
# ---------------------------------------------------------------------------


class GnssvodAdapter:
    """Project a canvodpy (epoch, sid) dataset into gnssvod variable space.

    Transforms canvodpy's SID-indexed data into PRN-indexed data with
    gnssvod-compatible variable names and units. One adapter instance
    handles one frequency band.

    Parameters
    ----------
    ds : xarray.Dataset
        canvodpy dataset with (epoch, sid) dims and variables: SNR, phi,
        theta, and optionally VOD.
    band_filter : str
        SID band suffix to select, e.g. ``"L1|C"`` or ``"L2|W"``.
    snr_col : str
        gnssvod column name for SNR at this band (e.g. ``"S1C"``).
    vod_col : str or None
        gnssvod column name for VOD at this band (e.g. ``"VOD1"``).
    """

    def __init__(
        self,
        ds: xr.Dataset,
        band_filter: str,
        snr_col: str,
        vod_col: str | None = None,
    ):
        # Select SIDs matching this band
        all_sids = [str(s) for s in ds.sid.values]
        band_sids = [s for s in all_sids if s.endswith(f"|{band_filter}")]
        if not band_sids:
            raise ValueError(
                f"No SIDs match band filter '|{band_filter}'. "
                f"Available: {all_sids[:10]}"
            )

        self.ds = ds.sel(sid=band_sids)
        self.band_filter = band_filter
        self.snr_col = snr_col
        self.vod_col = vod_col

        # Map SIDs to PRNs
        self.prns = [s.split("|")[0] for s in band_sids]
        if len(self.prns) != len(set(self.prns)):
            from collections import Counter

            dupes = {k: v for k, v in Counter(self.prns).items() if v > 1}
            raise ValueError(
                f"Duplicate PRNs after SID→PRN mapping for band {band_filter}: "
                f"{dupes}. Input RINEX was not trimmed to one code per band. "
                f"See canvod.audit.rinex_trimmer."
            )

    def to_gnssvod_dataset(self) -> xr.Dataset:
        """Convert to a gnssvod-shaped dataset with PRN sids.

        Returns dataset with variables named like gnssvod output:
        - ``{snr_col}`` (e.g. S1C): SNR in dB-Hz (unchanged)
        - ``Azimuth``: degrees from North, clockwise [0, 360)
        - ``Elevation``: degrees from horizon [0, 90]
        - ``{vod_col}`` (e.g. VOD1): VOD (unchanged) if present

        Coordinates: epoch (datetime), sid (PRN strings like "G01").
        """
        data_vars = {}

        # SNR: same units (dB-Hz), just rename
        if "SNR" in self.ds.data_vars:
            data_vars[self.snr_col] = (["epoch", "sid"], self.ds["SNR"].values)

        # Azimuth: phi (rad, from North CW) → degrees
        if "phi" in self.ds.data_vars:
            az_deg = np.degrees(self.ds["phi"].values) % 360.0
            data_vars["Azimuth"] = (["epoch", "sid"], az_deg)

        # Elevation: theta (rad, polar angle) → 90 - degrees(theta)
        if "theta" in self.ds.data_vars:
            el_deg = 90.0 - np.degrees(self.ds["theta"].values)
            data_vars["Elevation"] = (["epoch", "sid"], el_deg)

        # VOD: same units, just rename
        if self.vod_col and "VOD" in self.ds.data_vars:
            data_vars[self.vod_col] = (["epoch", "sid"], self.ds["VOD"].values)

        return xr.Dataset(
            data_vars,
            coords={
                "epoch": self.ds.epoch.values,
                "sid": self.prns,
            },
        )


# ---------------------------------------------------------------------------
# gnssvod fillna merge replication
# ---------------------------------------------------------------------------


def gnssvod_merge_codes(
    ds: xr.Dataset,
    band_num: str,
    snr_var: str = "SNR",
) -> xr.Dataset:
    """Replicate gnssvod's fillna merge for a given band.

    gnssvod merges multiple tracking codes per band using fillna in
    lexicographic order (determined by ``numpy.intersect1d`` sorting).
    For example, with S1C and S1W both present:
    - S1C values used where available
    - S1W fills remaining NaN gaps

    .. note::

        In gnssvod (``vod_calc.py``), the fillna merge operates on
        **per-code VOD values**, not raw SNR. Each code's VOD is
        computed independently first::

            VOD_code = -ln(10^((grn - ref) / 10)) * cos(90 - elev)

        Then ``band_VOD.fillna(code_VOD)`` cascades in lex order.
        This function merges raw variable values (SNR, phi, theta)
        instead, which is equivalent for SNR (a direct observable)
        but **not** equivalent for VOD. For correct VOD merging,
        compute VOD per code first, then call this function on the
        VOD variable.

    Parameters
    ----------
    ds : xarray.Dataset
        canvodpy dataset with (epoch, sid) dims.
    band_num : str
        Band number to merge, e.g. ``"1"`` for L1, ``"2"`` for L2.
    snr_var : str
        SNR variable name in the dataset.

    Returns
    -------
    xarray.Dataset
        Dataset with one SID per PRN for this band (merged), sid
        values replaced with PRN strings.
    """
    # Find all SIDs for this band: e.g. "G01|L1|C", "G01|L1|W"
    band_prefix = f"|L{band_num}|"
    all_sids = [str(s) for s in ds.sid.values]
    band_sids = [s for s in all_sids if band_prefix in s]
    if not band_sids:
        raise ValueError(f"No SIDs match band L{band_num}")

    # Group SIDs by PRN, sort codes lexicographically (matching gnssvod)
    from collections import defaultdict

    prn_groups: dict[str, list[str]] = defaultdict(list)
    for sid in band_sids:
        prn = sid.split("|")[0]
        prn_groups[prn].append(sid)
    for prn in prn_groups:
        prn_groups[prn].sort()  # lexicographic = gnssvod order

    # Merge: for each PRN, fillna across codes in sorted order
    prns = sorted(prn_groups.keys())
    merged_data = {}

    for var in ds.data_vars:
        merged_arrays = []
        for prn in prns:
            sids_for_prn = prn_groups[prn]
            # Start with NaN, fillna in lex order
            merged = np.full(len(ds.epoch), np.nan)
            for sid in sids_for_prn:
                vals = ds[var].sel(sid=sid).values.astype(np.float64)
                nan_mask = np.isnan(merged)
                merged[nan_mask] = vals[nan_mask]
            merged_arrays.append(merged)
        merged_data[var] = (["epoch", "sid"], np.column_stack(merged_arrays))

    return xr.Dataset(
        merged_data,
        coords={"epoch": ds.epoch.values, "sid": prns},
    )


# ---------------------------------------------------------------------------
# Band configuration
# ---------------------------------------------------------------------------


def detect_band_map(
    ds_canvod: xr.Dataset,
    ds_gnssvod: xr.Dataset,
) -> list[tuple[str, str, str | None]]:
    """Auto-detect band mapping from available variables.

    Scans canvodpy SIDs and gnssvod columns to find matching bands.
    Returns list of (canvodpy_band_suffix, gnssvod_snr_col, gnssvod_vod_col).
    """
    # Detect bands in canvodpy SIDs
    all_sids = [str(s) for s in ds_canvod.sid.values]
    canvod_bands: dict[str, set[str]] = {}  # band_num → set of tracking codes
    for sid in all_sids:
        parts = sid.split("|")
        if len(parts) == 3:
            band = parts[1]  # e.g. "L1"
            code = parts[2]  # e.g. "C"
            band_num = band[1:]  # e.g. "1"
            canvod_bands.setdefault(band_num, set()).add(code)

    # Match against gnssvod SNR columns (S1C, S2W, etc.)
    gnssvod_snr_cols = [
        c for c in ds_gnssvod.data_vars if c.startswith("S") and len(c) == 3
    ]

    # VOD column mapping: band 1 → VOD1, band 2 → VOD2, etc.
    gnssvod_vod_cols = {c[-1]: c for c in ds_gnssvod.data_vars if c.startswith("VOD")}

    band_map = []
    for band_num, codes in sorted(canvod_bands.items()):
        # Find which gnssvod SNR columns exist for this band
        matching_snr = [c for c in gnssvod_snr_cols if c[1] == band_num]
        if not matching_snr:
            continue

        # Match canvodpy code to the gnssvod SNR column's tracking code
        # e.g. gnssvod has S1C → code "C", so pick canvodpy code "C"
        snr_col = None
        primary_code = None
        for mc in matching_snr:
            gnssvod_code = mc[2]  # e.g. "C" from "S1C"
            if gnssvod_code in codes:
                snr_col = mc
                primary_code = gnssvod_code
                break
        if snr_col is None:
            # Fallback: lex-first code with first matching gnssvod column
            primary_code = sorted(codes)[0]
            snr_col = matching_snr[0]

        vod_col = gnssvod_vod_cols.get(band_num)
        band_suffix = f"L{band_num}|{primary_code}"

        band_map.append((band_suffix, snr_col, vod_col))

    return band_map


# Default band map — used when auto-detection is not possible
BAND_MAP = [
    ("L1|C", "S1C", "VOD1"),
    ("L2|W", "S2W", "VOD2"),
    ("L5|Q", "S5Q", None),  # L5 VOD not standard in gnssvod
]


# ---------------------------------------------------------------------------
# gnssvod DataFrame → xarray conversion
# ---------------------------------------------------------------------------


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
        # Use pivot (not pivot_table) to raise on duplicates instead of
        # silently averaging them
        try:
            pivoted = df.pivot(index=epoch_col, columns=sid_col, values=col)
        except ValueError:
            # Fallback if there are true duplicates (shouldn't happen with
            # trimmed RINEX, but be defensive)
            pivoted = df.pivot_table(index=epoch_col, columns=sid_col, values=col)
        pivoted = pivoted.reindex(index=epochs, columns=sids)
        data_vars[col] = (["epoch", "sid"], pivoted.values)

    return xr.Dataset(
        data_vars,
        coords={"epoch": epochs, "sid": [str(s) for s in sids]},
    )


# ---------------------------------------------------------------------------
# Azimuth wrap-around handling
# ---------------------------------------------------------------------------


def _wrap_aware_azimuth_diff(ds_a, ds_b, var="Azimuth"):
    """Replace Azimuth in ds_a with wrap-corrected values relative to ds_b.

    Azimuth differences near 0°/360° boundary can produce spurious ~360°
    diffs. This adjusts ds_a's Azimuth so that (ds_a - ds_b) gives the
    shortest angular distance, enabling standard abs-diff statistics.

    Returns a copy of ds_a with corrected Azimuth.
    """
    if var not in ds_a.data_vars or var not in ds_b.data_vars:
        return ds_a

    a = ds_a[var].values.copy()
    b = ds_b[var].values

    diff = a - b
    a[diff > 180] -= 360
    a[diff < -180] += 360

    ds_a = ds_a.copy()
    ds_a[var] = (ds_a[var].dims, a)
    return ds_a


# ---------------------------------------------------------------------------
# Main audit function
# ---------------------------------------------------------------------------


def _load_canvodpy(canvodpy_store, canvodpy_ds, group):
    """Load canvodpy data from store or return pre-loaded dataset."""
    if canvodpy_ds is not None:
        return canvodpy_ds

    if canvodpy_store is None:
        raise ValueError("Provide either canvodpy_store or canvodpy_ds.")

    store_path = Path(canvodpy_store)
    if (
        (store_path / ".zgroup").exists()
        or (store_path / ".zmetadata").exists()
        or (store_path / "zarr.json").exists()
    ):
        ds = xr.open_zarr(str(store_path))
        print(f"canvodpy (Zarr): {dict(ds.sizes)}")
    else:
        s = open_store(canvodpy_store)
        print(f"Loading canvodpy group '{group}' ...")
        ds = load_group(s, group)
        print(f"canvodpy: {dict(ds.sizes)}")
    return ds


def _load_gnssvod(gnssvod_dataframe, gnssvod_file, epoch_col, sid_col):
    """Load gnssvod data and convert to xarray."""
    if gnssvod_dataframe is None and gnssvod_file is None:
        raise ValueError(
            "Provide either gnssvod_dataframe (a pandas DataFrame) "
            "or gnssvod_file (path to CSV/Parquet)."
        )

    if gnssvod_dataframe is None:
        gnssvod_file = Path(gnssvod_file)
        print(f"Loading gnssvod data from {gnssvod_file} ...")
        import pandas as pd

        if gnssvod_file.suffix == ".parquet":
            gnssvod_dataframe = pd.read_parquet(gnssvod_file)
        else:
            gnssvod_dataframe = pd.read_csv(gnssvod_file)

    return gnssvod_df_to_xarray(
        gnssvod_dataframe,
        epoch_col=epoch_col,
        sid_col=sid_col,
    )


def _compare_band(
    ds_canvod_band: xr.Dataset,
    ds_gnssvod: xr.Dataset,
    band_suffix: str,
    snr_col: str,
    vod_col: str | None,
) -> ComparisonResult | None:
    """Compare one band. Returns ComparisonResult or None if skipped."""
    # Determine which variables to compare
    compare_vars = []
    if snr_col in ds_canvod_band.data_vars and snr_col in ds_gnssvod.data_vars:
        compare_vars.append(snr_col)
    if "Azimuth" in ds_canvod_band.data_vars and "Azimuth" in ds_gnssvod.data_vars:
        compare_vars.append("Azimuth")
    if "Elevation" in ds_canvod_band.data_vars and "Elevation" in ds_gnssvod.data_vars:
        compare_vars.append("Elevation")
    if (
        vod_col
        and vod_col in ds_canvod_band.data_vars
        and vod_col in ds_gnssvod.data_vars
    ):
        compare_vars.append(vod_col)

    if not compare_vars:
        print("  No shared variables to compare")
        return None

    # Align on shared SIDs and epochs before comparison
    shared_sids = np.intersect1d(ds_canvod_band.sid.values, ds_gnssvod.sid.values)
    shared_epochs = np.intersect1d(ds_canvod_band.epoch.values, ds_gnssvod.epoch.values)
    if len(shared_sids) == 0 or len(shared_epochs) == 0:
        print(
            f"  No shared sids/epochs (canvod: {len(ds_canvod_band.sid)}, gnssvod: {len(ds_gnssvod.sid)})"
        )
        return None
    ds_canvod_band = ds_canvod_band.sel(sid=shared_sids, epoch=shared_epochs)
    ds_gnssvod = ds_gnssvod.sel(sid=shared_sids, epoch=shared_epochs)

    # Handle azimuth wrap-around
    ds_canvod_band = _wrap_aware_azimuth_diff(ds_canvod_band, ds_gnssvod)

    # Build tolerance overrides
    tol_overrides = {}
    if snr_col in compare_vars:
        tol_overrides[snr_col] = GNSSVOD_TOLERANCES["SNR"]
    if "Azimuth" in compare_vars:
        tol_overrides["Azimuth"] = GNSSVOD_TOLERANCES["Azimuth"]
    if "Elevation" in compare_vars:
        tol_overrides["Elevation"] = GNSSVOD_TOLERANCES["Elevation"]
    if vod_col and vod_col in compare_vars:
        tol_overrides[vod_col] = GNSSVOD_TOLERANCES["VOD"]

    label = f"canvodpy vs gnssvod: {band_suffix} ({snr_col})"
    return compare_datasets(
        ds_canvod_band,
        ds_gnssvod,
        variables=compare_vars,
        tier=ToleranceTier.SCIENTIFIC,
        tolerance_overrides=tol_overrides,
        label=label,
        metadata={
            "comparison_type": "external",
            "band": band_suffix,
            "snr_col": snr_col,
            "vod_col": vod_col,
        },
    )


def audit_vs_gnssvod(
    canvodpy_store=None,
    canvodpy_ds=None,
    gnssvod_dataframe=None,
    gnssvod_file=None,
    *,
    group="canopy_01",
    band_map=None,
    mode="trimmed",
    epoch_col="Epoch",
    sid_col="SV",
) -> AuditResult:
    """Compare canvodpy output against gnssvod output.

    Two comparison modes:

    ``mode="trimmed"`` (default):
        Both tools ran on the same trimmed RINEX (one code per band).
        canvodpy SIDs map 1:1 to gnssvod PRNs via ``GnssvodAdapter``.
        SNR should be bit-identical.

    ``mode="merged"``:
        Both tools ran on the same untrimmed RINEX (multiple codes per
        band). canvodpy's per-SID values are merged using gnssvod's
        fillna logic (lexicographic priority via numpy.intersect1d) to
        produce one merged value per PRN. **Important**: gnssvod
        computes VOD per-code *before* merging (see ``vod_calc.py``),
        so raw-variable merging is only equivalent for direct
        observables (SNR). For VOD comparison in merged mode, compute
        VOD per code first, then merge the VOD values.

    Provide either ``canvodpy_store`` or ``canvodpy_ds`` for canvodpy data,
    and either ``gnssvod_dataframe`` or ``gnssvod_file`` for gnssvod data.

    Parameters
    ----------
    canvodpy_store : str, Path, or MyIcechunkStore, optional
        Path to the canvodpy store (Icechunk or plain Zarr).
    canvodpy_ds : xarray.Dataset, optional
        Pre-loaded canvodpy dataset (alternative to store).
    gnssvod_dataframe : pandas.DataFrame, optional
        gnssvod output as a DataFrame (MultiIndex or flat).
    gnssvod_file : str or Path, optional
        Path to a saved gnssvod output (CSV or Parquet).
    group : str
        Which store group to compare (ignored if canvodpy_ds given).
    band_map : list of tuples, optional
        Override band mapping. Each tuple:
        ``(canvodpy_band_suffix, gnssvod_snr_col, gnssvod_vod_col)``.
        If None, auto-detected from the data.
    mode : str
        ``"trimmed"`` (1:1 SID→PRN) or ``"merged"`` (replicate fillna).
    epoch_col, sid_col : str
        Column/index names in the gnssvod DataFrame.

    Returns
    -------
    AuditResult
    """
    if mode not in ("trimmed", "merged"):
        raise ValueError(f"mode must be 'trimmed' or 'merged', got '{mode}'")

    # ── Load data ─────────────────────────────────────────────────────
    canvodpy_ds = _load_canvodpy(canvodpy_store, canvodpy_ds, group)
    print(f"  SID examples: {list(canvodpy_ds.sid.values[:5])}")
    print(f"  Variables: {sorted(canvodpy_ds.data_vars)}")

    ds_gnssvod = _load_gnssvod(gnssvod_dataframe, gnssvod_file, epoch_col, sid_col)
    print(
        f"gnssvod: {dict(ds_gnssvod.sizes)}, variables: {sorted(ds_gnssvod.data_vars)}"
    )

    # ── Auto-detect band map if needed ────────────────────────────────
    if band_map is None:
        band_map = detect_band_map(canvodpy_ds, ds_gnssvod)
        if band_map:
            print(f"\n  Auto-detected bands: {[(b, s) for b, s, _ in band_map]}")
        else:
            print("  WARNING: Could not auto-detect bands, falling back to defaults")
            band_map = BAND_MAP

    print(f"  Mode: {mode}")

    # ── Compare per band ──────────────────────────────────────────────
    result = AuditResult()

    for band_suffix, snr_col, vod_col in band_map:
        if snr_col not in ds_gnssvod.data_vars:
            print(f"\n  Skipping {band_suffix}: {snr_col} not in gnssvod output")
            continue

        print(f"\n{'─' * 60}")
        print(f"Band: {band_suffix} → {snr_col} (mode={mode})")

        if mode == "trimmed":
            # One code per band: direct SID→PRN mapping
            matching_sids = [
                s for s in canvodpy_ds.sid.values if str(s).endswith(f"|{band_suffix}")
            ]
            if not matching_sids:
                print(f"  Skipping: no matching SIDs for |{band_suffix}")
                continue

            print(f"  canvodpy SIDs: {len(matching_sids)}")
            try:
                adapter = GnssvodAdapter(
                    canvodpy_ds,
                    band_filter=band_suffix,
                    snr_col=snr_col,
                    vod_col=vod_col,
                )
            except ValueError as e:
                print(f"  ERROR: {e}")
                continue

            ds_adapted = adapter.to_gnssvod_dataset()

        else:
            # Merged mode: replicate gnssvod's fillna across codes
            band_num = band_suffix.split("|")[0].replace("L", "")  # "L1|C" → "1"
            try:
                ds_merged = gnssvod_merge_codes(canvodpy_ds, band_num)
            except ValueError as e:
                print(f"  Skipping: {e}")
                continue

            # Convert merged dataset to gnssvod variable space
            # ds_merged already has PRN sids; rename variables
            data_vars = {}
            if "SNR" in ds_merged.data_vars:
                data_vars[snr_col] = ds_merged["SNR"]
            if "phi" in ds_merged.data_vars:
                data_vars["Azimuth"] = (
                    ds_merged["phi"].dims,
                    np.degrees(ds_merged["phi"].values) % 360.0,
                )
            if "theta" in ds_merged.data_vars:
                data_vars["Elevation"] = (
                    ds_merged["theta"].dims,
                    90.0 - np.degrees(ds_merged["theta"].values),
                )
            if vod_col and "VOD" in ds_merged.data_vars:
                data_vars[vod_col] = ds_merged["VOD"]

            ds_adapted = xr.Dataset(
                data_vars,
                coords=ds_merged.coords,
            )

        print(
            f"  Adapted: {dict(ds_adapted.sizes)}, vars={sorted(ds_adapted.data_vars)}"
        )

        r = _compare_band(ds_adapted, ds_gnssvod, band_suffix, snr_col, vod_col)
        if r is None:
            continue

        result.results[f"gnssvod_{band_suffix}"] = r

        # Print per-variable summary
        for var, vs in r.variable_stats.items():
            status = "PASS" if var not in r.failures else "FAIL"
            print(
                f"  [{status}] {var}: "
                f"RMSE={vs.rmse:.6g}, MAE={vs.mae:.6g}, "
                f"max={vs.max_abs_diff:.6g}, bias={vs.bias:.6g}, "
                f"n={vs.n_compared:,}, "
                f"NaN: {vs.pct_nan_a:.1%} vs {vs.pct_nan_b:.1%}"
            )
        if r.failures:
            for var, reason in r.failures.items():
                print(f"  !! {var}: {reason}")

    # ── Summary ───────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print(result.summary())
    return result
