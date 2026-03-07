"""
Preprocessing utilities for auxiliary GNSS data.

Handles conversion of raw auxiliary data (SP3, CLK) from satellite vehicle (sv)
dimension to signal ID (sid) dimension required for matching with RINEX data.

Matches gnssvodpy.icechunk_manager.preprocessing.IcechunkPreprocessor exactly.
"""

from typing import Any

import numpy as np
import structlog
import xarray as xr
from canvod.readers.gnss_specs.constellations import (
    BEIDOU,
    GALILEO,
    GLONASS,
    GPS,
    IRNSS,
    QZSS,
    SBAS,
)
from canvod.readers.gnss_specs.signals import SignalIDMapper

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Per-process SID issue accumulators
# ---------------------------------------------------------------------------
# Each worker subprocess accumulates SID issues during pad_to_global_sid()
# calls. The main process retrieves them via flush_sid_accumulators() at the
# end of each file's processing, then aggregates across all files.
_accumulated_not_in_global_space: set[str] = set()
_accumulated_dropped_by_filter: set[str] = set()


def flush_sid_accumulators() -> dict[str, list[str]]:
    """Return accumulated SID issues and clear the module-level accumulators.

    Called once per RINEX file at the end of ``preprocess_with_hermite_aux``.
    The returned dict is aggregated by the main process across all files for
    a receiver and logged once per receiver run.

    Returns
    -------
    dict[str, list[str]]
        Keys: ``"not_in_global_space"``, ``"dropped_by_filter"``.
    """
    global _accumulated_not_in_global_space, _accumulated_dropped_by_filter
    result: dict[str, list[str]] = {
        "not_in_global_space": sorted(_accumulated_not_in_global_space),
        "dropped_by_filter": sorted(_accumulated_dropped_by_filter),
    }
    _accumulated_not_in_global_space = set()
    _accumulated_dropped_by_filter = set()
    return result


def create_sv_to_sid_mapping(
    svs: list[str], aggregate_glonass_fdma: bool = True
) -> dict[str, list[str]]:
    """Build mapping from each SV to its possible SIDs.

    Builds all SIDs from known band/code combinations.

    Parameters
    ----------
    svs : list[str]
        List of space vehicles (e.g., ["G01", "E02"]).
    aggregate_glonass_fdma : bool, default True
        Whether to aggregate GLONASS FDMA bands.

    Returns
    -------
    dict[str, list[str]]
        Mapping from sv → list of SIDs.
    """
    mapper = SignalIDMapper(aggregate_glonass_fdma=aggregate_glonass_fdma)
    systems = {
        "G": GPS(),
        "E": GALILEO(),
        "R": GLONASS(aggregate_fdma=aggregate_glonass_fdma),
        "C": BEIDOU(),
        "I": IRNSS(),
        "S": SBAS(),
        "J": QZSS(),
    }

    sv_to_sids: dict[str, list[str]] = {}
    for sv in svs:
        sys_letter = sv[0]
        if sys_letter not in systems:
            continue

        system = systems[sys_letter]
        sids = []
        if sys_letter in mapper.SYSTEM_BANDS:
            for _, band in mapper.SYSTEM_BANDS[sys_letter].items():
                codes = system.BAND_CODES.get(band, ["X"])
                sids.extend(f"{sv}|{band}|{code}" for code in codes)

        sv_to_sids[sv] = sorted(sids)

    return sv_to_sids


def map_aux_sv_to_sid(
    aux_ds: xr.Dataset,
    fill_value: float = np.nan,
    aggregate_glonass_fdma: bool = True,
) -> xr.Dataset:
    """Transform auxiliary dataset from sv → sid dimension.

    Each sv in the dataset is expanded to all its possible SIDs.
    Values are replicated across SIDs for the same satellite.

    Parameters
    ----------
    aux_ds : xr.Dataset
        Dataset with 'sv' dimension.
    fill_value : float, default np.nan
        Fill value for missing entries.
    aggregate_glonass_fdma : bool, default True
        Whether to aggregate GLONASS FDMA bands.

    Returns
    -------
    xr.Dataset
        Dataset with 'sid' dimension replacing 'sv'.
    """
    svs = aux_ds["sv"].values.tolist()
    sv_to_sids = create_sv_to_sid_mapping(svs, aggregate_glonass_fdma)
    all_sids = sorted({sid for sv in svs for sid in sv_to_sids.get(sv, [])})

    new_data_vars = {}
    for name, arr in aux_ds.data_vars.items():
        if "sv" in arr.dims:
            sv_dim = arr.dims.index("sv")
            new_shape = list(arr.shape)
            new_shape[sv_dim] = len(all_sids)
            expanded = np.full(new_shape, fill_value, dtype=arr.dtype)

            for sv_idx, sv in enumerate(svs):
                for sid in sv_to_sids.get(sv, []):
                    sid_idx = all_sids.index(sid)
                    if sv_dim == 0:
                        expanded[sid_idx, ...] = arr.values[sv_idx, ...]
                    elif sv_dim == 1:
                        expanded[..., sid_idx] = arr.values[..., sv_idx]
                    else:
                        slices_new = [slice(None)] * len(new_shape)
                        slices_old = [slice(None)] * len(arr.shape)
                        slices_new[sv_dim] = sid_idx
                        slices_old[sv_dim] = sv_idx
                        expanded[tuple(slices_new)] = arr.values[tuple(slices_old)]

            new_dims = list(arr.dims)
            new_dims[sv_dim] = "sid"
            new_data_vars[name] = (tuple(new_dims), expanded, arr.attrs)
        else:
            new_data_vars[name] = arr

    # Coordinates
    new_coords = {
        **{k: v for k, v in aux_ds.coords.items() if k != "sv"},
        "sid": ("sid", all_sids),
    }

    return xr.Dataset(new_data_vars, coords=new_coords, attrs=aux_ds.attrs.copy())


def pad_to_global_sid(
    ds: xr.Dataset,
    keep_sids: list[str] | None = None,
    aggregate_glonass_fdma: bool = True,
) -> xr.Dataset:
    """Pad dataset so it has all possible SIDs across all constellations.
    Ensures consistent sid dimension for appending to Icechunk.

    Parameters
    ----------
    ds : xr.Dataset
        Dataset with 'sid' dimension.
    keep_sids : list[str] | None
        Optional list of specific SIDs to keep. If None, keeps all.
    aggregate_glonass_fdma : bool, default True
        Whether to aggregate GLONASS FDMA bands.

    Returns
    -------
    xr.Dataset
        Dataset padded with NaN for missing SIDs.
    """
    mapper = SignalIDMapper(aggregate_glonass_fdma=aggregate_glonass_fdma)
    systems = {
        "G": GPS(),
        "E": GALILEO(),
        "R": GLONASS(aggregate_fdma=aggregate_glonass_fdma),
        "C": BEIDOU(),
        "I": IRNSS(),
        "S": SBAS(),
        "J": QZSS(),
    }

    # Generate all possible SIDs
    sids = [
        f"{sv}|{band}|{code}"
        for sys_letter, bands in mapper.SYSTEM_BANDS.items()
        for _, band in bands.items()
        for sv in systems[sys_letter].svs
        for code in systems[sys_letter].BAND_CODES.get(band, ["X"])
    ]
    sids = sorted(sids)
    global_sid_set = set(sids)

    # Accumulate SIDs that fall outside the constellation model's universe.
    # These are logged once per receiver run by the pipeline (not per file).
    if "sid" in ds.coords:
        ds_sids = set(ds.sid.values)
        unknown_sids = ds_sids - global_sid_set
        if unknown_sids:
            _accumulated_not_in_global_space.update(unknown_sids)

    # Filter to keep_sids if provided
    if keep_sids is not None and len(keep_sids) > 0:
        keep_set = set(keep_sids)
        sids_before = set(sids)
        sids = sorted(sids_before.intersection(keep_set))

        # Accumulate observed SIDs dropped by keep_sids filter.
        # Logged once per receiver run by the pipeline (not per file).
        if "sid" in ds.coords:
            ds_sids = set(ds.sid.values)
            dropped_by_filter = (ds_sids & sids_before) - keep_set
            if dropped_by_filter:
                _accumulated_dropped_by_filter.update(dropped_by_filter)

    return ds.reindex({"sid": sids}, fill_value=np.nan)


def normalize_sid_dtype(ds: xr.Dataset) -> xr.Dataset:
    """Ensure sid coordinate uses object dtype.

    Parameters
    ----------
    ds : xr.Dataset
        Dataset with 'sid' coordinate.

    Returns
    -------
    xr.Dataset
        Dataset with sid as object dtype.
    """
    if ds is None:
        return ds
    if "sid" in ds.coords and ds.sid.dtype.kind == "U":
        ds = ds.assign_coords(
            sid=xr.Variable("sid", ds.sid.values.astype(object), ds.sid.attrs)
        )
    return ds


def strip_fillvalue(ds: xr.Dataset) -> xr.Dataset:
    """Remove _FillValue attrs/encodings.

    Parameters
    ----------
    ds : xr.Dataset
        Dataset to clean.

    Returns
    -------
    xr.Dataset
        Dataset with _FillValue attributes removed.
    """
    if ds is None:
        return ds
    for v in ds.data_vars:
        ds[v].attrs.pop("_FillValue", None)
        ds[v].encoding.pop("_FillValue", None)
    return ds


def add_future_datavars(
    ds: xr.Dataset, var_config: dict[str, dict[str, Any]]
) -> xr.Dataset:
    """Add placeholder data variables from a configuration dictionary.

    Parameters
    ----------
    ds : xr.Dataset
        Dataset to add variables to.
    var_config : dict[str, dict[str, Any]]
        Configuration dict with structure:
        {
            "var_name": {
                "fill_value": value,
                "dtype": numpy dtype,
                "attrs": {attribute dict}
            }
        }

    Returns
    -------
    xr.Dataset
        Dataset with new variables added.
    """
    n_epochs, n_sids = ds.sizes["epoch"], ds.sizes["sid"]
    for name, cfg in var_config.items():
        if name not in ds:
            arr = np.full((n_epochs, n_sids), cfg["fill_value"], dtype=cfg["dtype"])
            ds[name] = (("epoch", "sid"), arr, cfg["attrs"])
    return ds


def prep_aux_ds(
    aux_ds: xr.Dataset,
    fill_value: float = np.nan,
    aggregate_glonass_fdma: bool = True,
    keep_sids: list[str] | None = None,
) -> xr.Dataset:
    """Preprocess auxiliary dataset before writing to Icechunk.

    Performs complete 4-step preprocessing:
    1. Convert sv → sid dimension
    2. Pad to global sid list (all constellations) or filter to keep_sids
    3. Normalize sid dtype to object
    4. Strip _FillValue attributes

    This matches
    gnssvodpy.icechunk_manager.preprocessing.IcechunkPreprocessor.prep_aux_ds().

    Parameters
    ----------
    aux_ds : xr.Dataset
        Dataset with 'sv' dimension.
    fill_value : float, default np.nan
        Fill value for missing entries.
    aggregate_glonass_fdma : bool, default True
        Whether to aggregate GLONASS FDMA bands.
    keep_sids : list[str] | None, default None
        List of specific SIDs to keep. If None, keeps all possible SIDs.

    Returns
    -------
    xr.Dataset
        Fully preprocessed dataset ready for Icechunk or interpolation.
    """
    ds = map_aux_sv_to_sid(aux_ds, fill_value, aggregate_glonass_fdma)
    ds = pad_to_global_sid(
        ds, keep_sids=keep_sids, aggregate_glonass_fdma=aggregate_glonass_fdma
    )
    ds = normalize_sid_dtype(ds)
    ds = strip_fillvalue(ds)
    return ds


def preprocess_aux_for_interpolation(
    aux_ds: xr.Dataset,
    fill_value: float = np.nan,
    full_preprocessing: bool = False,
    aggregate_glonass_fdma: bool = True,
) -> xr.Dataset:
    """Preprocess auxiliary dataset before interpolation.

    Converts satellite vehicle (sv) dimension to Signal ID (sid) dimension,
    which is required for matching with RINEX observations after interpolation.

    Parameters
    ----------
    aux_ds : xr.Dataset
        Raw auxiliary dataset with 'sv' dimension.
    fill_value : float, default np.nan
        Fill value for missing entries.
    full_preprocessing : bool, default False
        If True, applies full 4-step preprocessing (pad_to_global_sid,
        normalize_sid_dtype, strip_fillvalue). If False, only converts
        sv → sid (sufficient for interpolation).
    aggregate_glonass_fdma : bool, default True
        Whether to aggregate GLONASS FDMA bands.

    Returns
    -------
    xr.Dataset
        Preprocessed dataset with 'sid' dimension.

    Notes
    -----
    This must be called BEFORE interpolation. The workflow is:
    1. Load raw SP3/CLK data (sv dimension)
    2. Convert sv → sid (this function)
    3. Interpolate to target epochs
    4. Match with RINEX data (sid dimension)

    For most interpolation use cases, `full_preprocessing=False` is sufficient.
    Use `full_preprocessing=True` when preparing data for Icechunk storage.

    Examples
    --------
    >>> # Load raw SP3 data
    >>> sp3_data = Sp3File(...).to_dataset()
    >>> sp3_data.dims
    {'epoch': 96, 'sv': 32}
    >>>
    >>> # Preprocess before interpolation (minimal)
    >>> sp3_preprocessed = preprocess_aux_for_interpolation(sp3_data)
    >>> sp3_preprocessed.dims
    {'epoch': 96, 'sid': 384}
    >>>
    >>> # Preprocess before Icechunk (full)
    >>> sp3_preprocessed = preprocess_aux_for_interpolation(
    ...     sp3_data,
    ...     full_preprocessing=True,
    ... )
    >>> sp3_preprocessed.dims
    {'epoch': 96, 'sid': ~2000}  # Padded to all possible sids
    >>>
    >>> # Now interpolate
    >>> sp3_interp = interpolator.interpolate(sp3_preprocessed, target_epochs)
    """
    if full_preprocessing:
        return prep_aux_ds(aux_ds, fill_value, aggregate_glonass_fdma)
    else:
        return map_aux_sv_to_sid(aux_ds, fill_value, aggregate_glonass_fdma)
