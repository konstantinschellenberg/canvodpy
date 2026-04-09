"""Temporal aggregation operation."""

import re
import time
from typing import Any

import numpy as np
import pandas as pd
import polars as pl
import structlog
import xarray as xr

from canvod.ops.base import Op, OpResult

logger = structlog.get_logger(__name__)


def _convert_to_polars_freq(freq_str: str) -> str:
    """Convert a pandas-style frequency string to Polars truncate format.

    Examples: ``"1min"`` -> ``"1m"``, ``"10T"`` -> ``"10m"``, ``"1D"`` -> ``"1d"``.
    """
    freq_lower = freq_str.lower()
    match = re.search(r"(\d+)min", freq_lower)
    if match:
        return f"{match.group(1)}m"
    if "T" in freq_str:
        return freq_str.replace("T", "m")
    return freq_str.replace("D", "d").replace("H", "h")


def _median_epoch_spacing(epochs: np.ndarray) -> pd.Timedelta:
    """Return the median spacing between sorted epoch values."""
    diffs = np.diff(np.sort(epochs))
    return pd.Timedelta(np.median(diffs))  # ty: ignore[invalid-return-type]


class TemporalAggregate(Op):
    """Aggregate an ``(epoch, sid)`` dataset to regular time bins.

    Parameters
    ----------
    freq : str
        Target frequency as a pandas offset alias (e.g. ``"1min"``, ``"30s"``).
    method : str
        Aggregation method: ``"mean"`` or ``"median"``.
    """

    def __init__(self, freq: str = "1min", method: str = "mean") -> None:
        if method not in ("mean", "median"):
            msg = f"Unsupported aggregation method: {method!r}"
            raise ValueError(msg)
        self._freq = freq
        self._method = method

    @property
    def name(self) -> str:
        return "temporal_aggregate"

    def __call__(self, ds: xr.Dataset) -> tuple[xr.Dataset, OpResult]:
        t0 = time.perf_counter()
        params: dict[str, Any] = {"freq": self._freq, "method": self._method}
        input_shape = {str(k): int(v) for k, v in dict(ds.sizes).items()}

        # --- Early exit if data is already at or coarser than requested freq ---
        requested_ns = int(pd.tseries.frequencies.to_offset(self._freq).nanos)
        requested_td = pd.Timedelta(requested_ns, unit="ns")
        median_spacing = _median_epoch_spacing(ds.epoch.values)

        if median_spacing >= requested_td:
            logger.info(
                "temporal_aggregation_skipped",
                median_spacing=str(median_spacing),
                requested=str(requested_td),
            )
            result = OpResult(
                op_name=self.name,
                parameters=params,
                input_shape=input_shape,
                output_shape=input_shape,
                duration_seconds=time.perf_counter() - t0,
                notes=f"no-op: median spacing {median_spacing} >= {requested_td}",
            )
            return ds, result

        polars_freq = _convert_to_polars_freq(self._freq)

        # --- Identify coordinate roles ---
        sid_only_coords: list[str] = []
        epoch_sid_coords: list[str] = []
        for cname, coord in ds.coords.items():
            if cname in ("epoch", "sid"):
                continue
            dims = coord.dims
            if dims == ("sid",):
                sid_only_coords.append(str(cname))
            elif set(dims) == {"epoch", "sid"}:
                epoch_sid_coords.append(str(cname))

        data_var_names: list[str] = [str(v) for v in ds.data_vars]

        # --- Build long-form Polars DataFrame ---
        epoch_vals = ds.epoch.values  # datetime64
        sid_vals = ds.sid.values

        rows: dict[str, list[Any]] = {"epoch": [], "sid": []}
        agg_columns: list[str] = data_var_names + epoch_sid_coords
        for col in agg_columns:
            rows[col] = []

        for col in agg_columns:
            arr = ds[col].values if col in data_var_names else ds.coords[col].values
            # arr shape is (epoch, sid)
            rows[col] = arr.ravel().tolist()

        n_epoch, n_sid = len(epoch_vals), len(sid_vals)
        rows["epoch"] = np.repeat(epoch_vals, n_sid).tolist()
        rows["sid"] = np.tile(sid_vals, n_epoch).tolist()

        df = pl.DataFrame(rows)

        # --- Truncate + group_by ---
        df = df.with_columns(
            pl.col("epoch")
            .cast(pl.Datetime("ns"))
            .dt.truncate(polars_freq)
            .alias("time_bin")
        )

        agg_exprs = []
        for col in agg_columns:
            if self._method == "mean":
                agg_exprs.append(pl.col(col).mean().alias(col))
            else:
                agg_exprs.append(pl.col(col).median().alias(col))

        grouped = (
            df.group_by(["time_bin", "sid"]).agg(agg_exprs).sort(["time_bin", "sid"])
        )

        # --- Pivot back to (epoch, sid) ---
        new_epochs = grouped["time_bin"].unique().sort().to_numpy()
        new_sids = sid_vals  # sid dimension unchanged

        n_new_epoch = len(new_epochs)
        n_new_sid = len(new_sids)

        # Build sid index for fast lookup
        sid_to_idx = {s: i for i, s in enumerate(new_sids)}
        epoch_to_idx = {np.datetime64(e): i for i, e in enumerate(new_epochs)}

        # Pre-allocate arrays
        var_arrays: dict[str, np.ndarray] = {}
        for col in agg_columns:
            var_arrays[col] = np.full(
                (n_new_epoch, n_new_sid), np.nan, dtype=np.float64
            )

        # Fill from grouped
        g_time = grouped["time_bin"].to_numpy()
        g_sid = grouped["sid"].to_numpy()

        for col in agg_columns:
            g_vals = grouped[col].to_numpy()
            arr = var_arrays[col]
            for k in range(len(g_time)):
                ei = epoch_to_idx.get(np.datetime64(g_time[k]))
                si = sid_to_idx.get(g_sid[k])
                if ei is not None and si is not None:
                    arr[ei, si] = g_vals[k]

        # --- Rebuild xarray Dataset ---
        new_coords: dict[str, Any] = {
            "epoch": new_epochs,
            "sid": new_sids,
        }
        # Preserve sid-only coords
        for cname in sid_only_coords:
            new_coords[cname] = ds.coords[cname]

        # Add aggregated (epoch, sid) coords
        for cname in epoch_sid_coords:
            new_coords[cname] = (("epoch", "sid"), var_arrays.pop(cname))

        new_data_vars: dict[str, Any] = {}
        for vname in data_var_names:
            new_data_vars[vname] = (("epoch", "sid"), var_arrays[vname])

        out = xr.Dataset(new_data_vars, coords=new_coords, attrs=ds.attrs.copy())

        duration = time.perf_counter() - t0
        output_shape = {str(k): int(v) for k, v in dict(out.sizes).items()}

        logger.info(
            "temporal_aggregation_complete",
            input_shape=input_shape,
            output_shape=output_shape,
            duration_s=round(duration, 2),
        )

        result = OpResult(
            op_name=self.name,
            parameters=params,
            input_shape=input_shape,
            output_shape=output_shape,
            duration_seconds=duration,
        )
        return out, result


def temporal_aggregate(
    ds: xr.Dataset,
    freq: str = "1min",
    method: str = "mean",
) -> xr.Dataset:
    """Convenience function: temporally aggregate a dataset.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset with ``(epoch, sid)`` dimensions.
    freq : str
        Target frequency.
    method : str
        ``"mean"`` or ``"median"``.

    Returns
    -------
    xr.Dataset
        Aggregated dataset.
    """
    op = TemporalAggregate(freq=freq, method=method)
    out, _ = op(ds)
    return out
