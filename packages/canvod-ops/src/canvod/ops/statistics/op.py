"""UpdateStatistics — Op that feeds observations into streaming accumulators."""

from __future__ import annotations

import re
import time
from typing import Any

import numpy as np
import xarray as xr
from loguru import logger

from canvod.ops.base import Op, OpResult
from canvod.ops.statistics.profile import ProfileRegistry
from canvod.streamstats import CellSignalKey


class UpdateStatistics(Op):
    """Feed per-cell observations into a :class:`ProfileRegistry`.

    The dataset passes through unchanged; side-effect is accumulator updates.

    Parameters
    ----------
    registry : ProfileRegistry
        Accumulator registry to update.
    receiver_type : str
        Receiver type label (``"canopy"``, ``"reference"``, ``"derived"``).
    variables : list[str]
        Data variable names to profile (must exist in the dataset).
    cell_id_pattern : str
        Regex pattern matching cell_id coordinate names (default ``"cell_id_.*"``).
    """

    def __init__(
        self,
        registry: ProfileRegistry,
        receiver_type: str,
        variables: list[str] | None = None,
        cell_id_pattern: str = r"cell_id_.*",
    ) -> None:
        self._registry = registry
        self._receiver_type = receiver_type
        self._variables = variables
        self._cell_id_re = re.compile(cell_id_pattern)

    @property
    def name(self) -> str:
        return "update_statistics"

    def _find_cell_id_coord(self, ds: xr.Dataset) -> str | None:
        """Return the first coordinate name matching the cell_id pattern."""
        for cname in ds.coords:
            if self._cell_id_re.fullmatch(str(cname)):
                return str(cname)
        return None

    def __call__(self, ds: xr.Dataset) -> tuple[xr.Dataset, OpResult]:
        t0 = time.perf_counter()
        params: dict[str, Any] = {
            "receiver_type": self._receiver_type,
            "variables": self._variables,
        }
        input_shape = dict(ds.sizes)

        # Find cell_id coordinate
        cell_id_name = self._find_cell_id_coord(ds)
        if cell_id_name is None:
            logger.warning("UpdateStatistics skipped — no cell_id coordinate found")
            return ds, OpResult(
                op_name=self.name,
                parameters=params,
                input_shape=input_shape,
                output_shape=input_shape,
                duration_seconds=time.perf_counter() - t0,
                notes="skipped: no cell_id coordinate",
            )

        # Determine which variables to profile
        variables = self._variables or [v for v in ds.data_vars if v in ds.data_vars]
        if not variables:
            return ds, OpResult(
                op_name=self.name,
                parameters=params,
                input_shape=input_shape,
                output_shape=input_shape,
                duration_seconds=time.perf_counter() - t0,
                notes="skipped: no variables to profile",
            )

        cell_ids = ds.coords[cell_id_name].values  # (epoch, sid)
        sids = ds.sid.values
        n_epoch, n_sid = cell_ids.shape

        n_updates = 0

        for var_name in variables:
            if var_name not in ds.data_vars:
                continue
            var_data = ds[var_name].values  # (epoch, sid)

            # Flatten to 1-D: tile sid indices across epochs
            flat_cells = cell_ids.ravel()  # (n_epoch * n_sid,)
            flat_vals = var_data.ravel()
            flat_sid_idx = np.tile(np.arange(n_sid, dtype=np.int64), n_epoch)

            # Mask: both cell_id and value must be finite
            valid_mask = np.isfinite(flat_cells) & np.isfinite(flat_vals)
            if not np.any(valid_mask):
                continue

            v_cells = flat_cells[valid_mask].astype(np.int64)
            v_vals = flat_vals[valid_mask]
            v_sid_idx = flat_sid_idx[valid_mask]

            # Composite key for grouping: cell_id * n_sid + sid_idx
            composite = v_cells * n_sid + v_sid_idx
            order = np.argsort(composite, kind="mergesort")
            sorted_composite = composite[order]
            sorted_vals = v_vals[order]
            sorted_sid_idx = v_sid_idx[order]
            sorted_cells = v_cells[order]

            # Find group boundaries via diff
            breaks = np.nonzero(np.diff(sorted_composite))[0] + 1
            starts = np.concatenate([[0], breaks])
            ends = np.concatenate([breaks, [len(sorted_composite)]])

            for start, end in zip(starts, ends):
                cell_id = int(sorted_cells[start])
                sid_i = int(sorted_sid_idx[start])
                group_vals = sorted_vals[start:end]

                key = CellSignalKey(
                    cell_id=cell_id,
                    signal_id=str(sids[sid_i]),
                    variable=var_name,
                    receiver_type=self._receiver_type,
                )
                self._registry.update_batch(key, group_vals)
                n_updates += len(group_vals)

        duration = time.perf_counter() - t0
        logger.info(
            "UpdateStatistics: {} updates across {} keys in {:.3f}s",
            n_updates,
            len(self._registry),
            duration,
        )

        return ds, OpResult(
            op_name=self.name,
            parameters=params,
            input_shape=input_shape,
            output_shape=input_shape,
            duration_seconds=duration,
            notes=f"{n_updates} updates, {len(self._registry)} keys",
        )


def update_statistics(
    ds: xr.Dataset,
    registry: ProfileRegistry,
    receiver_type: str = "canopy",
    variables: list[str] | None = None,
) -> xr.Dataset:
    """Convenience function: update statistics for a dataset.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset with ``cell_id_*`` coordinate.
    registry : ProfileRegistry
        Accumulator registry to update.
    receiver_type : str
        Receiver type.
    variables : list[str] | None
        Variables to profile. ``None`` profiles all data vars.

    Returns
    -------
    xr.Dataset
        The input dataset (unmodified).
    """
    op = UpdateStatistics(registry, receiver_type, variables)
    out, _ = op(ds)
    return out
