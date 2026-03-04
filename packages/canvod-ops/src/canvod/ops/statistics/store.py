"""StatisticsStore — Zarr serialization for streaming statistics."""

from __future__ import annotations

from typing import Any

import numpy as np
import zarr

from canvod.ops.statistics.profile import AccumulatorSet, ProfileRegistry
from canvod.streamstats import (
    DEFAULT_HISTOGRAM_BINS,
    DEFAULT_QUANTILE_PROBS,
    CellSignalKey,
    GKSketch,
    StreamingAutocovariance,
    StreamingHistogram,
    WelfordAccumulator,
)


class StatisticsStore:
    """Persist and load a :class:`ProfileRegistry` to/from a Zarr group.

    Parameters
    ----------
    group : zarr.Group
        Root Zarr group for statistics storage.
    """

    def __init__(self, group: zarr.Group) -> None:
        self._group = group

    def save(self, registry: ProfileRegistry, receiver_type: str) -> None:
        """Write registry arrays for a given receiver_type.

        Creates sub-group ``<receiver_type>/`` containing:
        - ``moments``    (n_cells, n_sids, n_vars, 8) float64
        - ``quantiles``  (n_cells, n_sids, n_vars, n_probs) float64
        - ``histograms`` (n_cells, n_sids, n_vars, max_hist_len) int64
        - ``cell_ids``   (n_cells,) int64
        - ``signal_ids`` (n_sids,) str
        - ``variables``  (n_vars,) str
        """
        keys = [k for k in registry.keys() if k.receiver_type == receiver_type]
        if not keys:
            return

        cell_ids = sorted({k.cell_id for k in keys})
        signal_ids = sorted({k.signal_id for k in keys})
        variables = sorted({k.variable for k in keys})

        cell_idx = {c: i for i, c in enumerate(cell_ids)}
        sid_idx = {s: i for i, s in enumerate(signal_ids)}
        var_idx = {v: i for i, v in enumerate(variables)}

        n_cells = len(cell_ids)
        n_sids = len(signal_ids)
        n_vars = len(variables)
        n_probs = len(DEFAULT_QUANTILE_PROBS)

        # Determine max histogram serialization length
        max_hist_len = 0
        for key in keys:
            acc = registry[key]
            if acc.histogram is not None:
                max_hist_len = max(max_hist_len, acc.histogram.n_bins + 2)

        moments = np.full((n_cells, n_sids, n_vars, 8), np.nan, dtype=np.float64)
        quantiles = np.full(
            (n_cells, n_sids, n_vars, n_probs), np.nan, dtype=np.float64
        )
        histograms = np.zeros(
            (n_cells, n_sids, n_vars, max(max_hist_len, 1)), dtype=np.int64
        )

        # Detect autocovariance and determine max_lag
        autocovariance_max_lag = 0
        for key in keys:
            acc = registry[key]
            if acc.autocovariance is not None:
                autocovariance_max_lag = max(
                    autocovariance_max_lag, acc.autocovariance.max_lag
                )

        # Allocate autocovariance array if any accumulator has it
        if autocovariance_max_lag > 0:
            acov_len = 2 * autocovariance_max_lag + 5
            autocovariances = np.zeros(
                (n_cells, n_sids, n_vars, acov_len), dtype=np.float64
            )
        else:
            autocovariances = None

        for key in keys:
            ci = cell_idx[key.cell_id]
            si = sid_idx[key.signal_id]
            vi = var_idx[key.variable]
            acc = registry[key]

            moments[ci, si, vi, :] = acc.welford.to_array()
            quantiles[ci, si, vi, :] = acc.gk.snapshot(DEFAULT_QUANTILE_PROBS)
            if acc.histogram is not None:
                h_arr = acc.histogram.to_array()
                histograms[ci, si, vi, : len(h_arr)] = h_arr
            if acc.autocovariance is not None and autocovariances is not None:
                acov_arr = acc.autocovariance.to_array()
                autocovariances[ci, si, vi, : len(acov_arr)] = acov_arr

        # Write to zarr group
        rx_group = self._group.require_group(receiver_type)

        _write_array(rx_group, "moments", moments)
        _write_array(rx_group, "quantiles", quantiles)
        _write_array(rx_group, "histograms", histograms)
        if autocovariances is not None:
            _write_array(rx_group, "autocovariance", autocovariances)
        _write_array(rx_group, "cell_ids", np.array(cell_ids, dtype=np.int64))
        # Store string coordinate arrays as attributes (Zarr v3 doesn't support object dtype)
        rx_group.attrs["signal_ids"] = list(signal_ids)
        rx_group.attrs["variables"] = list(variables)

        # Store histogram bin specs as attributes
        hist_specs: dict[str, Any] = {}
        for v in variables:
            if v in DEFAULT_HISTOGRAM_BINS:
                low, high, n_bins = DEFAULT_HISTOGRAM_BINS[v]
                hist_specs[v] = {"low": low, "high": high, "n_bins": n_bins}
        rx_group.attrs["histogram_specs"] = hist_specs
        rx_group.attrs["quantile_probs"] = list(DEFAULT_QUANTILE_PROBS)
        if autocovariance_max_lag > 0:
            rx_group.attrs["autocovariance_max_lag"] = autocovariance_max_lag

    def load(self, receiver_type: str, epsilon: float = 0.01) -> ProfileRegistry:
        """Load a ProfileRegistry from stored arrays.

        Parameters
        ----------
        receiver_type : str
            Receiver type group to load.
        epsilon : float
            GK sketch epsilon for restored sketches.

        Returns
        -------
        ProfileRegistry
            Restored registry.
        """
        rx_group = self._group[receiver_type]

        cell_ids = np.asarray(rx_group["cell_ids"])
        signal_ids = list(rx_group.attrs["signal_ids"])
        variables = list(rx_group.attrs["variables"])
        moments = np.asarray(rx_group["moments"])
        hist_data = np.asarray(rx_group["histograms"])
        hist_specs = dict(rx_group.attrs.get("histogram_specs", {}))

        # Load autocovariance data if present
        acov_max_lag = int(rx_group.attrs.get("autocovariance_max_lag", 0))
        has_acov = acov_max_lag > 0 and "autocovariance" in rx_group
        acov_data = np.asarray(rx_group["autocovariance"]) if has_acov else None

        registry = ProfileRegistry(
            gk_epsilon=epsilon,
            autocovariance_enabled=has_acov,
            autocovariance_max_lag=acov_max_lag if acov_max_lag > 0 else 1440,
        )

        for ci, cell_id in enumerate(cell_ids):
            for si, sid in enumerate(signal_ids):
                for vi, var in enumerate(variables):
                    state = moments[ci, si, vi, :]
                    if np.all(np.isnan(state)):
                        continue

                    welford = WelfordAccumulator.from_array(state)

                    # Restore histogram if spec available
                    var_str = str(var)
                    histogram = None
                    if var_str in hist_specs:
                        spec = hist_specs[var_str]
                        h_arr = hist_data[ci, si, vi, :]
                        histogram = StreamingHistogram.from_array(
                            h_arr, spec["low"], spec["high"], spec["n_bins"]
                        )

                    # Restore autocovariance if available
                    autocovariance = None
                    if has_acov and acov_data is not None:
                        acov_arr = acov_data[ci, si, vi, :]
                        if acov_arr[1] > 0:  # count > 0
                            autocovariance = StreamingAutocovariance.from_array(
                                acov_arr
                            )

                    key = CellSignalKey(
                        cell_id=int(cell_id),
                        signal_id=str(sid),
                        variable=var_str,
                        receiver_type=receiver_type,
                    )

                    acc = AccumulatorSet(
                        welford=welford,
                        gk=GKSketch(epsilon=epsilon),
                        histogram=histogram,
                        autocovariance=autocovariance,
                    )
                    registry._accumulators[key] = acc

        return registry

    def record_epoch_range(self, receiver_type: str, start: str, end: str) -> None:
        """Record a processed epoch range for idempotency."""
        rx_group = self._group.require_group(receiver_type)
        processed = list(rx_group.attrs.get("processed_ranges", []))
        processed.append({"start": start, "end": end})
        rx_group.attrs["processed_ranges"] = processed

    def is_epoch_range_processed(
        self, receiver_type: str, start: str, end: str
    ) -> bool:
        """Check if an epoch range was already processed."""
        try:
            rx_group = self._group[receiver_type]
        except KeyError:
            return False
        processed = rx_group.attrs.get("processed_ranges", [])
        return any(r["start"] == start and r["end"] == end for r in processed)

    def list_receiver_types(self) -> list[str]:
        """Return receiver types that have been stored."""
        return [name for name in self._group.group_keys()]


def _write_array(group: zarr.Group, name: str, data: np.ndarray) -> None:
    """Write an array, replacing if it already exists."""
    if name in group:
        del group[name]
    group.create_array(name, data=data)
