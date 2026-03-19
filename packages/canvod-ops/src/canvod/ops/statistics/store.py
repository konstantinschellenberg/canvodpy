"""StatisticsStore — Zarr serialization for streaming statistics."""

from __future__ import annotations

from typing import Any

import numpy as np
import zarr

from canvod.ops.statistics.profile import AccumulatorSet, ProfileRegistry
from canvod.streamstats import (
    DEFAULT_EWMA_HALFLIFE,
    DEFAULT_HISTOGRAM_BINS,
    DEFAULT_QUANTILE_PROBS,
    BOCPDAccumulator,
    CellSignalKey,
    ClimatologyGrid,
    EWMAAccumulator,
    GKSketch,
    S4Accumulator,
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

        # Detect EWMA and S4 presence
        has_ewma = any(registry[k].ewma is not None for k in keys)
        has_s4 = any(registry[k].s4 is not None for k in keys)

        # EWMA state: 6 float64 per accumulator
        ewma_states = (
            np.full((n_cells, n_sids, n_vars, 6), np.nan, dtype=np.float64)
            if has_ewma
            else None
        )
        # S4 state: same as Welford (8 float64, tracks intensity)
        s4_states = (
            np.full((n_cells, n_sids, n_vars, 8), np.nan, dtype=np.float64)
            if has_s4
            else None
        )

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
            if acc.ewma is not None and ewma_states is not None:
                ewma_states[ci, si, vi, :] = acc.ewma.to_array()
            if acc.s4 is not None and s4_states is not None:
                s4_states[ci, si, vi, :] = acc.s4._welford.to_array()

        # Write to zarr group
        rx_group = self._group.require_group(receiver_type)

        _write_array(rx_group, "moments", moments)
        _write_array(rx_group, "quantiles", quantiles)
        _write_array(rx_group, "histograms", histograms)
        if autocovariances is not None:
            _write_array(rx_group, "autocovariance", autocovariances)
        if ewma_states is not None:
            _write_array(rx_group, "ewma", ewma_states)
        if s4_states is not None:
            _write_array(rx_group, "s4", s4_states)
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
        if has_ewma:
            rx_group.attrs["ewma_halflife"] = DEFAULT_EWMA_HALFLIFE
        if has_s4:
            rx_group.attrs["s4_enabled"] = True

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

        # Load EWMA data if present
        ewma_halflife = float(
            rx_group.attrs.get("ewma_halflife", DEFAULT_EWMA_HALFLIFE)
        )
        has_ewma = "ewma" in rx_group
        ewma_data = np.asarray(rx_group["ewma"]) if has_ewma else None

        # Load S4 data if present
        has_s4 = "s4" in rx_group
        s4_data = np.asarray(rx_group["s4"]) if has_s4 else None

        registry = ProfileRegistry(
            gk_epsilon=epsilon,
            autocovariance_enabled=has_acov,
            autocovariance_max_lag=acov_max_lag if acov_max_lag > 0 else 1440,
            ewma_enabled=has_ewma,
            ewma_halflife=ewma_halflife,
            s4_enabled=has_s4,
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

                    # Restore EWMA if available
                    ewma = None
                    if has_ewma and ewma_data is not None:
                        ewma_arr = ewma_data[ci, si, vi, :]
                        if not np.all(np.isnan(ewma_arr)):
                            ewma = EWMAAccumulator.from_array(ewma_arr)

                    # Restore S4 if available
                    s4 = None
                    if has_s4 and s4_data is not None:
                        s4_arr = s4_data[ci, si, vi, :]
                        if not np.all(np.isnan(s4_arr)):
                            s4_acc = S4Accumulator()
                            s4_acc._welford = WelfordAccumulator.from_array(s4_arr)
                            s4 = s4_acc

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
                        ewma=ewma,
                        s4=s4,
                    )
                    registry._accumulators[key] = acc

        return registry

    # ------------------------------------------------------------------
    # Generic idempotency helpers
    # ------------------------------------------------------------------

    def _record_range(
        self, receiver_type: str, attr_key: str, start: str, end: str
    ) -> None:
        """Append a ``{start, end}`` record to *attr_key* on the receiver group."""
        rx_group = self._group.require_group(receiver_type)
        processed = list(rx_group.attrs.get(attr_key, []))
        processed.append({"start": start, "end": end})
        rx_group.attrs[attr_key] = processed

    def _is_range_processed(
        self, receiver_type: str, attr_key: str, start: str, end: str
    ) -> bool:
        """Return ``True`` if ``(start, end)`` already exists under *attr_key*."""
        try:
            rx_group = self._group[receiver_type]
        except KeyError:
            return False
        processed = rx_group.attrs.get(attr_key, [])
        return any(r["start"] == start and r["end"] == end for r in processed)

    # --- profile (existing, now delegates) ---

    def record_epoch_range(self, receiver_type: str, start: str, end: str) -> None:
        """Record a processed epoch range for idempotency."""
        self._record_range(receiver_type, "processed_ranges", start, end)

    def is_epoch_range_processed(
        self, receiver_type: str, start: str, end: str
    ) -> bool:
        """Check if an epoch range was already processed."""
        return self._is_range_processed(receiver_type, "processed_ranges", start, end)

    # ------------------------------------------------------------------
    # Climatology persistence
    # ------------------------------------------------------------------

    def save_climatology(
        self, grids: dict[str, ClimatologyGrid], receiver_type: str
    ) -> None:
        """Write each ``ClimatologyGrid`` into ``<rx_type>/climatology/<var>``."""
        rx_group = self._group.require_group(receiver_type)
        clim_group = rx_group.require_group("climatology")
        for var_name, grid in grids.items():
            _write_array(clim_group, var_name, grid.to_array())

    def load_climatology(self, receiver_type: str) -> dict[str, ClimatologyGrid]:
        """Load all climatology grids for *receiver_type*."""
        try:
            clim_group = self._group[receiver_type]["climatology"]
        except KeyError:
            return {}
        grids: dict[str, ClimatologyGrid] = {}
        for name in clim_group.array_keys():
            grids[name] = ClimatologyGrid.from_array(np.asarray(clim_group[name]))
        return grids

    def record_climatology_range(
        self, receiver_type: str, start: str, end: str
    ) -> None:
        self._record_range(receiver_type, "climatology_ranges", start, end)

    def is_climatology_range_processed(
        self, receiver_type: str, start: str, end: str
    ) -> bool:
        return self._is_range_processed(receiver_type, "climatology_ranges", start, end)

    # ------------------------------------------------------------------
    # BOCPD persistence
    # ------------------------------------------------------------------

    def save_bocpd(
        self, accumulators: dict[str, BOCPDAccumulator], receiver_type: str
    ) -> None:
        """Write each ``BOCPDAccumulator`` into ``<rx_type>/bocpd/<var>``."""
        rx_group = self._group.require_group(receiver_type)
        bocpd_group = rx_group.require_group("bocpd")
        for var_name, acc in accumulators.items():
            _write_array(bocpd_group, var_name, acc.to_array())

    def load_bocpd(self, receiver_type: str) -> dict[str, BOCPDAccumulator]:
        """Load all BOCPD accumulators for *receiver_type*."""
        try:
            bocpd_group = self._group[receiver_type]["bocpd"]
        except KeyError:
            return {}
        accumulators: dict[str, BOCPDAccumulator] = {}
        for name in bocpd_group.array_keys():
            accumulators[name] = BOCPDAccumulator.from_array(
                np.asarray(bocpd_group[name])
            )
        return accumulators

    def record_bocpd_range(self, receiver_type: str, start: str, end: str) -> None:
        self._record_range(receiver_type, "bocpd_ranges", start, end)

    def is_bocpd_range_processed(
        self, receiver_type: str, start: str, end: str
    ) -> bool:
        return self._is_range_processed(receiver_type, "bocpd_ranges", start, end)

    # ------------------------------------------------------------------
    # Anomaly summary persistence
    # ------------------------------------------------------------------

    def save_anomaly_summary(
        self,
        receiver_type: str,
        date_str: str,
        summary: dict[str, tuple],
    ) -> None:
        """Append one day's anomaly summary for each variable.

        Each entry in *summary* maps ``variable_name`` to a 6-tuple:
        ``(n_normal, n_mild, n_moderate, n_severe, mean_abs_z, max_abs_z)``.
        """
        rx_group = self._group.require_group(receiver_type)
        anom_group = rx_group.require_group("anomalies")

        variables = sorted(summary.keys())
        row = np.array([summary[v] for v in variables], dtype=np.float64)  # (n_vars, 6)
        row = row.reshape(1, len(variables), 6)

        if "daily_summary" in anom_group:
            existing = np.asarray(anom_group["daily_summary"])
            combined = np.concatenate([existing, row], axis=0)
            _write_array(anom_group, "daily_summary", combined)
        else:
            anom_group.create_array("daily_summary", data=row)

        # Track dates and variable ordering as attributes
        dates = list(anom_group.attrs.get("dates", []))
        dates.append(date_str)
        anom_group.attrs["dates"] = dates
        anom_group.attrs["variables"] = variables

    def load_anomaly_summaries(
        self, receiver_type: str
    ) -> tuple[list[str], list[str], np.ndarray]:
        """Load all anomaly summaries.

        Returns
        -------
        dates : list[str]
        variables : list[str]
        data : np.ndarray, shape ``(n_days, n_vars, 6)``
        """
        try:
            anom_group = self._group[receiver_type]["anomalies"]
        except KeyError:
            return [], [], np.empty((0, 0, 6), dtype=np.float64)
        dates = list(anom_group.attrs.get("dates", []))
        variables = list(anom_group.attrs.get("variables", []))
        data = np.asarray(anom_group["daily_summary"])
        return dates, variables, data

    def record_anomaly_range(self, receiver_type: str, start: str, end: str) -> None:
        self._record_range(receiver_type, "anomaly_ranges", start, end)

    def is_anomaly_range_processed(
        self, receiver_type: str, start: str, end: str
    ) -> bool:
        return self._is_range_processed(receiver_type, "anomaly_ranges", start, end)

    # ------------------------------------------------------------------
    # Pipeline completion marker
    # ------------------------------------------------------------------

    def record_pipeline_completed(
        self, receiver_type: str, start: str, end: str
    ) -> None:
        """Record that the full pipeline (stats → snapshot) completed."""
        self._record_range(receiver_type, "pipeline_completed", start, end)

    def is_pipeline_completed(self, receiver_type: str, start: str, end: str) -> bool:
        return self._is_range_processed(receiver_type, "pipeline_completed", start, end)

    # ------------------------------------------------------------------

    def list_receiver_types(self) -> list[str]:
        """Return receiver types that have been stored."""
        return [name for name in self._group.group_keys()]


def _write_array(group: zarr.Group, name: str, data: np.ndarray) -> None:
    """Write an array, replacing if it already exists."""
    if name in group:
        del group[name]
    group.create_array(name, data=data)
