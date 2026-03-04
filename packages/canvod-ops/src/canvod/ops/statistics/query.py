"""Read-only query API over StatisticsStore for live display."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import numpy as np

from canvod.ops.statistics.profile import ProfileRegistry
from canvod.streamstats import (
    DEFAULT_QUANTILE_PROBS,
    EWMAAccumulator,
    RunningMedianFilter,
    effective_sample_size_from_autocovariance,
)

if TYPE_CHECKING:
    from canvod.ops.statistics.store import StatisticsStore


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class VariableStats:
    """Aggregated statistics for a single variable across all (cell, signal) keys."""

    variable: str
    n_keys: int
    total_count: int
    global_mean: float
    global_std: float
    min_val: float
    max_val: float
    quantiles: dict[float, float]


@dataclass(frozen=True)
class ConfidenceEnvelope:
    """Confidence envelope: mean +/- z * std / sqrt(n_eff)."""

    variable: str
    mean: float
    std: float
    n_eff: float
    lower: float
    upper: float
    z_multiplier: float


@dataclass(frozen=True)
class ClimatologyHeatmap:
    """2-D DOY x TOD climatology grid as arrays."""

    variable: str
    doy_bins: np.ndarray
    tod_bins: np.ndarray
    mean_grid: np.ndarray
    std_grid: np.ndarray
    count_grid: np.ndarray


@dataclass(frozen=True)
class AnomalyTimeline:
    """Daily anomaly summary time series."""

    dates: list[str]
    variables: list[str]
    data: np.ndarray  # (n_days, n_vars, 6)


@dataclass(frozen=True)
class ChangepointStatus:
    """Current BOCPD state across variables."""

    variables: list[str]
    changepoint_probs: list[float]
    map_run_lengths: list[int]
    predictive_means: list[float]
    predictive_stds: list[float]
    n_observations: list[int]


@dataclass(frozen=True)
class StatisticsSnapshot:
    """Complete snapshot of all statistics for a receiver type."""

    receiver_type: str
    registry_summary: dict[str, Any]
    variable_stats: dict[str, VariableStats]
    changepoint_status: ChangepointStatus | None
    anomaly_timeline: AnomalyTimeline | None
    climatology_heatmaps: dict[str, ClimatologyHeatmap]
    confidence_envelopes: dict[str, ConfidenceEnvelope]


# ---------------------------------------------------------------------------
# Query class
# ---------------------------------------------------------------------------


class StatisticsQuery:
    """Read-only query layer over :class:`StatisticsStore`.

    Parameters
    ----------
    store : StatisticsStore
        The underlying Zarr-backed statistics store.
    """

    def __init__(self, store: StatisticsStore) -> None:
        self._store = store

    def list_receiver_types(self) -> list[str]:
        """Return receiver types that have stored data."""
        return self._store.list_receiver_types()

    # ------------------------------------------------------------------
    # Full snapshot
    # ------------------------------------------------------------------

    def snapshot(self, receiver_type: str, *, z: float = 2.0) -> StatisticsSnapshot:
        """Build a complete snapshot for *receiver_type*."""
        registry = self._store.load(receiver_type)
        summary = registry.summary()
        variables = summary.get("variables", [])

        var_stats: dict[str, VariableStats] = {}
        envelopes: dict[str, ConfidenceEnvelope] = {}
        for var in variables:
            var_stats[var] = self.variable_summary(
                receiver_type, var, registry=registry
            )
            envelopes[var] = self.confidence_envelope(
                receiver_type, var, z=z, registry=registry
            )

        cp = self.changepoint_status(receiver_type)
        at = self.anomaly_timeline(receiver_type)

        heatmaps: dict[str, ClimatologyHeatmap] = {}
        for var in variables:
            hm = self.climatology_heatmap(receiver_type, var)
            if hm is not None:
                heatmaps[var] = hm

        return StatisticsSnapshot(
            receiver_type=receiver_type,
            registry_summary=summary,
            variable_stats=var_stats,
            changepoint_status=cp,
            anomaly_timeline=at,
            climatology_heatmaps=heatmaps,
            confidence_envelopes=envelopes,
        )

    # ------------------------------------------------------------------
    # Per-variable queries
    # ------------------------------------------------------------------

    def variable_summary(
        self,
        receiver_type: str,
        variable: str,
        *,
        registry: ProfileRegistry | None = None,
    ) -> VariableStats:
        """Aggregate Welford stats across all (cell, signal) keys for *variable*."""
        if registry is None:
            registry = self._store.load(receiver_type)

        keys = [
            k
            for k in registry.keys()
            if k.variable == variable and k.receiver_type == receiver_type
        ]
        if not keys:
            return VariableStats(
                variable=variable,
                n_keys=0,
                total_count=0,
                global_mean=float("nan"),
                global_std=float("nan"),
                min_val=float("nan"),
                max_val=float("nan"),
                quantiles={},
            )

        # Collect per-key stats
        counts = []
        means = []
        variances = []
        mins = []
        maxs = []
        for k in keys:
            acc = registry[k]
            w = acc.welford
            if w.count > 0:
                counts.append(w.count)
                means.append(w.mean)
                variances.append(w.variance if w.count >= 2 else 0.0)
                mins.append(w.min)
                maxs.append(w.max)

        if not counts:
            return VariableStats(
                variable=variable,
                n_keys=len(keys),
                total_count=0,
                global_mean=float("nan"),
                global_std=float("nan"),
                min_val=float("nan"),
                max_val=float("nan"),
                quantiles={},
            )

        total_count = sum(counts)
        # Weighted mean
        global_mean = sum(c * m for c, m in zip(counts, means)) / total_count
        # Pooled variance (within + between)
        pooled_var = (
            sum(
                c * (v + (m - global_mean) ** 2)
                for c, m, v in zip(counts, means, variances)
            )
            / total_count
        )
        global_std = math.sqrt(max(pooled_var, 0.0))

        # Quantiles from the first GK sketch with data (median key)
        quantiles: dict[float, float] = {}
        for k in keys:
            acc = registry[k]
            if acc.gk.count > 0:
                try:
                    snap = acc.gk.snapshot(DEFAULT_QUANTILE_PROBS)
                    quantiles = dict(zip(DEFAULT_QUANTILE_PROBS, snap))
                except Exception:
                    pass
                break

        return VariableStats(
            variable=variable,
            n_keys=len(keys),
            total_count=total_count,
            global_mean=global_mean,
            global_std=global_std,
            min_val=min(mins),
            max_val=max(maxs),
            quantiles=quantiles,
        )

    def confidence_envelope(
        self,
        receiver_type: str,
        variable: str,
        *,
        z: float = 2.0,
        registry: ProfileRegistry | None = None,
    ) -> ConfidenceEnvelope:
        """Compute confidence envelope using Welford variance + n_eff."""
        if registry is None:
            registry = self._store.load(receiver_type)

        keys = [
            k
            for k in registry.keys()
            if k.variable == variable and k.receiver_type == receiver_type
        ]

        # Aggregate across keys
        total_count = 0
        sum_cx = 0.0
        sum_cv = 0.0
        has_acov = False
        acov_arr = None

        for k in keys:
            acc = registry[k]
            w = acc.welford
            if w.count > 0:
                total_count += w.count
                sum_cx += w.count * w.mean
                v = w.variance if w.count >= 2 else 0.0
                sum_cv += w.count * v
            if acc.autocovariance is not None and acc.autocovariance.count > 0:
                has_acov = True
                # Use the first available autocovariance for n_eff
                if acov_arr is None:
                    acov_arr = acc.autocovariance.covariances

        if total_count == 0:
            return ConfidenceEnvelope(
                variable=variable,
                mean=float("nan"),
                std=float("nan"),
                n_eff=0.0,
                lower=float("nan"),
                upper=float("nan"),
                z_multiplier=z,
            )

        mean = sum_cx / total_count
        std = math.sqrt(max(sum_cv / total_count, 0.0))

        # Effective sample size
        if has_acov and acov_arr is not None and len(acov_arr) > 1:
            n_eff = effective_sample_size_from_autocovariance(acov_arr)
        else:
            n_eff = float(total_count)

        half_width = z * std / math.sqrt(n_eff) if n_eff > 0 else float("nan")

        return ConfidenceEnvelope(
            variable=variable,
            mean=mean,
            std=std,
            n_eff=n_eff,
            lower=mean - half_width,
            upper=mean + half_width,
            z_multiplier=z,
        )

    # ------------------------------------------------------------------
    # BOCPD / Anomaly / Climatology
    # ------------------------------------------------------------------

    def changepoint_status(self, receiver_type: str) -> ChangepointStatus | None:
        """Load BOCPD state for all variables. Returns None if no data."""
        bocpd = self._store.load_bocpd(receiver_type)
        if not bocpd:
            return None
        variables = sorted(bocpd.keys())
        return ChangepointStatus(
            variables=variables,
            changepoint_probs=[bocpd[v].changepoint_prob for v in variables],
            map_run_lengths=[bocpd[v].map_run_length for v in variables],
            predictive_means=[bocpd[v].result.predictive_mean for v in variables],
            predictive_stds=[bocpd[v].result.predictive_std for v in variables],
            n_observations=[bocpd[v].count for v in variables],
        )

    def anomaly_timeline(self, receiver_type: str) -> AnomalyTimeline | None:
        """Load anomaly summaries. Returns None if no data."""
        dates, variables, data = self._store.load_anomaly_summaries(receiver_type)
        if not dates:
            return None
        return AnomalyTimeline(dates=dates, variables=variables, data=data)

    def climatology_heatmap(
        self, receiver_type: str, variable: str
    ) -> ClimatologyHeatmap | None:
        """Build 2-D grid arrays from ClimatologyGrid. Returns None if not found."""
        grids = self._store.load_climatology(receiver_type)
        if variable not in grids:
            return None
        grid = grids[variable]
        n_doy = grid.n_doy_bins
        n_tod = grid.n_tod_bins

        mean_grid = np.full((n_doy, n_tod), np.nan)
        std_grid = np.full((n_doy, n_tod), np.nan)
        count_grid = np.zeros((n_doy, n_tod), dtype=np.int64)

        for di in range(n_doy):
            for ti in range(n_tod):
                c = grid.count(di, ti)
                count_grid[di, ti] = c
                if c > 0:
                    mean_grid[di, ti] = grid.mean(di, ti)
                    std_grid[di, ti] = grid.std(di, ti)

        # Bin edges
        doy_bins = np.arange(n_doy + 1, dtype=np.float64) * grid.doy_window + 1
        tod_bins = np.arange(n_tod + 1, dtype=np.float64) * grid.tod_window

        return ClimatologyHeatmap(
            variable=variable,
            doy_bins=doy_bins,
            tod_bins=tod_bins,
            mean_grid=mean_grid,
            std_grid=std_grid,
            count_grid=count_grid,
        )

    # ------------------------------------------------------------------
    # Filtered series (static)
    # ------------------------------------------------------------------

    @staticmethod
    def filtered_series(
        values: np.ndarray, method: str = "ewma", **kwargs: Any
    ) -> np.ndarray:
        """Apply a streaming filter to *values* and return smoothed output.

        Parameters
        ----------
        values : np.ndarray
            Input array.
        method : ``"ewma"`` or ``"median"``
        **kwargs
            Forwarded to the accumulator constructor (e.g. ``half_life``, ``window``).

        Returns
        -------
        np.ndarray
            Same-length smoothed array.
        """
        arr = np.asarray(values, dtype=np.float64).ravel()
        out = np.empty_like(arr)

        if method == "ewma":
            acc = EWMAAccumulator(**kwargs)
            for i, x in enumerate(arr):
                acc.update(float(x))
                out[i] = acc.mean
        elif method == "median":
            acc = RunningMedianFilter(**kwargs)
            for i, x in enumerate(arr):
                acc.update(float(x))
                out[i] = acc.median
        else:
            msg = f"Unknown method {method!r}, expected 'ewma' or 'median'"
            raise ValueError(msg)

        return out
