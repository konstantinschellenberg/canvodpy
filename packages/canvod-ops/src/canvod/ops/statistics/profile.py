"""AccumulatorSet and ProfileRegistry — bundles and indexes streaming accumulators."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np

from canvod.streamstats import (
    DEFAULT_AUTOCOVARIANCE_MAX_LAG,
    DEFAULT_EWMA_HALFLIFE,
    DEFAULT_HISTOGRAM_BINS,
    CellSignalKey,
    EWMAAccumulator,
    GKSketch,
    S4Accumulator,
    StreamingAutocovariance,
    StreamingHistogram,
    WelfordAccumulator,
)


@dataclass
class AccumulatorSet:
    """Bundle of streaming accumulators for a single CellSignalKey.

    Core (always active):
    - welford: mean, variance, skewness, kurtosis, min, max
    - gk: streaming quantiles (ε-approximate)

    Optional (enabled via ProfileRegistry config):
    - histogram: fixed-bin distribution
    - autocovariance: lag covariance for effective sample size
    - ewma: exponentially weighted moving average for non-stationary tracking
    - s4: amplitude scintillation index (only for SNR variables)
    """

    welford: WelfordAccumulator = field(default_factory=WelfordAccumulator)
    gk: GKSketch = field(default_factory=lambda: GKSketch(epsilon=0.01))
    histogram: StreamingHistogram | None = None
    autocovariance: StreamingAutocovariance | None = None
    ewma: EWMAAccumulator | None = None
    s4: S4Accumulator | None = None

    def update(self, x: float) -> None:
        """Update all accumulators with a single value."""
        self.welford.update(x)
        self.gk.update(x)
        if self.histogram is not None:
            self.histogram.update(x)
        if self.autocovariance is not None:
            self.autocovariance.update(x)
        if self.ewma is not None:
            self.ewma.update(x)
        if self.s4 is not None:
            self.s4.update(x)

    def update_batch(self, values: np.ndarray) -> None:
        """Update all accumulators with an array of values."""
        self.welford.update_batch(values)
        self.gk.update_batch(values)
        if self.histogram is not None:
            self.histogram.update_batch(values)
        if self.autocovariance is not None:
            self.autocovariance.update_batch(values)
        if self.ewma is not None:
            self.ewma.update_batch(values)
        if self.s4 is not None:
            self.s4.update_batch(values)

    def merge(self, other: AccumulatorSet) -> AccumulatorSet:
        """Merge another set into this one. Returns self."""
        self.welford.merge(other.welford)
        self.gk.merge(other.gk)
        if self.histogram is not None and other.histogram is not None:
            self.histogram.merge(other.histogram)
        if self.autocovariance is not None and other.autocovariance is not None:
            self.autocovariance.merge(other.autocovariance)
        if self.ewma is not None and other.ewma is not None:
            self.ewma.merge(other.ewma)
        if self.s4 is not None and other.s4 is not None:
            self.s4.merge(other.s4)
        return self


# Variables for which S4 scintillation tracking is meaningful
_S4_VARIABLES = {"cn0", "snr", "SNR", "C/N0"}


class ProfileRegistry:
    """Dictionary of AccumulatorSets indexed by CellSignalKey.

    Lazy-creates accumulators with variable-specific histogram bins.
    """

    def __init__(
        self,
        gk_epsilon: float = 0.01,
        autocovariance_enabled: bool = False,
        autocovariance_max_lag: int = DEFAULT_AUTOCOVARIANCE_MAX_LAG,
        ewma_enabled: bool = True,
        ewma_halflife: float = DEFAULT_EWMA_HALFLIFE,
        s4_enabled: bool = True,
    ) -> None:
        self._accumulators: dict[CellSignalKey, AccumulatorSet] = {}
        self._gk_epsilon = gk_epsilon
        self._autocovariance_enabled = autocovariance_enabled
        self._autocovariance_max_lag = autocovariance_max_lag
        self._ewma_enabled = ewma_enabled
        self._ewma_halflife = ewma_halflife
        self._s4_enabled = s4_enabled

    def get_or_create(self, key: CellSignalKey) -> AccumulatorSet:
        """Return the accumulator set for *key*, creating if needed."""
        if key not in self._accumulators:
            hist_spec = DEFAULT_HISTOGRAM_BINS.get(key.variable)
            histogram = (
                StreamingHistogram(*hist_spec) if hist_spec is not None else None
            )
            autocovariance = (
                StreamingAutocovariance(max_lag=self._autocovariance_max_lag)
                if self._autocovariance_enabled
                else None
            )
            ewma = (
                EWMAAccumulator(half_life=self._ewma_halflife)
                if self._ewma_enabled
                else None
            )
            s4 = (
                S4Accumulator()
                if self._s4_enabled and key.variable in _S4_VARIABLES
                else None
            )
            self._accumulators[key] = AccumulatorSet(
                welford=WelfordAccumulator(),
                gk=GKSketch(epsilon=self._gk_epsilon),
                histogram=histogram,
                autocovariance=autocovariance,
                ewma=ewma,
                s4=s4,
            )
        return self._accumulators[key]

    def update(self, key: CellSignalKey, value: float) -> None:
        """Update the accumulator set for *key* with a single value."""
        self.get_or_create(key).update(value)

    def update_batch(self, key: CellSignalKey, values: np.ndarray) -> None:
        """Update the accumulator set for *key* with an array of values."""
        self.get_or_create(key).update_batch(values)

    def merge(self, other: ProfileRegistry) -> ProfileRegistry:
        """Merge another registry into this one. Returns self."""
        for key, acc_set in other._accumulators.items():
            if key in self._accumulators:
                self._accumulators[key].merge(acc_set)
            else:
                self._accumulators[key] = acc_set
        return self

    def keys(self):
        return self._accumulators.keys()

    def items(self):
        return self._accumulators.items()

    def __len__(self) -> int:
        return len(self._accumulators)

    def __getitem__(self, key: CellSignalKey) -> AccumulatorSet:
        return self._accumulators[key]

    def __contains__(self, key: CellSignalKey) -> bool:
        return key in self._accumulators

    def summary(self) -> dict[str, Any]:
        """Return a summary dict suitable for logging."""
        if not self._accumulators:
            return {"n_keys": 0}
        total_obs = sum(a.welford.count for a in self._accumulators.values())
        variables = sorted({k.variable for k in self._accumulators})
        cell_ids = sorted({k.cell_id for k in self._accumulators})
        return {
            "n_keys": len(self._accumulators),
            "total_observations": total_obs,
            "variables": variables,
            "n_cells": len(cell_ids),
            "gk_epsilon": self._gk_epsilon,
            "autocovariance_enabled": self._autocovariance_enabled,
            "ewma_enabled": self._ewma_enabled,
            "s4_enabled": self._s4_enabled,
        }
