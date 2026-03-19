"""Dataset quality diagnostics for xarray Datasets.

Inspects shape, NaN ratios, epoch gaps, and size. Use after each
processing step to catch silent data loss.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from canvod.utils.diagnostics._store import record


@dataclass
class DatasetReport:
    """Diagnostics snapshot of an xarray Dataset."""

    operation: str
    n_epochs: int = 0
    n_sids: int = 0
    variables: list[str] = field(default_factory=list)
    nan_ratios: dict[str, float] = field(default_factory=dict)
    epoch_gaps: list[str] = field(default_factory=list)
    size_mb: float = 0.0
    extras: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        """Return report as a flat dict."""
        return {
            "operation": self.operation,
            "n_epochs": self.n_epochs,
            "n_sids": self.n_sids,
            "n_variables": len(self.variables),
            "size_mb": round(self.size_mb, 2),
            "nan_ratios": self.nan_ratios,
            "epoch_gaps": self.epoch_gaps,
            **self.extras,
        }


def _inspect_dataset(ds: Any, operation: str, **extras: Any) -> DatasetReport:
    """Build a DatasetReport from an xarray Dataset."""
    import numpy as np

    report = DatasetReport(operation=operation, extras=extras)

    report.n_epochs = len(ds.epoch) if "epoch" in ds.dims else 0
    report.n_sids = len(ds.sid) if "sid" in ds.dims else 0
    report.variables = list(ds.data_vars)
    report.size_mb = ds.nbytes / (1024 * 1024)

    # NaN ratios for float variables
    for var in ds.data_vars:
        arr = ds[var]
        if np.issubdtype(arr.dtype, np.floating):
            total = arr.size
            if total > 0:
                n_nan = int(np.isnan(arr.values).sum()) if not arr.chunks else 0
                report.nan_ratios[var] = round(n_nan / total, 4) if total else 0.0

    # Epoch gap detection
    if "epoch" in ds.dims and report.n_epochs > 1:
        epochs = ds.epoch.values
        if np.issubdtype(epochs.dtype, np.datetime64):
            diffs = np.diff(epochs)
            median_dt = np.median(diffs)
            if median_dt > np.timedelta64(0):
                threshold = median_dt * 3
                gap_indices = np.where(diffs > threshold)[0]
                for idx in gap_indices[:10]:  # cap at 10 gaps
                    report.epoch_gaps.append(
                        f"{epochs[idx]} → {epochs[idx + 1]} "
                        f"(gap={diffs[idx] / np.timedelta64(1, 's'):.0f}s, "
                        f"expected≈{median_dt / np.timedelta64(1, 's'):.0f}s)"
                    )

    return report


def track_dataset(
    operation: str,
    ds: Any,
    *,
    log: bool = True,
    warn_nan_threshold: float = 0.5,
    **extras: Any,
) -> DatasetReport:
    """Inspect an xarray Dataset and record diagnostics.

    Logs shape, NaN ratios, epoch gaps, and size. Use after each
    processing step to catch silent data loss.

    Parameters
    ----------
    operation : str
        Processing step name (e.g. "after_read", "after_augment").
    ds : xarray.Dataset
        The dataset to inspect.
    log : bool
        Emit structlog messages.
    warn_nan_threshold : float
        Log a warning if any variable exceeds this NaN ratio (0-1).
    **extras
        Additional key-value pairs stored with the record.

    Returns
    -------
    DatasetReport
        Diagnostics snapshot.

    Examples
    --------
    ::

        ds = reader.to_ds()
        report = track_dataset("after_read", ds)

        ds = augment_with_ephemeris(ds, sp3, clk)
        report = track_dataset("after_augment", ds)
        # warns if NaN ratio jumped (e.g. missing SP3 data)

    In an Airflow pipeline::

        @task
        def process_day(site, date):
            ds = read_rinex(...)
            track_dataset("read", ds, site=site, date=str(date))
            ds = augment(ds)
            track_dataset("augment", ds, site=site, date=str(date))
    """
    report = _inspect_dataset(ds, operation, **extras)

    record(
        operation,
        0.0,
        metric_type="dataset",
        n_epochs=report.n_epochs,
        n_sids=report.n_sids,
        n_variables=len(report.variables),
        size_mb=round(report.size_mb, 2),
        n_epoch_gaps=len(report.epoch_gaps),
        **extras,
    )

    if log:
        log_extras: dict[str, Any] = {
            "n_epochs": report.n_epochs,
            "n_sids": report.n_sids,
            "n_vars": len(report.variables),
            "size_mb": round(report.size_mb, 2),
            **extras,
        }

        if report.epoch_gaps:
            log_extras["epoch_gaps"] = len(report.epoch_gaps)

        high_nan = {
            k: v for k, v in report.nan_ratios.items() if v > warn_nan_threshold
        }
        if high_nan:
            log_extras["high_nan_vars"] = high_nan

        try:
            import structlog

            logger = structlog.get_logger(__name__)
            if high_nan or report.epoch_gaps:
                logger.warning("dataset_quality", operation=operation, **log_extras)
            else:
                logger.info("dataset_ok", operation=operation, **log_extras)
        except ImportError:
            pass

    return report
