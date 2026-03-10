"""Shared utilities for audit runners."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import xarray as xr

from canvod.audit.core import ComparisonResult


@dataclass
class AuditResult:
    """Collects results from multiple comparisons into one object.

    Attributes
    ----------
    results : dict[str, ComparisonResult]
        Named comparison results. Keys are descriptive names like
        ``"rinex_canopy_01"`` or ``"vod_l2_reference_01_canopy_01"``.

    Examples
    --------
    ::

        audit = AuditResult()
        audit.results["my_comparison"] = compare_datasets(ds_a, ds_b, ...)

        print(audit.passed)    # True if everything passed
        print(audit.summary()) # Human-readable report
        df = audit.to_polars() # All stats in one table
    """

    results: dict[str, ComparisonResult] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        """True if every comparison passed."""
        return all(r.passed for r in self.results.values())

    @property
    def n_passed(self) -> int:
        return sum(1 for r in self.results.values() if r.passed)

    @property
    def n_total(self) -> int:
        return len(self.results)

    def summary(self) -> str:
        """Print a human-readable report of all comparisons."""
        lines = [
            f"Audit: {self.n_passed}/{self.n_total} passed",
            "=" * 60,
        ]
        for r in self.results.values():
            status = "PASS" if r.passed else "FAIL"
            lines.append(f"[{status}] {r.label}")
            if r.failures:
                for var, reason in r.failures.items():
                    lines.append(f"       {var}: {reason}")
        return "\n".join(lines)

    def to_polars(self):
        """All per-variable stats across all comparisons as one table."""
        import polars as pl

        frames = []
        for name, r in self.results.items():
            df = r.to_polars()
            if not df.is_empty():
                frames.append(df.with_columns(pl.lit(name).alias("comparison")))
        if not frames:
            return pl.DataFrame()
        return pl.concat(frames)


def open_store(path):
    """Open an Icechunk store from a path.

    Accepts a ``Path``, a ``str``, or an already-opened ``MyIcechunkStore``
    (in which case it is returned as-is).
    """
    if isinstance(path, (str, Path)):
        from canvod.store import MyIcechunkStore

        return MyIcechunkStore(store_path=Path(path))
    return path


def load_group(store, group: str) -> xr.Dataset:
    """Read a group from a store into memory, removing duplicate epochs.

    Duplicate epochs happen when daily and sub-daily files overlap
    (e.g. a daily concatenation file + 96 x 15-min files for the same day).
    """
    ds = store.read_group(group)
    ds = ds.load()

    # Remove duplicate epochs (keep first occurrence)
    _, unique_idx = np.unique(ds.epoch.values, return_index=True)
    if len(unique_idx) < len(ds.epoch):
        n_dupes = len(ds.epoch) - len(unique_idx)
        print(f"  {group}: removed {n_dupes} duplicate epochs")
        ds = ds.isel(epoch=np.sort(unique_idx))

    return ds


def find_shared_groups(*stores) -> list[str]:
    """Find group names that exist in all given stores."""
    groups = set(stores[0].list_groups())
    for s in stores[1:]:
        groups &= set(s.list_groups())
    return sorted(groups)
