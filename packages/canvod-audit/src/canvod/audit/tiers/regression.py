"""Regression verification against frozen reference outputs.

Freeze a known-good output as a checkpoint, then compare future
outputs against it to detect regressions.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import xarray as xr

from canvod.audit.core import ComparisonResult, compare_datasets
from canvod.audit.tolerances import ToleranceTier


def freeze_checkpoint(
    ds: Any,
    output_path: Path,
    *,
    metadata: dict[str, Any] | None = None,
) -> Path:
    """Save a dataset as a NetCDF checkpoint for future regression testing.

    Parameters
    ----------
    ds : xarray.Dataset
        The known-good reference output.
    output_path : Path
        Where to write the checkpoint file (.nc).
    metadata : dict, optional
        Metadata to store as dataset attributes (git hash, date, config).

    Returns
    -------
    Path
        The written checkpoint path.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if metadata:
        ds = ds.assign_attrs(**{f"checkpoint_{k}": str(v) for k, v in metadata.items()})

    ds.to_netcdf(output_path)
    return output_path


def compare_against_checkpoint(
    ds: Any,
    checkpoint_path: Path,
    *,
    variables: list[str] | None = None,
    tier: ToleranceTier = ToleranceTier.EXACT,
    label: str = "",
) -> ComparisonResult:
    """Compare a dataset against a frozen checkpoint.

    Parameters
    ----------
    ds : xarray.Dataset
        Current output to verify.
    checkpoint_path : Path
        Path to the reference checkpoint (.nc file).
    variables : list[str], optional
        Variables to compare. Defaults to intersection.
    tier : ToleranceTier
        Default EXACT — regressions should produce identical output.
    label : str
        Comparison label.

    Returns
    -------
    ComparisonResult
    """
    checkpoint_path = Path(checkpoint_path)
    ds_ref = xr.open_dataset(checkpoint_path)

    return compare_datasets(
        ds,
        ds_ref,
        variables=variables,
        tier=tier,
        label=label or f"Regression check vs {checkpoint_path.name}",
        metadata={
            "comparison_type": "regression",
            "checkpoint_path": str(checkpoint_path),
        },
    )
