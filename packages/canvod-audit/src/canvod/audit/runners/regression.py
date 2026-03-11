"""Tier 2: Regression testing against frozen checkpoints.

Freeze a known-good output as a NetCDF file, then compare future outputs
against it after code changes. If the output changes when it shouldn't,
the checkpoint catches it.

Usage::

    from canvod.audit.runners import freeze_checkpoint, audit_regression

    # Step 1: Freeze a known-good output (do this once)
    freeze_checkpoint(
        store="/path/to/store",
        group="canopy_01",
        output_dir="checkpoints/",
        version="0.3.0",
    )

    # Step 2: After code changes, check against the checkpoint
    result = audit_regression(
        store="/path/to/store",
        checkpoint_dir="checkpoints/",
    )

    print(result.summary())
"""

from __future__ import annotations

from pathlib import Path

import xarray as xr

from canvod.audit.core import compare_datasets
from canvod.audit.runners.common import AuditResult, load_group, open_store
from canvod.audit.tolerances import ToleranceTier


def freeze_checkpoint(
    store,
    group,
    output_dir,
    *,
    version="",
    metadata=None,
):
    """Save a store group as a NetCDF checkpoint file.

    Parameters
    ----------
    store : str or Path
        Path to the Icechunk store.
    group : str
        Group to freeze (e.g. "canopy_01", "reference_01_canopy_01").
    output_dir : str or Path
        Directory to write the checkpoint into. Created if it doesn't exist.
    version : str, optional
        Version label (e.g. "0.3.0"). Included in the filename and metadata.
    metadata : dict, optional
        Extra metadata to store as dataset attributes (git hash, date, notes).

    Returns
    -------
    Path
        Path to the written checkpoint file.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    s = open_store(store)
    ds = load_group(s, group)

    # Build checkpoint filename
    parts = [group]
    if version:
        parts.append(f"v{version}")
    filename = "_".join(parts) + ".nc"
    output_path = output_dir / filename

    # Attach metadata as dataset attributes
    checkpoint_attrs = {"checkpoint_group": group}
    if version:
        checkpoint_attrs["checkpoint_version"] = version
    if metadata:
        for k, v in metadata.items():
            checkpoint_attrs[f"checkpoint_{k}"] = str(v)
    ds = ds.assign_attrs(**checkpoint_attrs)

    ds.to_netcdf(output_path)
    print(f"Checkpoint written: {output_path}")
    print(
        f"  {len(ds.epoch)} epochs, {len(ds.sid)} sids, {len(ds.data_vars)} variables"
    )
    return output_path


def audit_regression(
    store,
    checkpoint_dir,
    *,
    groups=None,
    variables=None,
    tier=ToleranceTier.EXACT,
):
    """Compare current store output against frozen checkpoints.

    Looks for ``*.nc`` files in ``checkpoint_dir``. For each checkpoint,
    reads the corresponding group from the store and compares.

    Parameters
    ----------
    store : str or Path
        Path to the current Icechunk store.
    checkpoint_dir : str or Path
        Directory containing checkpoint ``.nc`` files.
    groups : list of str, optional
        Only compare these groups. If not given, compares all checkpoints found.
    variables : list of str, optional
        Which variables to compare. If not given, compares all shared.
    tier : ToleranceTier
        Comparison strictness. Default is EXACT — regressions should produce
        bit-identical output unless you intentionally changed the algorithm.

    Returns
    -------
    AuditResult
    """
    checkpoint_dir = Path(checkpoint_dir)
    checkpoints = sorted(checkpoint_dir.glob("*.nc"))

    if not checkpoints:
        print(f"No checkpoints found in {checkpoint_dir}")
        return AuditResult()

    print(f"Found {len(checkpoints)} checkpoints in {checkpoint_dir}")

    s = open_store(store)
    available_groups = set(s.list_groups())
    result = AuditResult()

    for cp_path in checkpoints:
        ds_ref = xr.open_dataset(cp_path).load()

        # Figure out which group this checkpoint belongs to
        group = ds_ref.attrs.get("checkpoint_group", cp_path.stem.split("_v")[0])

        if groups is not None and group not in groups:
            continue

        if group not in available_groups:
            print(f"  {cp_path.name}: group '{group}' not in store, skipping")
            continue

        print(f"Checking {cp_path.name} against store group '{group}' ...")
        ds_current = load_group(s, group)

        version = ds_ref.attrs.get("checkpoint_version", "?")

        r = compare_datasets(
            ds_current,
            ds_ref,
            variables=variables,
            tier=tier,
            label=f"{group}: current vs checkpoint (v{version})",
        )
        result.results[f"regression_{group}"] = r

    print()
    print(result.summary())
    return result
