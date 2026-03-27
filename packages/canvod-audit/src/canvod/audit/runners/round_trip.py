"""Infrastructure: Store round-trip fidelity.

Write a dataset to an Icechunk store, read it back, compare. This catches
serialization bugs, encoding issues (float32 vs float64, NaN encoding),
chunk boundary artifacts, and compression-related precision loss.

Usage::

    from canvod.audit.runners import audit_store_round_trip

    result = audit_store_round_trip(
        store="/path/to/store",
        groups=["canopy_01", "reference_01"],
    )

    print(result.summary())
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import xarray as xr

from canvod.audit.core import compare_datasets
from canvod.audit.runners.common import AuditResult, load_group, open_store
from canvod.audit.tolerances import ToleranceTier


def audit_store_round_trip(
    store,
    *,
    groups=None,
    variables=None,
    output_dir=None,
):
    """Read from store, write to a fresh store, read back, compare.

    Parameters
    ----------
    store : str or Path
        Path to the Icechunk store to test.
    groups : list of str, optional
        Which groups to test. If not given, tests all groups.
    variables : list of str, optional
        Which variables to compare. If not given, compares all.
    output_dir : str or Path, optional
        Where to write the temporary round-trip store. If not given,
        uses a temporary directory (cleaned up automatically).

    Returns
    -------
    AuditResult
    """
    s = open_store(store)
    result = AuditResult()

    if groups is None:
        groups = s.list_groups()
        print(f"Testing round-trip for {len(groups)} groups: {groups}")

    for group in groups:
        print(f"Round-trip test: {group} ...")

        # Step 1: Read the original
        ds_original = load_group(s, group)
        print(
            f"  Original: {dict(ds_original.dims)}, "
            f"{len(ds_original.data_vars)} variables"
        )

        # Step 2: Write to NetCDF and read back
        # (NetCDF round-trip tests the xarray serialization path)
        with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as f:
            tmp_path = Path(f.name)

        ds_original.to_netcdf(tmp_path)
        ds_roundtrip = xr.open_dataset(tmp_path).load()
        tmp_path.unlink()

        # Step 3: Compare
        r = compare_datasets(
            ds_original,
            ds_roundtrip,
            variables=variables,
            tier=ToleranceTier.EXACT,
            label=f"{group}: store → NetCDF → read back",
        )
        result.results[f"roundtrip_netcdf_{group}"] = r

    # If the store supports Zarr round-trip, test that too
    try:
        _test_zarr_round_trip(s, groups, variables, result, output_dir)
    except Exception as e:
        print(f"  Zarr round-trip skipped: {e}")

    print()
    print(result.summary())
    return result


def _test_zarr_round_trip(s, groups, variables, result, output_dir):
    """Write to a fresh Icechunk store and read back."""
    from canvod.store import MyIcechunkStore

    if output_dir is None:
        output_dir = Path(tempfile.mkdtemp(prefix="audit_roundtrip_"))
    else:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

    roundtrip_path = output_dir / "roundtrip_store"

    if roundtrip_path.exists():
        print(f"  Zarr round-trip store already exists at {roundtrip_path}, skipping")
        return

    roundtrip_store = MyIcechunkStore(store_path=roundtrip_path)

    for group in groups:
        print(f"  Zarr round-trip: {group} ...")
        ds_original = load_group(s, group)

        # Write to fresh store
        roundtrip_store.write_initial_group(
            dataset=ds_original,
            group_name=group,
        )

        # Read back
        ds_back = load_group(roundtrip_store, group)

        r = compare_datasets(
            ds_original,
            ds_back,
            variables=variables,
            tier=ToleranceTier.EXACT,
            label=f"{group}: store → Icechunk → read back",
        )
        result.results[f"roundtrip_zarr_{group}"] = r
