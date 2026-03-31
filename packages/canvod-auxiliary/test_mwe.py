"""Minimal working example for canvod-aux package."""

import datetime
import tempfile
from pathlib import Path

from canvod.auxiliary.clock.reader import ClkFile
from canvod.auxiliary.ephemeris.reader import Sp3File


def main():
    # Setup temporary directory
    temp_dir = Path(tempfile.gettempdir()) / "canvod_mwe"
    temp_dir.mkdir(exist_ok=True)
    print(f"Using directory: {temp_dir}\n")

    # Example 1: Download SP3 file using from_datetime_date
    print("=" * 60)
    print("Example 1: SP3 File - from_datetime_date()")
    print("=" * 60)

    try:
        sp3_file = Sp3File.from_datetime_date(
            date=datetime.date(2023, 9, 11),  # Day 254 of 2023
            agency="COD",
            product_type="final",
            ftp_server="ftp://gssc.esa.int/gnss",
            local_dir=temp_dir,
        )

        print(f"✓ File path: {sp3_file.fpath}")
        print(f"✓ Agency: {sp3_file.agency}")
        print(f"✓ Product: {sp3_file.product_type}")

        # Access data (lazy loads)
        ds = sp3_file.data
        print(f"\nDataset dimensions: {dict(ds.sizes)}")
        print(f"Variables: {list(ds.data_vars)}")
        print(f"Time range: {ds.epoch.values[0]} to {ds.epoch.values[-1]}")
        print(f"Satellites: {len(ds.sv)} ({list(ds.sv.values[:5])}...)")

    except Exception as e:
        print(f"✗ Failed: {e}")
        print("Tip: Some files may not be available on all servers")

    # Example 2: Read existing file with from_file
    print("\n" + "=" * 60)
    print("Example 2: SP3 File - from_file() with velocities")
    print("=" * 60)

    if sp3_file.fpath is not None and sp3_file.fpath.exists():
        sp3_from_file = Sp3File.from_file(
            sp3_file.fpath,
            add_velocities=True,
            dimensionless=True,
        )

        ds2 = sp3_from_file.data
        print(f"✓ Variables (with velocities): {list(ds2.data_vars)}")
        print(f"✓ Has velocities: {'Vx' in ds2.data_vars}")

        if "Vx" in ds2.data_vars:
            import numpy as np

            v_mag = np.sqrt(ds2.Vx**2 + ds2.Vy**2 + ds2.Vz**2)
            print(f"✓ Mean velocity magnitude: {float(v_mag.mean()):.2f} m/s")
    else:
        print("✗ Skipping - file not available")

    # Example 3: CLK file
    print("\n" + "=" * 60)
    print("Example 3: CLK File - from_datetime_date()")
    print("=" * 60)

    try:
        clk_file = ClkFile.from_datetime_date(
            date=datetime.date(2023, 9, 11),
            agency="COD",
            product_type="final",
            ftp_server="ftp://gssc.esa.int/gnss",
            local_dir=temp_dir,
        )

        print(f"✓ File path: {clk_file.fpath}")
        clk_ds = clk_file.data
        print(f"✓ Dataset dimensions: {dict(clk_ds.sizes)}")
        print(f"✓ Variables: {list(clk_ds.data_vars)}")
        print(
            f"✓ Clock offset range: {float(clk_ds.clock_offset.min()):.6e}"
            f" to {float(clk_ds.clock_offset.max()):.6e} seconds"
        )

    except Exception as e:
        print(f"✗ Failed: {e}")

    # Example 4: Different agencies
    print("\n" + "=" * 60)
    print("Example 4: Different Agencies")
    print("=" * 60)

    for agency in ["GFZ", "ESA"]:
        try:
            sp3 = Sp3File.from_datetime_date(
                date=datetime.date(2023, 9, 11),
                agency=agency,
                product_type="final",
                ftp_server="ftp://gssc.esa.int/gnss",
                local_dir=temp_dir,
            )
            if sp3.fpath is not None:
                print(f"✓ {agency}: {sp3.fpath.name}")
            else:
                print(f"✓ {agency}: (no local file path)")
        except Exception as e:
            print(f"✗ {agency}: {str(e)[:80]}...")

    print("\n" + "=" * 60)
    print("MWE completed!")
    print("=" * 60)
    print(f"\nDownloaded files are in: {temp_dir}")


if __name__ == "__main__":
    main()
