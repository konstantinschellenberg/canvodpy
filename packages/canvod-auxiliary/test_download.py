#!/usr/bin/env python3
"""Test download with fixed FTP paths."""

import datetime
import tempfile
from pathlib import Path

from canvod.auxiliary.ephemeris.reader import Sp3File


def main():
    temp_dir = Path(tempfile.gettempdir()) / "canvod_test"
    temp_dir.mkdir(exist_ok=True)

    print("Testing with corrected ESA FTP path...")
    print(f"Directory: {temp_dir}\n")

    try:
        sp3_file = Sp3File.from_datetime_date(
            date=datetime.date(2023, 9, 11),
            agency="COD",
            product_type="final",
            ftp_server="ftp://gssc.esa.int/gnss",
            local_dir=temp_dir,
        )

        print("✓ Success!")
        print(f"  File: {sp3_file.fpath}")
        file_path = sp3_file.fpath
        if file_path is not None:
            print(f"  Size: {file_path.stat().st_size / 1024:.1f} KB")

        # Test data access
        ds = sp3_file.data
        print("\n✓ Dataset loaded")
        print(f"  Dimensions: {dict(ds.sizes)}")
        print(f"  Satellites: {len(ds.sv)}")

    except Exception as e:
        print(f"✗ Failed: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
