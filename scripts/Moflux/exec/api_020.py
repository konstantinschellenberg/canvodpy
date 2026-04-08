#!/usr/bin/env python
from datetime import datetime

from canvodpy import Site

site = Site("moflux")


def run_functional():
    from canvodpy.functional import read_rinex

    site_canopy_path = (
        "/mnt/storage/GNSS/rawdata/MOFLUX/RINEX/tower/25005/SEPT005a.25.obs"
    )
    site_reference_path = (
        "/mnt/storage/GNSS/rawdata/MOFLUX/RINEX/subcanopy/25005/SEPT005a.25.obs"
    )

    # Use L4 for explicit step control
    ds_c = read_rinex(site_canopy_path, reader="rinex2")
    ds_r = read_rinex(site_reference_path, reader="rinex2")


def run_pipeline():
    # Stage 1: nightly ingestion (runs via cron or Airflow)
    with site.pipeline(n_workers=1) as pipe:
        data = pipe.process_date("2024005")

    # Stage 2: weekly VOD computation (triggered separately)
    result = site.vod.compute_bulk(
        "main",
        start=datetime(2025, 1, 4),
        end=datetime(2025, 2, 6),
        write=True,  # Write to VOD store
    )


if __name__ == "__main__":
    run_pipeline()
