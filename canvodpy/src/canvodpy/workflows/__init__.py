"""Workflow definitions for automation (Airflow, Prefect, etc.)."""

from canvodpy.workflows.tasks import (
    calculate_vod,
    check_rinex,
    detect_anomalies,
    detect_changepoints,
    fetch_aux_data,
    parse_sampling_interval_from_filename,
    process_rinex,
    snapshot_statistics,
    update_climatology,
    update_statistics,
)

__all__ = [
    "calculate_vod",
    "check_rinex",
    "detect_anomalies",
    "detect_changepoints",
    "fetch_aux_data",
    "parse_sampling_interval_from_filename",
    "process_rinex",
    "snapshot_statistics",
    "update_climatology",
    "update_statistics",
]
