"""Manual backfill DAG for reprocessing historical GNSS-T data.

Triggered manually with parameters. Processes a date range sequentially,
reusing the same task functions as the daily DAGs.

Usage (Airflow UI or CLI)::

    airflow dags trigger canvod_backfill --conf '{
        "site": "Rosalia",
        "branch": "sbf",
        "start_date": "2025-001",
        "end_date": "2025-010"
    }'

Or via ``af``::

    af runs trigger canvod_backfill \\
        -F site=Rosalia -F branch=sbf \\
        -F start_date=2025-001 -F end_date=2025-010
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param

logger = logging.getLogger(__name__)


def _task_failure_callback(context):
    """Log structured failure info."""
    ti = context["task_instance"]
    logger.error(
        "BACKFILL TASK FAILED | dag=%s task=%s date=%s error=%s",
        ti.dag_id,
        ti.task_id,
        context.get("ds", "?"),
        context.get("exception", "unknown"),
    )


@dag(
    dag_id="canvod_backfill",
    schedule=None,  # manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "canvod",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(hours=1),
        "execution_timeout": timedelta(hours=6),
        "on_failure_callback": _task_failure_callback,
    },
    tags=["canvod", "gnss", "backfill"],
    params={
        "site": Param(
            "Rosalia", type="string", description="Site name from sites.yaml"
        ),
        "branch": Param(
            "sbf",
            type="string",
            enum=["sbf", "rinex"],
            description="Processing branch: sbf (broadcast) or rinex (agency SP3/CLK)",
        ),
        "start_date": Param(
            "2025-001",
            type="string",
            description="Start date in YYYYDDD format",
        ),
        "end_date": Param(
            "2025-010",
            type="string",
            description="End date in YYYYDDD format (inclusive)",
        ),
    },
    doc_md=__doc__,
)
def canvod_backfill():
    """Process a date range for a single site and branch."""

    @task
    def t_resolve_dates(**context) -> list[str]:
        """Expand start_date..end_date into a list of YYYYDDD strings."""
        import datetime as dt

        from canvod.utils.tools import YYYYDOY

        params = context["params"]
        start = YYYYDOY.from_str(params["start_date"])
        end = YYYYDOY.from_str(params["end_date"])

        dates: list[str] = []
        current = start.date
        end_date = end.date
        while current <= end_date:
            doy = (current - dt.date(current.year, 1, 1)).days + 1
            dates.append(f"{current.year}{doy:03d}")
            current += dt.timedelta(days=1)

        logger.info(
            "backfill: %s %s — %d days from %s to %s",
            params["site"],
            params["branch"],
            len(dates),
            params["start_date"],
            params["end_date"],
        )
        return dates

    @task(execution_timeout=timedelta(hours=48))
    def t_process_date_range(dates: list[str], **context) -> dict:
        """Process each date sequentially.

        Sequential within task to prevent concurrent store writes
        (Icechunk max_active_runs=1). Each date runs the full ingest +
        analysis pipeline. Per-date errors are caught and logged —
        processing continues to the next date. Idempotent: already-
        processed dates are skipped by hash dedup in the store layer.
        """
        params = context["params"]
        site = params["site"]
        branch = params["branch"]

        results: dict[str, str] = {}

        for yyyydoy in dates:
            try:
                if branch == "sbf":
                    _process_single_day_sbf(site, yyyydoy)
                else:
                    _process_single_day_rinex(site, yyyydoy)
                results[yyyydoy] = "ok"
                logger.info("backfill: %s %s %s — ok", site, branch, yyyydoy)
            except Exception as exc:
                results[yyyydoy] = f"error: {exc}"
                logger.exception("backfill: %s %s %s — failed", site, branch, yyyydoy)

        n_ok = sum(1 for v in results.values() if v == "ok")
        n_fail = len(results) - n_ok
        logger.info(
            "backfill complete: %d ok, %d failed out of %d",
            n_ok,
            n_fail,
            len(results),
        )

        return {
            "site": site,
            "branch": branch,
            "total": len(results),
            "ok": n_ok,
            "failed": n_fail,
            "details": results,
        }

    dates = t_resolve_dates()
    t_process_date_range(dates=dates)


def _process_single_day_sbf(site: str, yyyydoy: str) -> None:
    """Run the full SBF pipeline for a single day."""
    from canvodpy.workflows.tasks import (
        calculate_vod,
        check_sbf,
        cleanup,
        detect_anomalies,
        detect_changepoints,
        process_sbf,
        snapshot_statistics,
        update_climatology,
        update_statistics,
        validate_ingest,
    )

    sbf_info = check_sbf(site, yyyydoy)
    process_sbf(site, yyyydoy, receiver_files=sbf_info["receivers"])
    validate_ingest(site, yyyydoy)
    calculate_vod(site, yyyydoy)
    update_statistics(site, yyyydoy)
    update_climatology(site, yyyydoy)
    detect_anomalies(site, yyyydoy)
    detect_changepoints(site, yyyydoy)
    snapshot_statistics(site, yyyydoy)
    cleanup(site, yyyydoy)


def _process_single_day_rinex(site: str, yyyydoy: str) -> None:
    """Run the full RINEX pipeline for a single day."""
    from canvodpy.workflows.tasks import (
        calculate_vod,
        check_rinex,
        cleanup,
        detect_anomalies,
        detect_changepoints,
        fetch_aux_data,
        process_rinex,
        snapshot_statistics,
        update_climatology,
        update_statistics,
        validate_ingest,
    )

    rinex_info = check_rinex(site, yyyydoy)
    aux_info = fetch_aux_data(site, yyyydoy)
    process_rinex(
        site,
        yyyydoy,
        aux_zarr_path=aux_info["aux_zarr_path"],
        receiver_files=rinex_info["receivers"],
    )
    validate_ingest(site, yyyydoy)
    calculate_vod(site, yyyydoy)
    update_statistics(site, yyyydoy)
    update_climatology(site, yyyydoy)
    detect_anomalies(site, yyyydoy)
    detect_changepoints(site, yyyydoy)
    snapshot_statistics(site, yyyydoy)
    cleanup(site, yyyydoy)


# Instantiate
canvod_backfill()
