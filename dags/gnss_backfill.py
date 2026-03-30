"""Manual backfill DAG for reprocessing historical GNSS-T data.

Triggered manually with parameters.  Each date in the range becomes an
independent Airflow task instance via dynamic task mapping (Airflow 2.7+).
A failure on one date retries that date only — the rest of the batch
continues unaffected.

**Concurrency safety** — Icechunk stores require serialised commits on a
branch.  ``max_active_tis_per_dagrun=1`` enforces this within a single
backfill run.  The ``canvod_store_write`` pool (create it with one slot in
the Airflow UI) provides the same guarantee across simultaneous backfill
runs and the daily DAGs::

    airflow pools set canvod_store_write 1 "Serialise Icechunk commits"

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

from airflow.decorators import dag, task  # type: ignore[unresolved-import]
from airflow.models.param import Param  # type: ignore[unresolved-import]

logger = logging.getLogger(__name__)


def _task_failure_callback(context):
    """Log structured failure info."""
    ti = context["task_instance"]
    logger.error(
        "BACKFILL TASK FAILED | dag=%s task=%s map_index=%s error=%s",
        ti.dag_id,
        ti.task_id,
        ti.map_index,
        context.get("exception", "unknown"),
    )


@dag(
    dag_id="canvod_backfill",
    schedule=None,  # manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=3,  # allow a few concurrent backfill runs (pool controls writes)
    default_args={
        "owner": "canvod",
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(hours=1),
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
            enum=["sbf", "rinex", "sbf_agency"],
            description=(
                "Processing branch: "
                "sbf (broadcast ephemeris, same-day), "
                "rinex (agency SP3/CLK, ~12-18 day lag), "
                "sbf_agency (SBF observables + agency geometry)"
            ),
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

        if start.date is None:
            raise ValueError(f"Invalid start_date: {params['start_date']!r}")
        if end.date is None:
            raise ValueError(f"Invalid end_date: {params['end_date']!r}")

        dates: list[str] = []
        current = start.date
        while current <= end.date:
            doy = (current - dt.date(current.year, 1, 1)).days + 1
            dates.append(f"{current.year}{doy:03d}")
            current += dt.timedelta(days=1)

        logger.info(
            "backfill: %s/%s — %d days (%s → %s)",
            params["site"],
            params["branch"],
            len(dates),
            params["start_date"],
            params["end_date"],
        )
        return dates

    @task(
        execution_timeout=timedelta(hours=6),
        # Serialise commits within this DAG run.  One date at a time prevents
        # concurrent Icechunk writes to the same branch.
        max_active_tis_per_dagrun=1,
        # Optional pool for cross-run serialisation (create with slot=1 in UI).
        pool="canvod_store_write",
        pool_slots=1,
    )
    def t_process_day(yyyydoy: str, **context) -> dict:
        """Process the full ingest + VOD pipeline for a single date.

        Idempotent: already-processed dates are skipped by the store's
        three-layer dedup (hash match → temporal overlap → intra-batch).
        """
        params = context["params"]
        site = params["site"]
        branch = params["branch"]

        if branch == "sbf":
            _process_single_day_sbf(site, yyyydoy)
        elif branch == "rinex":
            _process_single_day_rinex(site, yyyydoy)
        elif branch == "sbf_agency":
            _process_single_day_sbf_agency(site, yyyydoy)
        else:
            raise ValueError(f"Unknown branch: {branch!r}")

        logger.info("backfill: %s/%s %s — ok", site, branch, yyyydoy)
        return {"site": site, "branch": branch, "yyyydoy": yyyydoy, "status": "ok"}

    dates = t_resolve_dates()
    t_process_day.expand(yyyydoy=dates)


# ---------------------------------------------------------------------------
# Per-day pipeline helpers (ingest → VOD; no analytics in public package)
# ---------------------------------------------------------------------------


def _process_single_day_sbf(site: str, yyyydoy: str) -> None:
    """SBF + broadcast ephemeris pipeline for one day."""
    from canvodpy.workflows.tasks import (
        calculate_vod,
        check_sbf,
        cleanup,
        process_sbf,
        validate_ingest,
    )

    sbf_info = check_sbf(site, yyyydoy)
    process_sbf(site, yyyydoy, receiver_files=sbf_info["receivers"])
    validate_ingest(site, yyyydoy)
    calculate_vod(site, yyyydoy)
    cleanup(site, yyyydoy)


def _process_single_day_rinex(site: str, yyyydoy: str) -> None:
    """RINEX + agency SP3/CLK pipeline for one day."""
    from canvodpy.workflows.tasks import (
        calculate_vod,
        check_rinex,
        cleanup,
        fetch_aux_data,
        process_rinex,
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
    cleanup(site, yyyydoy)


def _process_single_day_sbf_agency(site: str, yyyydoy: str) -> None:
    """SBF observables + agency SP3/CLK geometry pipeline for one day."""
    from canvodpy.workflows.tasks import (
        calculate_vod,
        check_sbf,
        cleanup,
        fetch_aux_data,
        process_sbf,
        validate_ingest,
    )

    sbf_info = check_sbf(site, yyyydoy)
    aux_info = fetch_aux_data(site, yyyydoy)
    process_sbf(
        site,
        yyyydoy,
        receiver_files=sbf_info["receivers"],
        aux_zarr_path=aux_info["aux_zarr_path"],
    )
    validate_ingest(site, yyyydoy)
    calculate_vod(site, yyyydoy)
    cleanup(site, yyyydoy)


# Instantiate
canvod_backfill()
