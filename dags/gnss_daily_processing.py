"""Airflow DAG template — one DAG per configured GNSS research site.

Each DAG runs every 6 hours and processes the **previous day**.
The five tasks form a linear chain::

    check_rinex ──> fetch_aux_data ──> process_rinex ──> calculate_vod ──> update_statistics

``check_rinex`` fails with RuntimeError when RINEX files are not yet
present for both receivers — Airflow retries automatically.

``fetch_aux_data`` downloads SP3/CLK products from public FTP servers.
Final products lag ~12-14 days, rapid products ~1 day.  When products
are not yet available the FTP download raises RuntimeError, which
Airflow retries on the next scheduled run.

Requirements
------------
* ``canvodpy`` installed in the Airflow worker environment.
* Apache Airflow >= 2.4 (TaskFlow API with ``@dag``/``@task``).
* ``pendulum`` (ships with Airflow).
"""

from __future__ import annotations

import logging
from datetime import timedelta

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)


def _get_configured_sites() -> list[str]:
    """Return site names from canvodpy config.

    Imports are deferred so that the DAG file can be parsed by the Airflow
    scheduler even when ``canvodpy`` is unavailable (parse-time safety).
    """
    try:
        from canvod.utils.config import load_config

        return list(load_config().sites.sites.keys())
    except Exception:
        logger.warning("Could not load canvodpy config — no DAGs generated")
        return []


def _ds_to_yyyydoy(ds: str) -> str:
    """Convert Airflow ``ds`` (``YYYY-MM-DD``) to ``YYYYDDD``."""
    import datetime

    date = datetime.date.fromisoformat(ds)
    doy = (date - datetime.date(date.year, 1, 1)).days + 1
    return f"{date.year}{doy:03d}"


def create_site_dag(site_name: str):
    """Create a daily processing DAG for *site_name*."""

    @dag(
        dag_id=f"canvod_{site_name}",
        schedule="0 */6 * * *",  # every 6 hours
        start_date=None,  # set by Airflow Variable or override
        catchup=False,
        max_active_runs=1,
        default_args={
            "owner": "canvod",
            "retries": 3,
            "retry_delay": timedelta(hours=6),
        },
        tags=["canvod", "gnss", site_name],
        doc_md=__doc__,
    )
    def site_dag():
        @task
        def t_check_rinex(ds: str = "{{ ds }}") -> dict:
            from canvodpy.workflows.tasks import check_rinex

            return check_rinex(site_name, _ds_to_yyyydoy(ds))

        @task
        def t_fetch_aux_data(
            rinex_info: dict,
            ds: str = "{{ ds }}",
        ) -> dict:
            from canvodpy.workflows.tasks import fetch_aux_data

            _ = rinex_info  # dependency only — ensures RINEX is available
            return fetch_aux_data(site_name, _ds_to_yyyydoy(ds))

        @task
        def t_process_rinex(
            aux_info: dict,
            rinex_info: dict,
            ds: str = "{{ ds }}",
        ) -> dict:
            from canvodpy.workflows.tasks import process_rinex

            return process_rinex(
                site=site_name,
                yyyydoy=_ds_to_yyyydoy(ds),
                aux_zarr_path=aux_info["aux_zarr_path"],
                receiver_files=rinex_info["receivers"],
            )

        @task
        def t_calculate_vod(
            process_info: dict,
            ds: str = "{{ ds }}",
        ) -> dict:
            from canvodpy.workflows.tasks import calculate_vod

            _ = process_info  # dependency only
            return calculate_vod(site_name, _ds_to_yyyydoy(ds))

        @task
        def t_update_statistics(
            vod_info: dict,
            ds: str = "{{ ds }}",
        ) -> dict:
            from canvodpy.workflows.tasks import update_statistics

            _ = vod_info  # dependency only
            return update_statistics(site_name, _ds_to_yyyydoy(ds))

        # Wire the DAG — linear chain
        rinex_info = t_check_rinex()
        aux_info = t_fetch_aux_data(rinex_info=rinex_info)
        process_info = t_process_rinex(aux_info=aux_info, rinex_info=rinex_info)
        vod_info = t_calculate_vod(process_info=process_info)
        t_update_statistics(vod_info=vod_info)

    return site_dag()


# Dynamic DAG generation: one per configured site
for _site_name in _get_configured_sites():
    globals()[f"canvod_{_site_name}"] = create_site_dag(_site_name)
