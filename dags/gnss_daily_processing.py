"""Airflow DAGs for GNSS-Transmissometry daily processing.

Two DAGs per configured site:

**SBF DAG** (``canvod_{site}_sbf``) — same-day results::

    validate_dirs → check_sbf → process_sbf
      → validate_ingest → calculate_vod → cleanup

**RINEX DAG** (``canvod_{site}_rinex``) — agency-quality, delayed::

    validate_dirs → wait_for_rinex → wait_for_sp3 → fetch_aux_data
      → process_rinex → validate_ingest → calculate_vod → cleanup

Requirements
------------
* ``canvodpy`` installed in the Airflow worker environment.
* Apache Airflow >= 2.4 (TaskFlow API with ``@dag``/``@task``).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration helpers (parse-time safe)
# ---------------------------------------------------------------------------


def _get_configured_sites() -> dict:
    """Return {site_name: site_cfg} from canvodpy config.

    Imports are deferred so that the DAG file can be parsed by the Airflow
    scheduler even when ``canvodpy`` is unavailable (parse-time safety).
    """
    try:
        from canvod.utils.config import load_config

        return dict(load_config().sites.sites)
    except Exception:
        logger.warning("Could not load canvodpy config — no DAGs generated")
        return {}


def _ds_to_yyyydoy(ds: str) -> str:
    """Convert Airflow ``ds`` (``YYYY-MM-DD``) to ``YYYYDDD``."""
    import datetime as dt

    date = dt.date.fromisoformat(ds)
    doy = (date - dt.date(date.year, 1, 1)).days + 1
    return f"{date.year}{doy:03d}"


# ---------------------------------------------------------------------------
# Failure callback
# ---------------------------------------------------------------------------


def _task_failure_callback(context):
    """Log structured failure info. Future: Slack/email hook."""
    ti = context["task_instance"]
    logger.error(
        "TASK FAILED | dag=%s task=%s date=%s error=%s log_url=%s",
        ti.dag_id,
        ti.task_id,
        context.get("ds", "?"),
        context.get("exception", "unknown"),
        ti.log_url,
    )


# ---------------------------------------------------------------------------
# Shared default_args
# ---------------------------------------------------------------------------

_DEFAULT_ARGS = {
    "owner": "canvod",
    "retries": 5,
    "retry_delay": timedelta(minutes=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(hours=12),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": _task_failure_callback,
}

_START_DATE = datetime(2025, 1, 1)


# ---------------------------------------------------------------------------
# Shared analysis pipeline (used by both SBF and RINEX DAGs)
# ---------------------------------------------------------------------------


def _wire_analysis_pipeline(site_name: str, ingest_info: dict):
    """Wire the shared analysis tasks: validate → VOD → cleanup.

    Returns the final cleanup task result for DAG completion tracking.
    """

    @task(execution_timeout=timedelta(hours=1))
    def t_validate_ingest(
        process_info: dict,
        ds: str = "{{ ds }}",
    ) -> dict:
        from canvodpy.workflows.tasks import validate_ingest

        _ = process_info
        return validate_ingest(site_name, _ds_to_yyyydoy(ds))

    @task(execution_timeout=timedelta(hours=1))
    def t_calculate_vod(
        ingest_valid: dict,
        ds: str = "{{ ds }}",
    ) -> dict:
        from canvodpy.workflows.tasks import calculate_vod

        _ = ingest_valid
        return calculate_vod(site_name, _ds_to_yyyydoy(ds))

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def t_cleanup(
        vod_info: dict,
        ds: str = "{{ ds }}",
    ) -> dict:
        from canvodpy.workflows.tasks import cleanup

        _ = vod_info
        return cleanup(site_name, _ds_to_yyyydoy(ds))

    # Wire the chain
    ingest_valid = t_validate_ingest(process_info=ingest_info)
    vod_info = t_calculate_vod(ingest_valid=ingest_valid)
    return t_cleanup(vod_info=vod_info)


# ---------------------------------------------------------------------------
# SBF DAG — same-day results, broadcast ephemeris
# ---------------------------------------------------------------------------


def create_sbf_dag(site_name: str):
    """Create a daily SBF processing DAG for *site_name*."""

    @dag(
        dag_id=f"canvod_{site_name}_sbf",
        schedule="@daily",
        start_date=_START_DATE,
        catchup=False,
        max_active_runs=1,
        default_args=_DEFAULT_ARGS,
        tags=["canvod", "gnss", "sbf", site_name],
        doc_md=__doc__,
    )
    def sbf_dag():
        @task(retries=0)
        def t_validate_dirs(ds: str = "{{ ds }}") -> dict:
            from canvodpy.workflows.tasks import validate_data_dirs

            return validate_data_dirs(site_name)

        @task
        def t_check_sbf(
            valid_info: dict,
            ds: str = "{{ ds }}",
        ) -> dict:
            from canvodpy.workflows.tasks import check_sbf

            _ = valid_info
            return check_sbf(site_name, _ds_to_yyyydoy(ds))

        @task(execution_timeout=timedelta(hours=4))
        def t_process_sbf(
            sbf_info: dict,
            ds: str = "{{ ds }}",
        ) -> dict:
            from canvodpy.workflows.tasks import process_sbf

            return process_sbf(
                site=site_name,
                yyyydoy=_ds_to_yyyydoy(ds),
                receiver_files=sbf_info["receivers"],
            )

        # Wire ingest chain
        valid_info = t_validate_dirs()
        sbf_info = t_check_sbf(valid_info=valid_info)
        process_info = t_process_sbf(sbf_info=sbf_info)

        # Wire shared analysis pipeline
        _wire_analysis_pipeline(site_name, process_info)

    return sbf_dag()


# ---------------------------------------------------------------------------
# RINEX DAG — agency-quality, SP3/CLK sensor wait up to 21 days
# ---------------------------------------------------------------------------


def create_rinex_dag(site_name: str):
    """Create a daily RINEX processing DAG for *site_name*."""

    @dag(
        dag_id=f"canvod_{site_name}_rinex",
        schedule="@daily",
        start_date=_START_DATE,
        catchup=False,
        max_active_runs=1,
        default_args=_DEFAULT_ARGS,
        tags=["canvod", "gnss", "rinex", site_name],
        doc_md=__doc__,
    )
    def rinex_dag():
        @task(retries=0)
        def t_validate_dirs(ds: str = "{{ ds }}") -> dict:
            from canvodpy.workflows.tasks import validate_data_dirs

            return validate_data_dirs(site_name)

        @task.sensor(
            poke_interval=3600 * 6,
            timeout=3600 * 24 * 21,
            mode="reschedule",
        )
        def t_wait_for_rinex(
            valid_info: dict,
            ds: str = "{{ ds }}",
        ):
            """Wait for RINEX files to appear (up to 21 days)."""
            from airflow.sensors.base import PokeReturnValue
            from canvodpy.workflows.tasks import check_rinex

            _ = valid_info
            yyyydoy = _ds_to_yyyydoy(ds)
            try:
                result = check_rinex(site_name, yyyydoy)
                return PokeReturnValue(is_done=True, xcom_value=result)
            except RuntimeError:
                return PokeReturnValue(is_done=False)

        @task.sensor(
            poke_interval=3600 * 6,
            timeout=3600 * 24 * 21,
            mode="reschedule",
        )
        def t_wait_for_sp3(
            rinex_info: dict,
            ds: str = "{{ ds }}",
        ):
            """Wait for SP3/CLK products to be available (lightweight check).

            Only checks FTP availability — does NOT download or interpolate.
            Abandons after 30 days to prevent permanent DAG run clutter.
            """
            import datetime as dt

            from airflow.exceptions import AirflowSkipException
            from airflow.sensors.base import PokeReturnValue

            _ = rinex_info
            yyyydoy = _ds_to_yyyydoy(ds)

            # Abandonment: if date is >30 days old, give up
            target = dt.date.fromisoformat(ds)
            age = (dt.date.today() - target).days
            if age > 30:
                raise AirflowSkipException(
                    f"SP3 not available for {yyyydoy} after {age} days — abandoning"
                )

            try:
                from canvod.auxiliary.pipeline import AuxDataPipeline

                pipeline = AuxDataPipeline.create_standard()
                available = pipeline.check_availability(yyyydoy)
                return PokeReturnValue(
                    is_done=available,
                    xcom_value={"sp3_ready": available},
                )
            except Exception:
                return PokeReturnValue(is_done=False)

        @task(execution_timeout=timedelta(hours=2))
        def t_fetch_aux_data(
            sp3_info: dict,
            ds: str = "{{ ds }}",
        ) -> dict:
            """Download SP3/CLK and Hermite-interpolate to aux Zarr."""
            from canvodpy.workflows.tasks import fetch_aux_data

            _ = sp3_info
            return fetch_aux_data(site_name, _ds_to_yyyydoy(ds))

        @task(execution_timeout=timedelta(hours=4))
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

        # Wire ingest chain with sensors
        valid_info = t_validate_dirs()
        rinex_info = t_wait_for_rinex(valid_info=valid_info)
        sp3_info = t_wait_for_sp3(rinex_info=rinex_info)
        aux_info = t_fetch_aux_data(sp3_info=sp3_info)
        process_info = t_process_rinex(aux_info=aux_info, rinex_info=rinex_info)

        # Wire shared analysis pipeline
        _wire_analysis_pipeline(site_name, process_info)

    return rinex_dag()


# ---------------------------------------------------------------------------
# Dynamic DAG generation: two DAGs per configured site
# ---------------------------------------------------------------------------

for _site_name, _site_cfg in _get_configured_sites().items():
    # Generate SBF DAG for all sites (broadcast ephemeris always available)
    globals()[f"canvod_{_site_name}_sbf"] = create_sbf_dag(_site_name)
    # Generate RINEX DAG for all sites (waits for agency SP3/CLK)
    globals()[f"canvod_{_site_name}_rinex"] = create_rinex_dag(_site_name)
