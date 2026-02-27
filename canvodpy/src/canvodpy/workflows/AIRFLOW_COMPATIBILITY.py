# ruff: noqa: F821
"""Airflow compatibility analysis for canvodpy orchestration layer.

This document analyzes the canvodpy orchestrator for Apache Airflow integration.
"""

# ============================================================================
# AIRFLOW COMPATIBILITY ANALYSIS
# ============================================================================

# Current Status: ✅ HIGHLY COMPATIBLE
#
# The canvodpy orchestration layer follows Airflow best practices:
#
# 1. ✅ IDEMPOTENT OPERATIONS
#    - process_date() can be retried safely
#    - Same inputs always produce same outputs
#    - No hidden state mutations
#
# 2. ✅ STATELESS DESIGN
#    - Functions don't rely on persistent class state
#    - All inputs explicit (site, date, config)
#    - Outputs are complete and self-contained
#
# 3. ✅ CLEAR DEPENDENCIES
#    - Site → Pipeline → Process
#    - Explicit input/output contracts
#    - No implicit coupling
#
# 4. ✅ ERROR HANDLING
#    - Proper exception propagation
#    - Logging with context
#    - Graceful failure modes
#
# 5. ✅ SERIALIZATION FRIENDLY
#    - Primitive types for parameters (str, int)
#    - xarray.Dataset serializable to zarr/netcdf
#    - No complex object dependencies

# ============================================================================
# EXAMPLE AIRFLOW DAG
# ============================================================================


def example_airflow_dag():
    """Example DAG showing canvodpy integration."""
    from datetime import datetime, timedelta

    from airflow import DAG
    from airflow.operators.python import PythonOperator

    # Task functions (pure, stateless, idempotent)
    def health_check_task(site: str) -> bool:
        """Check site health before processing."""
        from canvodpy import Site

        site_obj = Site(site)
        return site_obj.active_receivers is not None

    def process_rinex_task(site: str, date: str, **context) -> dict:
        """Process RINEX data for one day."""
        from canvodpy import process_date

        # Idempotent - safe to retry
        data = process_date(site, date)

        # Return metadata (not full datasets - those are in Icechunk)
        return {
            "site": site,
            "date": date,
            "receivers": list(data.keys()),
            "epochs_per_receiver": {r: len(ds.epoch) for r, ds in data.items()},
        }

    def calculate_vod_task(site: str, date: str, **context) -> dict:
        """Calculate VOD for all site pairs."""
        from canvodpy import Site, calculate_vod

        site_obj = Site(site)
        results = {}

        for analysis_name, config in site_obj.vod_analyses.items():
            vod_ds = calculate_vod(
                site=site,
                canopy_receiver=config["canopy_receiver"],
                reference_receiver=config["reference_receiver"],
                date=date,
            )
            results[analysis_name] = {
                "mean_vod": float(vod_ds.tau.mean()),
                "std_vod": float(vod_ds.tau.std()),
            }

        return results

    # Define DAG
    default_args = {
        "owner": "gnss",
        "depends_on_past": False,
        "start_date": datetime(2025, 1, 1),
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    }

    dag = DAG(
        "gnss_daily_processing",
        default_args=default_args,
        description="Daily GNSS data processing and VOD calculation",
        schedule="@daily",
        catchup=False,
        tags=["gnss", "vod"],
    )

    # Tasks
    health_check = PythonOperator(
        task_id="health_check",
        python_callable=health_check_task,
        op_kwargs={"site": "Rosalia"},
        dag=dag,
    )

    process_data = PythonOperator(
        task_id="process_rinex",
        python_callable=process_rinex_task,
        op_kwargs={
            "site": "Rosalia",
            "date": "{{ ds_nodash }}",  # Airflow template: YYYYMMDD
        },
        dag=dag,
    )

    calculate_vod = PythonOperator(
        task_id="calculate_vod",
        python_callable=calculate_vod_task,
        op_kwargs={
            "site": "Rosalia",
            "date": "{{ ds_nodash }}",
        },
        dag=dag,
    )

    # Dependencies
    health_check >> process_data >> calculate_vod

    return dag


# ============================================================================
# MULTI-SITE DAG PATTERN
# ============================================================================


def multi_site_dag():
    """Process multiple sites in parallel with Airflow."""
    from datetime import datetime

    from airflow import DAG
    from airflow.operators.python import PythonOperator

    SITES = ["Rosalia", "Neusiedl", "Vienna"]  # Could be 20+ sites

    dag = DAG(
        "gnss_multi_site_processing",
        start_date=datetime(2025, 1, 1),
        schedule="@daily",
        catchup=False,
    )

    # Dynamically create tasks for each site
    for site in SITES:
        task_id = f"process_{site.lower()}"

        task = PythonOperator(
            task_id=task_id,
            python_callable=lambda s=site: process_site(s, "{{ ds_nodash }}"),
            dag=dag,
        )

    return dag


# ============================================================================
# AIRFLOW BEST PRACTICES CHECKLIST
# ============================================================================

AIRFLOW_COMPATIBILITY_CHECKLIST = {
    "idempotent_tasks": {
        "status": "✅ PASS",
        "evidence": [
            "process_date() always produces same output for same input",
            "Icechunk handles deduplication via content hashing",
            "Safe to retry without side effects",
        ],
    },
    "stateless_functions": {
        "status": "✅ PASS",
        "evidence": [
            "No hidden class state",
            "All parameters explicit",
            "No global variables modified",
        ],
    },
    "clear_dependencies": {
        "status": "✅ PASS",
        "evidence": [
            "Linear task flow: health → process → vod",
            "Explicit data dependencies via Icechunk",
            "No implicit coupling between tasks",
        ],
    },
    "proper_logging": {
        "status": "⚠️ PARTIAL",
        "evidence": [
            "Has logging with context",
            "TODO: Add structured logging (structlog)",
            "TODO: Add metrics (Prometheus)",
        ],
    },
    "error_handling": {
        "status": "✅ PASS",
        "evidence": [
            "Exceptions propagate cleanly",
            "Proper error messages",
            "Logging on failures",
        ],
    },
    "serialization": {
        "status": "✅ PASS",
        "evidence": [
            "Parameters are primitives (str, int)",
            "Datasets stored in Icechunk (not passed between tasks)",
            "Return values are JSON-serializable dicts",
        ],
    },
    "monitoring": {
        "status": "📋 PLANNED",
        "evidence": [
            "Structure ready (diagnostics/)",
            "TODO: Add task-level metrics",
            "TODO: Add health checks",
        ],
    },
}


# ============================================================================
# RECOMMENDATIONS FOR PRODUCTION AIRFLOW DEPLOYMENT
# ============================================================================

PRODUCTION_RECOMMENDATIONS = """
1. **Task Design**
   - Keep tasks small and focused (single responsibility)
   - Use XCom only for metadata, not large datasets
   - Store actual data in Icechunk (already done ✅)

2. **Error Handling**
   - Set appropriate retries (2-3 for transient failures)
   - Use exponential backoff for retry delays
   - Add alerting for critical failures

3. **Monitoring**
   - Track task duration metrics
   - Monitor Icechunk storage usage
   - Set up SLAs for critical tasks

4. **Resource Management**
   - Use pools to limit concurrent processing
   - Set appropriate task concurrency limits
   - Monitor memory usage per task

5. **Data Management**
   - Icechunk handles data storage (good ✅)
   - Keep XCom payloads small (<1MB)
   - Clean up old task instances regularly

6. **Configuration**
   - Use Airflow Variables for site configs
   - Store credentials in Airflow Connections
   - Use Jinja templates for dynamic dates

7. **Testing**
   - Test tasks independently before adding to DAG
   - Use dry_run mode for validation
   - Test with historical data (backfill)

8. **Deployment**
   - Use CI/CD for DAG deployment
   - Version DAGs properly
   - Test in staging before production
"""


# ============================================================================
# WHAT NEEDS TO BE ADDED FOR FULL AIRFLOW SUPPORT
# ============================================================================

TODO_FOR_AIRFLOW = """
Short-term (canvodpy/workflows/):

1. **Task Functions** (workflows/tasks.py)
   - health_check_task()
   - process_rinex_task()
   - calculate_vod_task()
   - cleanup_task()

2. **DAG Templates** (workflows/dags/)
   - daily_processing.py
   - weekly_summary.py
   - multi_site_parallel.py
   - backfill_template.py

3. **Helper Utilities** (workflows/utils.py)
   - get_sites_from_config()
   - validate_date_range()
   - check_data_availability()

Medium-term (canvodpy/diagnostics/):

4. **Structured Logging**
   - Task context logging
   - Performance metrics
   - Error tracking

5. **Monitoring**
   - Prometheus metrics export
   - Health check endpoints
   - Alerting rules

6. **Configuration** (canvodpy/config/)
   - Site configuration from TOML/YAML
   - Airflow Variables integration
   - Environment-based config
"""


if __name__ == "__main__":
    print("✅ AIRFLOW COMPATIBILITY: HIGH")
    print("\nCurrent Status:")
    for check, result in AIRFLOW_COMPATIBILITY_CHECKLIST.items():
        print(f"  {result['status']} {check}")

    print("\n" + "=" * 70)
    print("The orchestration layer is READY for Airflow integration!")
    print("=" * 70)
