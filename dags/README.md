# Airflow DAGs for canvodpy

## Overview

`gnss_daily_processing.py` generates one DAG per configured research site.
Each DAG runs every 6 hours and processes the previous day:

```
check_rinex ──> fetch_aux_data ──> process_rinex ──> calculate_vod
```

Tasks that fail because data is not yet available (RINEX files not
transferred, or SP3/CLK products not yet published) are retried
automatically by Airflow on the next scheduled run.

## Deployment

1. **Install canvodpy** in the Airflow worker environment:
   ```bash
   uv pip install -e ./canvodpy
   ```

2. **Copy (or symlink) the `dags/` directory** into Airflow's `dags_folder`
   (check `airflow.cfg` → `dags_folder`):
   ```bash
   ln -s /path/to/canvodpy/dags /path/to/airflow/dags/canvod
   ```

3. **Verify config** — the DAGs read site definitions from canvodpy's YAML
   config.  Ensure `config/sites.yaml` is accessible from the Airflow worker
   and that `gnss_site_data_root` points to the directory containing receiver
   subdirectories with `YYDDD` date folders.

4. **Set a `start_date`** — override the `start_date` in the DAG definition
   or use an Airflow Variable.

## Task descriptions

| Task | What it does |
|---|---|
| `check_rinex` | Verifies RINEX files exist for all receivers (fails if missing → Airflow retries) |
| `fetch_aux_data` | Downloads SP3+CLK from FTP, Hermite-interpolates, writes temp Zarr (fails if too recent) |
| `process_rinex` | Reads RINEX, augments with aux data, writes to Icechunk RINEX store |
| `calculate_vod` | Computes VOD for all analysis pairs, writes to VOD Icechunk store |

## Scheduling

- **Every 6 hours**, 3 retries per task with 6-hour retry delay
- Missing data (RINEX or SP3/CLK) causes a task failure → Airflow retries
  until the data appears
- `max_active_runs=1` prevents concurrent writes to the same Icechunk store
- `catchup=False` — only processes the current date
