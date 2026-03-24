"""Tests for Airflow DAG structure and best practices.

Validates DAG files parse correctly and follow expected patterns.
Does NOT require a running Airflow instance.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"


class TestDagFileSyntax:
    """Verify DAG files are valid Python."""

    @pytest.mark.parametrize(
        "dag_file",
        ["gnss_daily_processing.py", "gnss_backfill.py"],
    )
    def test_dag_parses(self, dag_file):
        """DAG file must be valid Python (AST parse)."""
        path = DAGS_DIR / dag_file
        assert path.exists(), f"DAG file not found: {path}"
        source = path.read_text()
        ast.parse(source, filename=dag_file)

    @pytest.mark.parametrize(
        "dag_file",
        ["gnss_daily_processing.py", "gnss_backfill.py"],
    )
    def test_no_hardcoded_paths(self, dag_file):
        """DAG files must not contain hardcoded absolute paths."""
        source = (DAGS_DIR / dag_file).read_text()
        # Allow /dev/null (used as dummy aux path)
        lines = [
            line
            for line in source.splitlines()
            if not line.strip().startswith("#") and "/dev/null" not in line
        ]
        for line in lines:
            assert not re.search(r'["\'][/~][a-zA-Z]', line), (
                f"Hardcoded path in {dag_file}: {line.strip()}"
            )


class TestDailyDagStructure:
    """Verify gnss_daily_processing.py structure."""

    @pytest.fixture()
    def dag_source(self):
        return (DAGS_DIR / "gnss_daily_processing.py").read_text()

    def test_has_two_dag_factories(self, dag_source):
        """Must define create_sbf_dag and create_rinex_dag."""
        assert "def create_sbf_dag" in dag_source
        assert "def create_rinex_dag" in dag_source

    def test_dynamic_generation(self, dag_source):
        """Must generate DAGs dynamically from config."""
        assert "_get_configured_sites" in dag_source
        assert "globals()" in dag_source

    def test_sbf_dag_id_pattern(self, dag_source):
        """SBF DAG ID must follow canvod_{site}_sbf pattern."""
        assert 'f"canvod_{site_name}_sbf"' in dag_source

    def test_rinex_dag_id_pattern(self, dag_source):
        """RINEX DAG ID must follow canvod_{site}_rinex pattern."""
        assert 'f"canvod_{site_name}_rinex"' in dag_source

    def test_max_active_runs(self, dag_source):
        """Both DAGs must have max_active_runs=1."""
        assert dag_source.count("max_active_runs=1") >= 2

    def test_catchup_disabled(self, dag_source):
        """Both DAGs must have catchup=False."""
        assert dag_source.count("catchup=False") >= 2

    def test_has_execution_timeout(self, dag_source):
        """Must set execution_timeout."""
        assert "execution_timeout" in dag_source

    def test_has_failure_callback(self, dag_source):
        """Must define on_failure_callback."""
        assert "on_failure_callback" in dag_source

    def test_has_exponential_backoff(self, dag_source):
        """Must use retry_exponential_backoff."""
        assert "retry_exponential_backoff" in dag_source

    def test_has_reschedule_sensors(self, dag_source):
        """RINEX DAG must use mode='reschedule' for sensors."""
        assert 'mode="reschedule"' in dag_source

    def test_has_sp3_abandonment(self, dag_source):
        """Must abandon SP3 sensor after 30 days."""
        assert "AirflowSkipException" in dag_source

    def test_validate_dirs_no_retry(self, dag_source):
        """validate_dirs must have retries=0."""
        assert "retries=0" in dag_source

    def test_cleanup_trigger_rule(self, dag_source):
        """cleanup must use TriggerRule.ALL_DONE."""
        assert "TriggerRule.ALL_DONE" in dag_source

    def test_shared_analysis_pipeline(self, dag_source):
        """Both DAGs must use _wire_analysis_pipeline."""
        assert "_wire_analysis_pipeline" in dag_source

    def test_sbf_tasks_present(self, dag_source):
        """SBF DAG must have validate_dirs, check_sbf, process_sbf."""
        assert "t_validate_dirs" in dag_source
        assert "t_check_sbf" in dag_source
        assert "t_process_sbf" in dag_source

    def test_rinex_tasks_present(self, dag_source):
        """RINEX DAG must have sensors and fetch_aux_data."""
        assert "t_wait_for_rinex" in dag_source
        assert "t_wait_for_sp3" in dag_source
        assert "t_fetch_aux_data" in dag_source
        assert "t_process_rinex" in dag_source

    def test_no_streaming_references(self, dag_source):
        """DAG must NOT reference streaming statistics tasks."""
        for name in [
            "update_statistics",
            "update_climatology",
            "detect_anomalies",
            "detect_changepoints",
            "snapshot_statistics",
            "ProfileRegistry",
            "StatisticsStore",
            "streamstats",
        ]:
            assert name not in dag_source, f"Streaming ref found: {name}"


class TestBackfillDagStructure:
    """Verify gnss_backfill.py structure."""

    @pytest.fixture()
    def dag_source(self):
        return (DAGS_DIR / "gnss_backfill.py").read_text()

    def test_manual_schedule(self, dag_source):
        """Backfill DAG must have schedule=None (manual only)."""
        assert "schedule=None" in dag_source

    def test_has_params(self, dag_source):
        """Must accept site, branch, start_date, end_date params."""
        for param in ["site", "branch", "start_date", "end_date"]:
            assert f'"{param}"' in dag_source

    def test_branch_enum(self, dag_source):
        """Branch param must be enum of sbf/rinex."""
        assert '"sbf"' in dag_source
        assert '"rinex"' in dag_source

    def test_sequential_processing(self, dag_source):
        """Must process dates sequentially (for Icechunk safety)."""
        assert "for yyyydoy in dates" in dag_source

    def test_per_date_error_handling(self, dag_source):
        """Must catch per-date errors and continue."""
        assert "except Exception" in dag_source
        assert 'results[yyyydoy] = f"error' in dag_source

    def test_both_branches_end_at_vod(self, dag_source):
        """Both _process_single_day functions must end at VOD + cleanup."""
        # Should have calculate_vod and cleanup but NOT streaming tasks
        assert "calculate_vod(site, yyyydoy)" in dag_source
        assert "cleanup(site, yyyydoy)" in dag_source
        assert "update_statistics" not in dag_source

    def test_timeout_sufficient(self, dag_source):
        """Backfill task timeout must be >= 48 hours."""
        match = re.search(r"execution_timeout=timedelta\(hours=(\d+)\)", dag_source)
        assert match, "No execution_timeout found on backfill task"
        hours = int(match.group(1))
        assert hours >= 48, f"Timeout {hours}h too short for year-long backfill"
