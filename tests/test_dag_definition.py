"""Tests for Airflow DAG definition.

These tests validate that the DAG is properly structured and
all tasks have correct dependencies.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

# Ensure src and dags are on path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))
sys.path.insert(0, str(BASE_DIR / "dags"))


def test_dag_import() -> None:
    """Test that DAG can be imported without errors."""
    try:
        from dags.transaction_pipeline_dag import dag
        assert dag is not None
    except ImportError as e:
        pytest.skip(f"DAG import requires Airflow: {e}")


class TestDAGStructure:
    """Tests for DAG structure and configuration."""

    @pytest.fixture(scope="class")
    def dag(self) -> Any:
        """Fixture to load DAG."""
        try:
            from dags.transaction_pipeline_dag import dag as test_dag
            return test_dag
        except ImportError:
            pytest.skip("Airflow not installed")

    def test_dag_id(self, dag: Any) -> None:
        """Test DAG has correct ID."""
        assert dag.dag_id == "bank_transaction_pipeline"

    def test_dag_has_tasks(self, dag: Any) -> None:
        """Test DAG has the expected tasks."""
        expected_tasks = [
            "wait_for_raw_file",
            "extract",
            "transform",
            "validate.quality_check",
            "validate.reconcile",
            "branch_on_reconcile",
            "load",
            "daily_aggregation",
            "aml_detection",
            "cleanup_intermediates",
            "alert_reconcile_failure",
        ]

        task_ids = [task.task_id for task in dag.tasks]
        for expected in expected_tasks:
            assert expected in task_ids, f"Missing task: {expected}"

    def test_dag_has_tags(self, dag: Any) -> None:
        """Test DAG has appropriate tags."""
        expected_tags = {"etl", "banking", "transactions", "aml", "data-quality"}
        actual_tags = set(dag.tags) if dag.tags else set()
        assert expected_tags <= actual_tags, f"Missing tags: {expected_tags - actual_tags}"

    def test_dag_schedule_interval(self, dag: Any) -> None:
        """Test DAG has correct schedule."""
        # Should be daily at 6 AM UTC
        assert dag.schedule_interval == "0 6 * * *"

    def test_dag_catchup_disabled(self, dag: Any) -> None:
        """Test DAG catchup is disabled."""
        assert dag.catchup is False

    def test_dag_max_active_runs(self, dag: Any) -> None:
        """Test DAG has max active runs set."""
        assert dag.max_active_runs == 1


class TestTaskDependencies:
    """Tests for task dependencies."""

    @pytest.fixture(scope="class")
    def dag(self) -> Any:
        """Fixture to load DAG."""
        try:
            from dags.transaction_pipeline_dag import dag as test_dag
            return test_dag
        except ImportError:
            pytest.skip("Airflow not installed")

    def test_wait_for_file_upstream(self, dag: Any) -> None:
        """Test wait_for_raw_file has no upstream dependencies."""
        task = dag.get_task("wait_for_raw_file")
        upstream = task.upstream_list
        assert len(upstream) == 0, "wait_for_raw_file should have no upstreams"

    def test_extract_dependencies(self, dag: Any) -> None:
        """Test extract depends on wait_for_raw_file."""
        extract_task = dag.get_task("extract")
        upstream_ids = [t.task_id for t in extract_task.upstream_list]
        assert "wait_for_raw_file" in upstream_ids

    def test_transform_dependencies(self, dag: Any) -> None:
        """Test transform depends on extract."""
        transform_task = dag.get_task("transform")
        upstream_ids = [t.task_id for t in transform_task.upstream_list]
        assert "extract" in upstream_ids

    def test_validate_parallel(self, dag: Any) -> None:
        """Test quality_check and reconcile run in parallel."""
        quality_task = dag.get_task("validate.quality_check")
        reconcile_task = dag.get_task("validate.reconcile")

        # Both should have transform as upstream
        quality_upstreams = [t.task_id for t in quality_task.upstream_list]
        reconcile_upstreams = [t.task_id for t in reconcile_task.upstream_list]

        assert "transform" in quality_upstreams
        assert "transform" in reconcile_upstreams

    def test_branch_dependencies(self, dag: Any) -> None:
        """Test branch depends on validate task group."""
        branch_task = dag.get_task("branch_on_reconcile")
        upstream_ids = [t.task_id for t in branch_task.upstream_list]
        # Should depend on validate task group
        assert "validate" in upstream_ids

    def test_load_dependencies(self, dag: Any) -> None:
        """Test load depends on branch."""
        load_task = dag.get_task("load")
        upstream_ids = [t.task_id for t in load_task.upstream_list]
        assert "branch_on_reconcile" in upstream_ids

    def test_daily_aggregation_dependencies(self, dag: Any) -> None:
        """Test daily_aggregation depends on load."""
        agg_task = dag.get_task("daily_aggregation")
        upstream_ids = [t.task_id for t in agg_task.upstream_list]
        assert "load" in upstream_ids

    def test_aml_dependencies(self, dag: Any) -> None:
        """Test aml_detection depends on daily_aggregation."""
        aml_task = dag.get_task("aml_detection")
        upstream_ids = [t.task_id for t in aml_task.upstream_list]
        assert "daily_aggregation" in upstream_ids

    def test_cleanup_dependencies(self, dag: Any) -> None:
        """Test cleanup depends on aml_detection."""
        cleanup_task = dag.get_task("cleanup_intermediates")
        upstream_ids = [t.task_id for t in cleanup_task.upstream_list]
        assert "aml_detection" in upstream_ids

    def test_alert_dependencies(self, dag: Any) -> None:
        """Test alert_reconcile_failure depends on branch."""
        alert_task = dag.get_task("alert_reconcile_failure")
        upstream_ids = [t.task_id for t in alert_task.upstream_list]
        assert "branch_on_reconcile" in upstream_ids


class TestTaskProperties:
    """Tests for task-specific properties."""

    @pytest.fixture(scope="class")
    def dag(self) -> Any:
        """Fixture to load DAG."""
        try:
            from dags.transaction_pipeline_dag import dag as test_dag
            return test_dag
        except ImportError:
            pytest.skip("Airflow not installed")

    def test_file_sensor_properties(self, dag: Any) -> None:
        """Test FileSensor configuration."""
        sensor = dag.get_task("wait_for_raw_file")
        assert sensor.poke_interval == 60
        assert sensor.timeout == 7200
        assert sensor.mode == "poke"

    def test_cleanup_trigger_rule(self, dag: Any) -> None:
        """Test cleanup has all_done trigger rule."""
        from airflow.utils.trigger_rule import TriggerRule

        cleanup = dag.get_task("cleanup_intermediates")
        assert cleanup.trigger_rule == TriggerRule.ALL_DONE

    def test_aml_trigger_rule(self, dag: Any) -> None:
        """Test AML has all_done trigger rule."""
        from airflow.utils.trigger_rule import TriggerRule

        aml = dag.get_task("aml_detection")
        assert aml.trigger_rule == TriggerRule.ALL_DONE

    def test_default_args(self, dag: Any) -> None:
        """Test default args are applied."""
        assert dag.default_args["retries"] == 2
        assert dag.default_args["retry_delay"].total_seconds() == 300  # 5 minutes
        assert dag.default_args["execution_timeout"].total_seconds() == 7200  # 2 hours


class TestDAGDocumentation:
    """Tests for DAG documentation."""

    @pytest.fixture(scope="class")
    def dag(self) -> Any:
        """Fixture to load DAG."""
        try:
            from dags.transaction_pipeline_dag import dag as test_dag
            return test_dag
        except ImportError:
            pytest.skip("Airflow not installed")

    def test_dag_has_doc_md(self, dag: Any) -> None:
        """Test DAG has doc_md."""
        assert dag.doc_md is not None
        assert len(dag.doc_md) > 0

    def test_tasks_have_doc_md(self, dag: Any) -> None:
        """Test tasks have doc_md."""
        for task in dag.tasks:
            assert task.doc_md is not None, f"Task {task.task_id} missing doc_md"


class TestDAGParameters:
    """Tests for DAG parameters and configuration."""

    def test_start_date(self) -> None:
        """Test DAG start date."""
        try:
            from dags.transaction_pipeline_dag import dag
            assert dag.start_date.year == 2024
            assert dag.start_date.month == 1
            assert dag.start_date.day == 1
        except ImportError:
            pytest.skip("Airflow not installed")

    def test_default_owner(self) -> None:
        """Test DAG default owner."""
        try:
            from dags.transaction_pipeline_dag import dag
            assert dag.default_args["owner"] == "data-engineering"
        except ImportError:
            pytest.skip("Airflow not installed")