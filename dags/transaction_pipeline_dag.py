"""Apache Airflow DAG for the Bank Transaction ETL Pipeline.

This DAG orchestrates the bank transaction pipeline with the following flow:

    FileSensor(wait_for_raw_file)
        ↓
    extract_task → XCom: raw_path, row_count
        ↓
    transform_task → XCom: transformed_path, ledger_path
        ↓
    [quality_check_task + reconcile_task] (parallel)
        ↓
    branch_on_reconcile
        ├─ success → load_task
        └─ failure → alert_reconcile_failure_task → fail
        ↓
    load_task
        ↓
    daily_aggregation_task
        ↓
    aml_detection_task (optional, based on config)
        ↓
    cleanup_intermediate_task (trigger_rule='all_done')

Data Passing Strategy:
- XCom for metadata (file paths, row counts, status flags)
- Parquet files in data/intermediate/ for DataFrames
- Each task reads from previous task's output path, writes to own output path

Scheduling:
- Default: Daily at 6:00 AM UTC
- Configurable via pipeline_config.yaml
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

try:
    from airflow import DAG
    from airflow.operators.python import BranchPythonOperator, PythonOperator
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    from airflow.sensors.filesystem import FileSensor
    from airflow.utils.task_group import TaskGroup
    from airflow.utils.trigger_rule import TriggerRule
except ImportError:
    # Fallback for local development without Airflow
    # Creates a mock DAG context for testing
    class MockDAG:
        def __init__(self, *args, **kwargs):
            self.dag_id = kwargs.get('dag_id', 'mock_dag')
            self.tasks = []
            self.tags = kwargs.get('tags', [])
            self.schedule_interval = kwargs.get('schedule_interval')
            self.catchup = kwargs.get('catchup', False)
            self.max_active_runs = kwargs.get('max_active_runs', 1)
            self.default_args = kwargs.get('default_args', {})
            self.start_date = kwargs.get('start_date')
            self.doc_md = kwargs.get('doc_md', '')

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def get_task(self, task_id):
            return next((t for t in self.tasks if t.task_id == task_id), None)

    DAG = MockDAG

    class MockOperator:
        def __init__(self, *args, **kwargs):
            self.task_id = kwargs.get('task_id', 'mock_task')
            self.upstream_list = []
            self.doc_md = kwargs.get('doc_md', '')
            self.trigger_rule = kwargs.get('trigger_rule', None)
            self.poke_interval = kwargs.get('poke_interval', 60)
            self.timeout = kwargs.get('timeout', 7200)
            self.mode = kwargs.get('mode', 'poke')

        def __rshift__(self, other):
            """Support >> syntax for task dependencies."""
            if isinstance(other, MockOperator):
                other.upstream_list.append(self)
            elif isinstance(other, MockTaskGroup):
                pass  # Task groups don't track upstreams directly
            return other

        def __lshift__(self, other):
            """Support << syntax for task dependencies."""
            self.upstream_list.append(other)
            return self

    class MockOperator:
        def __init__(self, *args, **kwargs):
            self.task_id = kwargs.get('task_id', 'mock_task')
            self.upstream_list = []
            self.downstream_list = []
            self.doc_md = kwargs.get('doc_md', '')
            self.trigger_rule = kwargs.get('trigger_rule', None)

        def __rshift__(self, other):
            """Support >> operator for task dependencies."""
            if hasattr(other, 'upstream_list'):
                other.upstream_list.append(self)
            if hasattr(self, 'downstream_list'):
                self.downstream_list.append(other)
            return other

        def __lshift__(self, other):
            """Support << operator for task dependencies."""
            if hasattr(self, 'upstream_list'):
                self.upstream_list.append(other)
            if hasattr(other, 'downstream_list'):
                other.downstream_list.append(self)
            return other

    PythonOperator = MockOperator
    BranchPythonOperator = MockOperator
    FileSensor = MockOperator
    TriggerDagRunOperator = MockOperator

    class MockTaskGroup:
        def __init__(self, *args, **kwargs):
            self.group_id = kwargs.get('group_id', 'mock_group')
            self.tooltip = kwargs.get('tooltip', '')
            self.doc_md = kwargs.get('doc_md', '')

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def __rshift__(self, other):
            """Support >> operator for task dependencies."""
            return other

        def __lshift__(self, other):
            """Support << operator for task dependencies."""
            return self

    TaskGroup = MockTaskGroup

    class TriggerRule:
        ALL_DONE = "all_done"
        ALL_SUCCESS = "all_success"

# Path setup for project imports
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.airflow_tasks import (
    alert_reconcile_failure_task,
    aml_detection_task,
    branch_on_reconcile,
    cleanup_intermediate_task,
    daily_aggregation_task,
    extract_task,
    load_task,
    quality_check_task,
    reconcile_task,
    transform_task,
)

# Logger
LOGGER = logging.getLogger(__name__)

# =============================================================================
# Default Arguments
# =============================================================================

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-team@example.com"],  # Update with actual email
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
    "sla": timedelta(hours=3),
}

# =============================================================================
# DAG Definition
# =============================================================================

with DAG(
    dag_id="bank_transaction_pipeline",
    default_args=default_args,
    description="ETL pipeline for bank transaction processing with data quality checks and AML detection",
    schedule_interval="0 6 * * *",  # Daily at 6 AM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    tags=["etl", "banking", "transactions", "aml", "data-quality"],
    doc_md=__doc__,
    template_searchpath=[str(BASE_DIR / "sql")],
    render_template_as_native_obj=True,
) as dag:

    # =========================================================================
    # Task 1: Wait for Raw File
    # =========================================================================

    wait_for_file = FileSensor(
        task_id="wait_for_raw_file",
        filepath="data/raw/transactions_raw.csv",
        fs_conn_id="fs_default",
        poke_interval=60,  # Check every minute
        timeout=60 * 60 * 2,  # Timeout after 2 hours
        mode="poke",
        soft_fail=False,
        doc_md="""
        ### Wait for Raw File

        Waits for the raw transactions CSV file to be available.

        **Configuration:**
        - File path: `data/raw/transactions_raw.csv`
        - Check interval: 60 seconds
        - Timeout: 2 hours

        **File Connection:**
        Uses the `fs_default` connection which should be configured with
        the base path for file system operations.
        """,
    )

    # =========================================================================
    # Task 2: Extract
    # =========================================================================

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_task,
        doc_md="""
        ### Extract Raw Data

        Extracts raw transactions from the CSV file.

        **Outputs (via XCom):**
        - `raw_path`: Path to intermediate Parquet file
        - `row_count`: Number of rows extracted
        - `run_id`: Unique identifier for this pipeline run
        - `extract_status`: "success" if completed

        **Raises:**
        - AirflowFailException: If file not found or extraction error
        """,
    )

    # =========================================================================
    # Task 3: Transform
    # =========================================================================

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
        doc_md="""
        ### Transform Data

        Transforms raw data by:
        - Normalizing status values (COMPLETED/SETTLED/OK → SUCCESS)
        - Normalizing amounts (refunds made negative)
        - Flagging late arrivals
        - Filtering to SUCCESS status for ledger

        **Inputs (via XCom from extract):**
        - `raw_path`: Path to raw data

        **Outputs (via XCom):**
        - `transformed_path`: Path to transformed data
        - `ledger_path`: Path to ledger data (SUCCESS only)
        - `transformed_count`: Number of transformed rows
        - `ledger_count`: Number of ledger rows
        - `transform_duration`: Processing time in seconds
        """,
    )

    # =========================================================================
    # Task Group: Validation (Parallel)
    # =========================================================================

    with TaskGroup(
        group_id="validate",
        tooltip="Data validation tasks (quality check and reconciliation)",
    ) as validate:

        # -----------------------------------------------------------------
        # Task 4a: Quality Check
        # -----------------------------------------------------------------

        quality_check = PythonOperator(
            task_id="quality_check",
            python_callable=quality_check_task,
            doc_md="""
            ### Quality Check

            Generates data quality metrics:
            - Row counts (raw, clean, ledger)
            - Rejection rate
            - Null value counts
            - Late arrival statistics

            **Outputs (via XCom):**
            - `quality_metrics_path`: Path to metrics JSON
            - `rejection_report_path`: Path to rejection CSV
            - `quality_passed`: Boolean indicating if quality checks passed
            - `rejection_rate`: Calculated rejection rate
            """,
        )

        # -----------------------------------------------------------------
        # Task 4b: Reconcile
        # -----------------------------------------------------------------

        reconcile = PythonOperator(
            task_id="reconcile",
            python_callable=reconcile_task,
            doc_md="""
            ### Reconcile

            Validates data integrity by comparing raw vs ledger totals by date.

            **Logic:**
            - Groups by transaction date
            - Compares raw totals vs ledger totals
            - Flags mismatches exceeding tolerance

            **Outputs (via XCom):**
            - `reconcile_passed`: Boolean indicating if reconciliation passed
            - `recon_dates_count`: Number of dates compared
            - `mismatch_count`: Number of mismatched dates
            - `recon_report_path`: Path to reconciliation report

            **Raises:**
            - AirflowFailException: If reconciliation fails and
              `fail_on_mismatch` is true in config
            """,
        )

    # =========================================================================
    # Task 5: Branch on Reconcile
    # =========================================================================

    branch = BranchPythonOperator(
        task_id="branch_on_reconcile",
        python_callable=branch_on_reconcile,
        doc_md="""
        ### Branch on Reconcile

        Decides whether to proceed with load or trigger failure alert.

        **Logic:**
        - If `reconcile_passed` is True → proceed to `load`
        - If `reconcile_passed` is False → proceed to `alert_reconcile_failure`

        **Returns:**
        - Task ID to execute next ('load' or 'alert_reconcile_failure')
        """,
    )

    # =========================================================================
    # Task 6: Load
    # =========================================================================

    load = PythonOperator(
        task_id="load",
        python_callable=load_task,
        doc_md="""
        ### Load to Parquet

        Persists the ledger DataFrame to the final Parquet output.

        **Inputs (via XCom from transform):**
        - `ledger_path`: Path to ledger data

        **Outputs (via XCom):**
        - `output_path`: Final output file path
        - `output_row_count`: Number of rows loaded
        - `load_duration`: Processing time in seconds
        """,
    )

    # =========================================================================
    # Task 7: Daily Aggregation
    # =========================================================================

    daily_agg = PythonOperator(
        task_id="daily_aggregation",
        python_callable=daily_aggregation_task,
        doc_md="""
        ### Daily Balance Aggregation

        Generates daily account balance aggregations from the ledger.

        **Aggregation:**
        - Group by: account_id, txn_date
        - Metrics: total_amount (sum), txn_count (count)

        **Outputs (via XCom):**
        - `aggregation_path`: Path to aggregated data
        - `aggregation_count`: Number of account-date combinations
        """,
    )

    # =========================================================================
    # Task 8: AML Detection
    # =========================================================================

    aml = PythonOperator(
        task_id="aml_detection",
        python_callable=aml_detection_task,
        trigger_rule=TriggerRule.ALL_DONE,  # Run even if daily_agg fails
        doc_md="""
        ### AML Detection

        Runs Anti-Money Laundering detection on the ledger data.

        **Detection Rules:**
        - Velocity checks (daily count, daily amount, 7-day amount)
        - Structuring detection (transactions near $10,000 CTR threshold)
        - Round number analysis (suspicious amounts)

        **Risk Scoring:**
        - Composite score 0-100 based on weighted rule triggers
        - Severity levels: none, low, medium, high

        **Outputs (via XCom):**
        - `aml_enabled`: Boolean indicating if AML was run
        - `aml_scored_path`: Path to scored transactions
        - `aml_alerts_path`: Path to alerts
        - `aml_scored_count`: Number of scored transactions
        - `aml_alert_count`: Number of alerts generated

        **Note:** This task is skipped if AML detection is disabled in config.
        """,
    )

    # =========================================================================
    # Task 9: Cleanup
    # =========================================================================

    cleanup = PythonOperator(
        task_id="cleanup_intermediates",
        python_callable=cleanup_intermediate_task,
        trigger_rule=TriggerRule.ALL_DONE,  # Always run, even on failure
        doc_md="""
        ### Cleanup Intermediate Files

        Deletes temporary intermediate Parquet files to free disk space.

        **Behavior:**
        - Runs regardless of upstream task status (all_done trigger)
        - Non-fatal: errors in cleanup don't fail the DAG
        - Deletes files from `data/intermediate/`

        **Outputs (via XCom):**
        - `cleanup_deleted_count`: Number of files deleted
        - `cleanup_errors`: Number of deletion errors
        """,
    )

    # =========================================================================
    # Task 10: Reconcile Failure Alert
    # =========================================================================

    alert_reconcile_failure = PythonOperator(
        task_id="alert_reconcile_failure",
        python_callable=alert_reconcile_failure_task,
        doc_md="""
        ### Reconcile Failure Alert

        Sends alerts when reconciliation fails.

        **Logic:**
        - Logs critical error
        - Sends console/webhook/email alerts if configured
        - Always raises AirflowFailException to mark DAG as failed

        **Triggered by:** branch_on_reconcile when reconciliation fails
        """,
    )

    # =========================================================================
    # Dependencies
    # =========================================================================

    # Main flow: wait → extract → transform → validate (parallel) → branch
    wait_for_file >> extract >> transform >> validate >> branch

    # Success branch: branch → load → daily_agg → aml → cleanup
    branch >> load >> daily_agg >> aml >> cleanup

    # Failure branch: branch → alert → (ends with failure)
    branch >> alert_reconcile_failure


# =============================================================================
# Helper Functions for Testing
# =============================================================================

if __name__ == "__main__":
    # For testing DAG structure
    dag.test()