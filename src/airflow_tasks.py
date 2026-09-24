"""Airflow-compatible task functions for the bank transaction pipeline.

This module provides task entry points that can be called from Apache Airflow DAGs.
Each task function accepts a TaskInstance (ti) for XCom access and returns values
that can be passed to downstream tasks via XCom.

Data passing strategy:
- XCom for metadata (paths, row counts, timestamps, status flags)
- Parquet files in data/intermediate/ for DataFrames
- Each task reads from previous task's output, writes to own output
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd

# Import Airflow exceptions (with fallback for local development)
try:
    from airflow.exceptions import AirflowFailException, AirflowSkipException
except ImportError:
    # Fallback for local development without Airflow installed
    class AirflowFailException(Exception):
        """Exception raised when a task should fail."""
        pass

    class AirflowSkipException(Exception):
        """Exception raised when a task should be skipped."""
        pass

# Project root setup for imports
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.extract import extract_transactions
from src.transform import transform_transactions
from src.load import load_ledger
from src.reconcile import reconcile_raw_vs_ledger
from src.config import load_config_validated
from src.quality import calculate_data_quality_metrics, export_data_quality_metrics, generate_rejection_report
from src.checkpoint import PipelineCheckpoint
from src.lineage import init_lineage_tracker, get_lineage_tracker
from src.sla_monitor import init_sla_monitor, SLAConfig
from src.schema_drift import init_drift_detector, SchemaDriftConfig
from src.alerting import init_alert_manager, AlertConfig, AlertChannel, AlertSeverity

# Logger configuration
LOGGER = logging.getLogger(__name__)

# Constants
INTERMEDIATE_DIR = BASE_DIR / "data" / "intermediate"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
DEFAULT_CONFIG_PATH = BASE_DIR / "config" / "pipeline_config.yaml"


def _get_config(config_path: Optional[str] = None) -> Any:
    """Load and return validated pipeline configuration."""
    path = config_path or str(DEFAULT_CONFIG_PATH)
    return load_config_validated(path)


def _ensure_dir(path: Path) -> Path:
    """Ensure directory exists, return path."""
    path.mkdir(parents=True, exist_ok=True)
    return path


def _timestamp_str() -> str:
    """Generate timestamp string for unique filenames."""
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


# =============================================================================
# Task 1: Extract
# =============================================================================

def extract_task(ti: Any, **context) -> str:
    """Extract raw transactions from CSV.

    Args:
        ti: Airflow TaskInstance for XCom access
        **context: Airflow context dictionary

    Returns:
        Path to intermediate Parquet file containing raw data

    Raises:
        AirflowFailException: If extraction fails
    """
    task_instance = context.get("ti") or ti
    run_id = context.get("run_id", _timestamp_str())

    try:
        LOGGER.info("Starting extract task - run_id: %s", run_id)

        # Load config
        config = _get_config()
        raw_path = BASE_DIR / config.paths.raw_transactions

        # Validate raw file exists
        if not raw_path.exists():
            raise AirflowFailException(f"Raw transactions file not found: {raw_path}")

        # Extract data
        chunksize = config.processing.chunksize
        raw_df = extract_transactions(
            str(raw_path),
            chunksize=chunksize,
            valid_txn_types=config.business_rules.valid_txn_types
        )

        row_count = len(raw_df)
        LOGGER.info("Extracted %d raw transactions", row_count)

        # Save intermediate output
        intermediate_path = _ensure_dir(INTERMEDIATE_DIR) / f"raw_extracted_{run_id}.parquet"
        raw_df.to_parquet(intermediate_path, index=False)
        LOGGER.info("Saved raw data to %s", intermediate_path)

        # Push to XCom
        task_instance.xcom_push(key="raw_path", value=str(intermediate_path))
        task_instance.xcom_push(key="row_count", value=row_count)
        task_instance.xcom_push(key="run_id", value=run_id)
        task_instance.xcom_push(key="extract_status", value="success")

        return str(intermediate_path)

    except Exception as exc:
        LOGGER.exception("Extract task failed: %s", exc)
        raise AirflowFailException(f"Extract task failed: {exc}") from exc


# =============================================================================
# Task 2: Transform
# =============================================================================

def transform_task(ti: Any, **context) -> dict[str, str]:
    """Transform raw data: normalize status/amounts, flag late arrivals.

    Args:
        ti: Airflow TaskInstance for XCom access
        **context: Airflow context dictionary

    Returns:
        Dictionary with paths to transformed and ledger DataFrames

    Raises:
        AirflowFailException: If transformation fails
        AirflowSkipException: If upstream task didn't produce data
    """
    task_instance = context.get("ti") or ti

    try:
        LOGGER.info("Starting transform task")

        # Pull from XCom
        raw_path = task_instance.xcom_pull(task_ids="extract", key="raw_path")
        if not raw_path:
            raise AirflowSkipException("No raw data available from extract task")

        # Load raw data
        raw_df = pd.read_parquet(raw_path)
        LOGGER.info("Loaded %d rows from %s", len(raw_df), raw_path)

        # Load config
        config = _get_config()
        success_statuses = set(config.business_rules.success_statuses)

        # Transform
        transform_start = datetime.now(timezone.utc)
        transformed_df, ledger_df = transform_transactions(raw_df, success_statuses)
        transform_duration = (datetime.now(timezone.utc) - transform_start).total_seconds()

        transformed_count = len(transformed_df)
        ledger_count = len(ledger_df)

        LOGGER.info(
            "Transformed: %d rows (cleaned), %d rows (ledger) in %.2fs",
            transformed_count, ledger_count, transform_duration
        )

        # Save intermediates
        run_id = task_instance.xcom_pull(task_ids="extract", key="run_id") or _timestamp_str()
        transformed_path = _ensure_dir(INTERMEDIATE_DIR) / f"transformed_{run_id}.parquet"
        ledger_path = _ensure_dir(INTERMEDIATE_DIR) / f"ledger_{run_id}.parquet"

        transformed_df.to_parquet(transformed_path, index=False)
        ledger_df.to_parquet(ledger_path, index=False)

        LOGGER.info("Saved transformed data to %s", transformed_path)
        LOGGER.info("Saved ledger data to %s", ledger_path)

        # Push to XCom
        task_instance.xcom_push(key="transformed_path", value=str(transformed_path))
        task_instance.xcom_push(key="ledger_path", value=str(ledger_path))
        task_instance.xcom_push(key="transformed_count", value=transformed_count)
        task_instance.xcom_push(key="ledger_count", value=ledger_count)
        task_instance.xcom_push(key="transform_duration", value=transform_duration)

        return {
            "transformed_path": str(transformed_path),
            "ledger_path": str(ledger_path),
            "transformed_count": transformed_count,
            "ledger_count": ledger_count
        }

    except AirflowSkipException:
        raise
    except Exception as exc:
        LOGGER.exception("Transform task failed: %s", exc)
        raise AirflowFailException(f"Transform task failed: {exc}") from exc


# =============================================================================
# Task 3: Quality Check
# =============================================================================

def quality_check_task(ti: Any, **context) -> dict[str, Any]:
    """Generate data quality metrics and rejection report.

    Args:
        ti: Airflow TaskInstance for XCom access
        **context: Airflow context dictionary

    Returns:
        Dictionary with quality metrics summary

    Raises:
        AirflowFailException: If quality check fails
    """
    task_instance = context.get("ti") or ti

    try:
        LOGGER.info("Starting quality check task")

        # Pull from XCom
        raw_path = task_instance.xcom_pull(task_ids="extract", key="raw_path")
        transformed_path = task_instance.xcom_pull(task_ids="transform", key="transformed_path")
        ledger_path = task_instance.xcom_pull(task_ids="transform", key="ledger_path")

        if not all([raw_path, transformed_path, ledger_path]):
            raise AirflowSkipException("Missing upstream data for quality check")

        # Load data
        raw_df = pd.read_parquet(raw_path)
        transformed_df = pd.read_parquet(transformed_path)
        ledger_df = pd.read_parquet(ledger_path)

        # Calculate quality metrics
        quality_metrics = calculate_data_quality_metrics(raw_df, transformed_df, ledger_df)

        # Export metrics
        metrics_path = _ensure_dir(PROCESSED_DIR) / "data_quality_metrics.json"
        export_data_quality_metrics(quality_metrics, metrics_path)

        # Generate rejection report
        rejection_path = _ensure_dir(PROCESSED_DIR) / "rejection_report.csv"
        generate_rejection_report(transformed_df, rejection_path)

        LOGGER.info("Quality metrics exported to %s", metrics_path)
        LOGGER.info("Rejection report exported to %s", rejection_path)

        # Determine if quality passes (configurable threshold)
        config = _get_config()
        rejection_rate = quality_metrics.get("rejection_rate", 0)
        rejection_threshold = getattr(config, "quality", {}).get("max_rejection_rate", 0.5)

        quality_passed = rejection_rate <= rejection_threshold

        # Push to XCom
        task_instance.xcom_push(key="quality_metrics_path", value=str(metrics_path))
        task_instance.xcom_push(key="rejection_report_path", value=str(rejection_path))
        task_instance.xcom_push(key="quality_passed", value=quality_passed)
        task_instance.xcom_push(key="rejection_rate", value=rejection_rate)
        task_instance.xcom_push(key="raw_row_count", value=quality_metrics.get("raw_row_count", 0))
        task_instance.xcom_push(key="clean_row_count", value=quality_metrics.get("clean_row_count", 0))
        task_instance.xcom_push(key="ledger_row_count", value=quality_metrics.get("ledger_row_count", 0))

        return {
            "quality_passed": quality_passed,
            "rejection_rate": rejection_rate,
            "metrics_path": str(metrics_path),
            "rejection_report_path": str(rejection_path),
            **quality_metrics
        }

    except AirflowSkipException:
        raise
    except Exception as exc:
        LOGGER.exception("Quality check task failed: %s", exc)
        raise AirflowFailException(f"Quality check task failed: {exc}") from exc


# =============================================================================
# Task 4: Reconcile
# =============================================================================

def reconcile_task(ti: Any, **context) -> dict[str, Any]:
    """Compare raw vs ledger totals and validate data integrity.

    Args:
        ti: Airflow TaskInstance for XCom access
        **context: Airflow context dictionary

    Returns:
        Dictionary with reconciliation results

    Raises:
        AirflowFailException: If reconciliation fails or mismatch detected
    """
    task_instance = context.get("ti") or ti

    try:
        LOGGER.info("Starting reconcile task")

        # Pull from XCom
        transformed_path = task_instance.xcom_pull(task_ids="transform", key="transformed_path")
        ledger_path = task_instance.xcom_pull(task_ids="transform", key="ledger_path")

        if not all([transformed_path, ledger_path]):
            raise AirflowSkipException("Missing transformed data for reconciliation")

        # Load data
        transformed_df = pd.read_parquet(transformed_path)
        ledger_df = pd.read_parquet(ledger_path)

        # Load config
        config = _get_config()
        tolerance = config.reconciliation.tolerance_amount

        # Reconcile
        recon_df = reconcile_raw_vs_ledger(transformed_df, ledger_df, tolerance)

        # Check for mismatches
        if not recon_df.empty:
            mismatches = recon_df[recon_df["diff"].abs() > tolerance]
            mismatch_count = len(mismatches)
        else:
            mismatch_count = 0

        reconcile_passed = mismatch_count == 0

        LOGGER.info("Reconciliation: %d dates compared, %d mismatches", len(recon_df), mismatch_count)

        # Save reconciliation report
        run_id = task_instance.xcom_pull(task_ids="extract", key="run_id") or _timestamp_str()
        recon_path = _ensure_dir(PROCESSED_DIR) / f"reconciliation_report_{run_id}.csv"
        recon_df.to_csv(recon_path, index=False)

        # Push to XCom
        task_instance.xcom_push(key="reconcile_passed", value=reconcile_passed)
        task_instance.xcom_push(key="recon_dates_count", value=len(recon_df))
        task_instance.xcom_push(key="mismatch_count", value=mismatch_count)
        task_instance.xcom_push(key="recon_report_path", value=str(recon_path))
        task_instance.xcom_push(key="tolerance", value=tolerance)

        # Fail if reconciliation fails and configured to fail
        if not reconcile_passed and config.reconciliation.fail_on_mismatch:
            raise AirflowFailException(
                f"Reconciliation failed: {mismatch_count} mismatches exceed tolerance {tolerance}"
            )

        return {
            "reconcile_passed": reconcile_passed,
            "dates_compared": len(recon_df),
            "mismatches": mismatch_count,
            "tolerance": tolerance,
            "report_path": str(recon_path)
        }

    except AirflowSkipException:
        raise
    except AirflowFailException:
        raise
    except Exception as exc:
        LOGGER.exception("Reconcile task failed: %s", exc)
        raise AirflowFailException(f"Reconcile task failed: {exc}") from exc


# =============================================================================
# Task 5: Branch on Reconcile
# =============================================================================

def branch_on_reconcile(ti: Any, **context) -> str:
    """Branch to load task or failure path based on reconciliation result.

    Args:
        ti: Airflow TaskInstance for XCom access
        **context: Airflow context dictionary

    Returns:
        Task ID to execute next ('load' or 'alert_reconcile_failure')
    """
    task_instance = context.get("ti") or ti

    try:
        # Note: reconcile task is inside TaskGroup, so full task_id is 'validate.reconcile'
        reconcile_passed = task_instance.xcom_pull(task_ids="validate.reconcile", key="reconcile_passed")

        if reconcile_passed:
            LOGGER.info("Reconciliation passed - proceeding to load task")
            return "load"
        else:
            LOGGER.warning("Reconciliation failed - proceeding to alert")
            return "alert_reconcile_failure"

    except Exception as exc:
        LOGGER.exception("Branch decision failed: %s", exc)
        # Default to alert on error
        return "alert_reconcile_failure"


# =============================================================================
# Task 6: Load
# =============================================================================

def load_task(ti: Any, **context) -> dict[str, Any]:
    """Persist ledger to Parquet output.

    Args:
        ti: Airflow TaskInstance for XCom access
        **context: Airflow context dictionary

    Returns:
        Dictionary with load results

    Raises:
        AirflowFailException: If load fails
    """
    task_instance = context.get("ti") or ti

    try:
        LOGGER.info("Starting load task")

        # Pull from XCom
        ledger_path = task_instance.xcom_pull(task_ids="transform", key="ledger_path")

        if not ledger_path:
            raise AirflowSkipException("No ledger data available for load")

        # Load ledger data
        ledger_df = pd.read_parquet(ledger_path)
        ledger_count = len(ledger_df)

        # Load config
        config = _get_config()
        output_path = BASE_DIR / config.paths.ledger_output

        # Persist ledger
        load_start = datetime.now(timezone.utc)
        final_path = load_ledger(ledger_df, str(output_path))
        load_duration = (datetime.now(timezone.utc) - load_start).total_seconds()

        LOGGER.info("Loaded %d rows to %s in %.2fs", ledger_count, final_path, load_duration)

        # Push to XCom
        task_instance.xcom_push(key="output_path", value=str(final_path))
        task_instance.xcom_push(key="output_row_count", value=ledger_count)
        task_instance.xcom_push(key="load_duration", value=load_duration)

        return {
            "output_path": str(final_path),
            "row_count": ledger_count,
            "duration_seconds": load_duration
        }

    except AirflowSkipException:
        raise
    except Exception as exc:
        LOGGER.exception("Load task failed: %s", exc)
        raise AirflowFailException(f"Load task failed: {exc}") from exc


# =============================================================================
# Task 7: Daily Aggregation
# =============================================================================

def daily_aggregation_task(ti: Any, **context) -> dict[str, Any]:
    """Generate daily account balance aggregations.

    Args:
        ti: Airflow TaskInstance for XCom access
        **context: Airflow context dictionary

    Returns:
        Dictionary with aggregation results

    Raises:
        AirflowFailException: If aggregation fails
    """
    task_instance = context.get("ti") or ti

    try:
        LOGGER.info("Starting daily aggregation task")

        # Pull from XCom
        ledger_path = task_instance.xcom_pull(task_ids="transform", key="ledger_path")

        if not ledger_path:
            raise AirflowSkipException("No ledger data available for aggregation")

        # Load ledger data
        ledger_df = pd.read_parquet(ledger_path)

        # Generate daily balance aggregation
        daily_balance_df = (
            ledger_df.groupby(["account_id", "txn_date"], observed=True)
            .agg(
                total_amount=pd.NamedAgg(column="amount", aggfunc="sum"),
                txn_count=pd.NamedAgg(column="txn_id", aggfunc="count"),
            )
            .reset_index()
        )

        # Sort for deterministic output
        daily_balance_df = daily_balance_df.sort_values(
            ["account_id", "txn_date"]
        ).reset_index(drop=True)

        # Save aggregation
        config = _get_config()
        agg_path = BASE_DIR / config.paths.daily_balance_output
        _ensure_dir(agg_path.parent)
        daily_balance_df.to_parquet(agg_path, index=False)

        LOGGER.info(
            "Generated daily balances: %d account-date combinations at %s",
            len(daily_balance_df), agg_path
        )

        # Push to XCom
        task_instance.xcom_push(key="aggregation_path", value=str(agg_path))
        task_instance.xcom_push(key="aggregation_count", value=len(daily_balance_df))

        return {
            "aggregation_path": str(agg_path),
            "aggregation_count": len(daily_balance_df)
        }

    except AirflowSkipException:
        raise
    except Exception as exc:
        LOGGER.exception("Daily aggregation task failed: %s", exc)
        raise AirflowFailException(f"Daily aggregation task failed: {exc}") from exc


# =============================================================================
# Task 8: AML Detection
# =============================================================================

def aml_detection_task(ti: Any, **context) -> Optional[dict[str, Any]]:
    """Run AML detection on ledger output (optional, based on config).

    Args:
        ti: Airflow TaskInstance for XCom access
        **context: Airflow context dictionary

    Returns:
        Dictionary with AML results or None if disabled

    Raises:
        AirflowFailException: If AML detection fails
    """
    task_instance = context.get("ti") or ti

    try:
        LOGGER.info("Starting AML detection task")

        # Check if AML is enabled in config
        config = _get_config()
        if not getattr(config, "aml_detection", None) or not config.aml_detection.enabled:
            LOGGER.info("AML detection disabled in config - skipping")
            task_instance.xcom_push(key="aml_enabled", value=False)
            return None

        # Pull from XCom
        ledger_path = task_instance.xcom_pull(task_ids="transform", key="ledger_path")
        if not ledger_path:
            # Try output path from load task
            ledger_path = task_instance.xcom_pull(task_ids="load", key="output_path")

        if not ledger_path:
            raise AirflowSkipException("No ledger data available for AML detection")

        # Import AML module
        from src.aml import run_aml_detection

        # Run AML detection
        aml_start = datetime.now(timezone.utc)
        aml_results = run_aml_detection(
            config=config,
            input_path=ledger_path
        )
        aml_duration = (datetime.now(timezone.utc) - aml_start).total_seconds()

        # Extract results
        scored_df = aml_results.get("scored_df", pd.DataFrame())
        alerts_df = aml_results.get("alerts_df", pd.DataFrame())

        # Get output paths from config
        scored_path = BASE_DIR / config.aml_detection.output_paths.scored_transactions
        alerts_path = BASE_DIR / config.aml_detection.output_paths.alerts

        LOGGER.info(
            "AML detection complete: %d scored transactions, %d alerts in %.2fs",
            len(scored_df), len(alerts_df), aml_duration
        )

        # Push to XCom
        task_instance.xcom_push(key="aml_enabled", value=True)
        task_instance.xcom_push(key="aml_scored_path", value=str(scored_path))
        task_instance.xcom_push(key="aml_alerts_path", value=str(alerts_path))
        task_instance.xcom_push(key="aml_scored_count", value=len(scored_df))
        task_instance.xcom_push(key="aml_alert_count", value=len(alerts_df))
        task_instance.xcom_push(key="aml_duration", value=aml_duration)

        return {
            "aml_enabled": True,
            "scored_path": str(scored_path),
            "alerts_path": str(alerts_path),
            "scored_count": len(scored_df),
            "alert_count": len(alerts_df),
            "duration_seconds": aml_duration
        }

    except AirflowSkipException:
        raise
    except Exception as exc:
        LOGGER.exception("AML detection task failed: %s", exc)
        raise AirflowFailException(f"AML detection task failed: {exc}") from exc


# =============================================================================
# Task 9: Cleanup
# =============================================================================

def cleanup_intermediate_task(ti: Any, **context) -> dict[str, int]:
    """Delete temporary intermediate files.

    This task runs regardless of upstream task status (all_done trigger).

    Args:
        ti: Airflow TaskInstance for XCom access
        **context: Airflow context dictionary

    Returns:
        Dictionary with cleanup results
    """
    task_instance = context.get("ti") or ti
    deleted_count = 0
    errors = []

    try:
        LOGGER.info("Starting cleanup task")

        # Get intermediate directory
        intermediate_dir = INTERMEDIATE_DIR

        if not intermediate_dir.exists():
            LOGGER.info("Intermediate directory does not exist - nothing to clean")
            return {"deleted_count": 0, "errors": 0}

        # Delete parquet files in intermediate directory
        for file_path in intermediate_dir.glob("*.parquet"):
            try:
                file_path.unlink()
                deleted_count += 1
                LOGGER.debug("Deleted %s", file_path)
            except Exception as exc:
                errors.append(str(exc))
                LOGGER.warning("Failed to delete %s: %s", file_path, exc)

        # Try to get run_id and clean specific files
        try:
            run_id = task_instance.xcom_pull(task_ids="extract", key="run_id")
            if run_id:
                # Clean any remaining files with this run_id
                for pattern in [f"*{run_id}*"]:
                    for file_path in intermediate_dir.glob(pattern):
                        try:
                            file_path.unlink()
                            deleted_count += 1
                        except Exception:
                            pass
        except Exception:
            pass

        LOGGER.info("Cleanup complete: %d files deleted, %d errors", deleted_count, len(errors))

        # Push to XCom
        task_instance.xcom_push(key="cleanup_deleted_count", value=deleted_count)
        task_instance.xcom_push(key="cleanup_errors", value=len(errors))

        return {"deleted_count": deleted_count, "errors": len(errors)}

    except Exception as exc:
        LOGGER.warning("Cleanup task encountered error (non-fatal): %s", exc)
        # Don't fail the DAG on cleanup errors
        return {"deleted_count": deleted_count, "errors": len(errors) + 1}


# =============================================================================
# Alert Tasks
# =============================================================================

def alert_reconcile_failure_task(ti: Any, **context) -> None:
    """Send alert when reconciliation fails.

    Args:
        ti: Airflow TaskInstance for XCom access
        **context: Airflow context dictionary
    """
    task_instance = context.get("ti") or ti

    try:
        LOGGER.error("Reconciliation failure alert triggered")

        # Get reconciliation details
        mismatch_count = task_instance.xcom_pull(task_ids="reconcile", key="mismatch_count") or "unknown"
        tolerance = task_instance.xcom_pull(task_ids="reconcile", key="tolerance") or "unknown"

        # Log alert (could be extended to email, Slack, etc.)
        LOGGER.error(
            "ALERT: Pipeline reconciliation failed - %s mismatches exceed tolerance %s",
            mismatch_count, tolerance
        )

        # Initialize alert manager and send alert if configured
        try:
            config = _get_config()
            if config.trust_layer.enabled and config.trust_layer.alerting.console:
                alert_manager = init_alert_manager(AlertConfig(
                    enabled=True,
                    channels=[AlertChannel.CONSOLE]
                ))
                alert_manager.alert_quality_issue(
                    title="Reconciliation Failure",
                    message=f"Pipeline failed reconciliation with {mismatch_count} mismatches",
                    severity=AlertSeverity.CRITICAL,
                    details={"mismatches": mismatch_count, "tolerance": tolerance}
                )
        except Exception:
            pass  # Don't fail if alerting fails

        # Always fail the task to mark DAG as failed
        raise AirflowFailException(
            f"Pipeline failed at reconciliation: {mismatch_count} mismatches exceed tolerance {tolerance}"
        )

    except AirflowFailException:
        raise
    except Exception as exc:
        LOGGER.exception("Alert task failed: %s", exc)
        raise AirflowFailException(f"Reconciliation failed and alert failed: {exc}")