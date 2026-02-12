"""Top‑level orchestrator for the bank‑transaction ETL pipeline

The script extracts raw CSV data, transforms it into a ledger‑ready
format, reconciles the two, persists the ledger as Parquet and finally
produces a daily‑balance aggregation
"""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd

# Project‑root helper – guarantees the script works from 
# any working directory it is invoked
BASE_DIR = Path(__file__).resolve().parent.parent  # repo root (pipeline.py lives in <root>/scripts or <root>)

# File locations anchored ``BASE_DIR``
RAW_PATH: Path = BASE_DIR / "data" / "raw" / "transactions_raw.csv"
LEDGER_PATH: Path = BASE_DIR / "data" / "processed" / "ledger_transactions.parquet"
AGG_PATH: Path = BASE_DIR / "data" / "processed" / "daily_account_balance.parquet"

from src.extract import extract_transactions
from src.transform import transform_transactions
from src.load import load_ledger
from src.reconcile import reconcile_raw_vs_ledger
from src.config import load_config_validated
from src.quality import calculate_data_quality_metrics, export_data_quality_metrics, generate_rejection_report
from src.checkpoint import PipelineCheckpoint, should_resume_from_checkpoint, resume_pipeline_from_checkpoint
from src.lineage import init_lineage_tracker, get_lineage_tracker
from src.sla_monitor import init_sla_monitor, get_sla_monitor, SLAConfig
from src.schema_drift import init_drift_detector, get_drift_detector, SchemaDriftConfig
from src.alerting import init_alert_manager, get_alert_manager, AlertConfig, AlertChannel, AlertSeverity
from src.data_contract import init_contract_enforcer, get_contract_enforcer

# Logging configuration
LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


@dataclass
class ResumeState:
    """Tracks the current resume state of the pipeline.

    Attributes
    completed_steps : set[str]
        Set of steps that have been completed (extract, transform, reconcile, load)
    raw_df : pd.DataFrame | None
        Raw data loaded from extract step
    transformed_df : pd.DataFrame | None
        Transformed data from transform step
    ledger_df : pd.DataFrame | None
        Ledger data from transform step
    recon_df : pd.DataFrame | None
        Reconciliation results from reconcile step
    """
    completed_steps: set[str] = field(default_factory=set)
    raw_df: Optional[pd.DataFrame] = None
    transformed_df: Optional[pd.DataFrame] = None
    ledger_df: Optional[pd.DataFrame] = None
    recon_df: Optional[pd.DataFrame] = None

    def is_step_completed(self, step: str) -> bool:
        return step in self.completed_steps

    def add_completed_step(self, step: str) -> None:
        self.completed_steps.add(step)


def load_resume_state(checkpoint: PipelineCheckpoint) -> ResumeState:
    """Load resume state from checkpoint.

    Parameters
    checkpoint : PipelineCheckpoint
        The checkpoint manager

    Returns
    ResumeState
        The loaded resume state with data from checkpoint
    """
    state = ResumeState()
    step, data = checkpoint.load_checkpoint()

    if step is None or data is None:
        return state

    # Mark steps up to and including the checkpointed step as completed
    step_order = ["extract", "transform", "reconcile", "load"]
    for s in step_order:
        state.add_completed_step(s)
        if s == step:
            break

    # Restore data from checkpoint
    if "raw_df" in data:
        state.raw_df = data["raw_df"]
    if "transformed_df" in data:
        state.transformed_df = data["transformed_df"]
    if "ledger_df" in data:
        state.ledger_df = data["ledger_df"]
    if "recon_df" in data:
        state.recon_df = data["recon_df"]

    LOGGER.info("Resume state loaded - completed steps: %s", state.completed_steps)
    return state

def run_pipeline(config_path: str = "config/pipeline_config.yaml", resume: bool = False) -> None:
    """
    Execute the full ETL workflow with checkpoint and resume support.

    Parameters
    config_path : str, optional
        Path to the pipeline configuration file. Defaults to "config/pipeline_config.yaml"
    resume : bool, optional
        Whether to attempt resuming from a checkpoint. Defaults to False

    Raises
    SystemExit
        The function never returns a value; it either finishes silently
        (logging its progress) or exits the interpreter with a non‑zero
        status code after logging the exception
    """
    # Load configuration
    config = load_config_validated(config_path)
    LOGGER.info("Loaded and validated configuration from %s", config_path)

    # Extract paths from config
    raw_path = BASE_DIR / config.paths.raw_transactions
    ledger_path = BASE_DIR / config.paths.ledger_output
    agg_path = BASE_DIR / config.paths.daily_balance_output

    # Initialize checkpoint manager
    checkpoint = PipelineCheckpoint(BASE_DIR / "data" / "checkpoints")

    # Initialize data trust layer if enabled
    lineage_tracker = None
    sla_monitor = None
    drift_detector = None
    alert_manager = None
    contract_enforcer = None

    if config.trust_layer.enabled:
        LOGGER.info("Initializing data trust layer...")

        # Initialize alerting first (other components use it)
        alert_channels = []
        if config.trust_layer.alerting.console:
            alert_channels.append(AlertChannel.CONSOLE)
        if config.trust_layer.alerting.webhook:
            alert_channels.append(AlertChannel.WEBHOOK)
        if config.trust_layer.alerting.email:
            alert_channels.append(AlertChannel.EMAIL)
        if config.trust_layer.alerting.file:
            alert_channels.append(AlertChannel.FILE)

        alert_manager = init_alert_manager(AlertConfig(
            enabled=True,
            channels=alert_channels,
            webhook_url=config.trust_layer.alerting.webhook_url,
            email_smtp_host=config.trust_layer.alerting.email_smtp_host,
            email_smtp_port=config.trust_layer.alerting.email_smtp_port,
            email_username=config.trust_layer.alerting.email_username,
            email_password=config.trust_layer.alerting.email_password,
            email_from=config.trust_layer.alerting.email_from,
            email_to=config.trust_layer.alerting.email_to,
            alert_file_path=(
                Path(config.trust_layer.alerting.file_path)
                if config.trust_layer.alerting.file_path else None
            ),
            throttle_minutes=config.trust_layer.alert_throttle_minutes,
        ))

        # Initialize lineage tracking
        lineage_tracker = init_lineage_tracker(BASE_DIR / config.trust_layer.lineage_dir)

        # Initialize SLA monitoring
        sla_monitor = init_sla_monitor(
            SLAConfig(
                max_data_age_hours=config.trust_layer.sla_max_data_age_hours,
                max_processing_time_minutes=config.trust_layer.sla_max_processing_time_minutes,
            ),
            BASE_DIR / "data" / "sla"
        )

        # Initialize schema drift detection
        drift_detector = init_drift_detector(
            SchemaDriftConfig(
                track_column_additions=config.trust_layer.drift_track_additions,
                track_column_removals=config.trust_layer.drift_track_removals,
                track_type_changes=config.trust_layer.drift_track_type_changes,
                critical_columns=config.trust_layer.drift_critical_columns,
            ),
            BASE_DIR / config.trust_layer.drift_detection_dir
        )

        # Initialize contract enforcer
        contract_enforcer = init_contract_enforcer(BASE_DIR / config.trust_layer.contracts_dir)

        LOGGER.info("Data trust layer initialized ✔")

    # Determine if we should resume and load resume state
    resume_state = ResumeState()
    should_resume = resume or should_resume_from_checkpoint(checkpoint)

    if should_resume:
        LOGGER.info("Attempting to resume from checkpoint...")
        resume_state = load_resume_state(checkpoint)
        if resume_state.completed_steps:
            LOGGER.info(
                "Resuming pipeline execution from checkpoint - completed steps: %s",
                resume_state.completed_steps
            )
        else:
            LOGGER.info("No valid checkpoint found - starting fresh pipeline run")
    else:
        # Clear any existing checkpoints if not resuming
        checkpoint.clear_checkpoint()

    LOGGER.info("Starting bank‑transaction pipeline ...")
    pipeline_start_time = pd.Timestamp.now()

    # STEP 1: EXTRACT
    if resume_state.is_step_completed("extract"):
        LOGGER.info("Skipping extract step - using checkpointed data")
        raw_df = resume_state.raw_df
        if raw_df is None:
            raise ValueError("Resume state missing raw_df for extract step")
    else:
        chunksize = config.processing.chunksize
        raw_df = extract_transactions(
            str(raw_path),
            chunksize=chunksize,
            valid_txn_types=config.business_rules.valid_txn_types
        )  # str works for legacy API
        LOGGER.info("Extracted %d raw transactions ✔", len(raw_df))

        # Register lineage for source
        if lineage_tracker:
            source_node_id = lineage_tracker.register_source(
                name="raw_transactions",
                file_path=raw_path,
                df=raw_df,
                metadata={"chunksize": chunksize, "valid_types": config.business_rules.valid_txn_types}
            )
            LOGGER.debug("Registered lineage source node: %s", source_node_id)

        # Detect schema drift for raw data
        if drift_detector:
            events, has_critical = drift_detector.detect_drift(raw_df, "raw_transactions")
            if events:
                LOGGER.warning("Schema drift detected in raw data: %d events", len(events))
                if alert_manager:
                    for event in events:
                        severity = AlertSeverity.CRITICAL if event.severity == "critical" else AlertSeverity.WARNING
                        alert_manager.alert_schema_drift(
                            title=f"Schema Drift: {event.event_type}",
                            message=f"Column '{event.column_name}': {event.description}",
                            severity=severity,
                            details={"event": event.__dict__}
                        )

        # Check data freshness
        if sla_monitor and config.trust_layer.sla_check_freshness:
            from datetime import datetime
            raw_file_mtime = datetime.fromtimestamp(raw_path.stat().st_mtime)
            freshness_report = sla_monitor.check_data_freshness("raw_transactions", raw_file_mtime)
            if freshness_report.status == "stale":
                LOGGER.warning("Raw data is stale: %s", freshness_report.violations)
                if alert_manager:
                    alert_manager.alert_sla_violation(
                        title="Data Freshness SLA Violation",
                        message=f"Raw data is {freshness_report.data_age_hours:.1f} hours old",
                        details={"violations": freshness_report.violations}
                    )

        # Save checkpoint after extraction
        checkpoint.save_checkpoint(
            "extract",
            {"raw_df": raw_df},
            {"raw_path": str(raw_path), "row_count": len(raw_df)}
        )

    # STEP 2: TRANSFORM
    if resume_state.is_step_completed("transform"):
        LOGGER.info("Skipping transform step - using checkpointed data")
        transformed_df = resume_state.transformed_df
        ledger_df = resume_state.ledger_df
        if transformed_df is None or ledger_df is None:
            raise ValueError("Resume state missing transformed data for transform step")
    else:
        transform_start = pd.Timestamp.now()
        success_statuses = set(config.business_rules.success_statuses)
        transformed_df, ledger_df = transform_transactions(raw_df, success_statuses)
        transform_duration = (pd.Timestamp.now() - transform_start).total_seconds()
        LOGGER.info(
            "Transformed – %d rows in cleaned data, %d rows in ledger ✔",
            len(transformed_df),
            len(ledger_df),
        )

        # Register lineage for transformation
        if lineage_tracker:
            transform_node_id = lineage_tracker.register_transformation(
                name="transform_transactions",
                operation="standardize_and_filter",
                input_df=raw_df,
                output_df=transformed_df,
                parent_node_id=source_node_id if 'source_node_id' in locals() else None,
                metadata={
                    "success_statuses": list(success_statuses),
                    "rejected_rows": len(raw_df) - len(transformed_df),
                    "duration_seconds": transform_duration
                }
            )
            ledger_node_id = lineage_tracker.register_transformation(
                name="filter_ledger",
                operation="filter_success_status",
                input_df=transformed_df,
                output_df=ledger_df,
                parent_node_id=transform_node_id,
                metadata={"ledger_rows": len(ledger_df)}
            )
            LOGGER.debug("Registered lineage transform nodes: %s, %s", transform_node_id, ledger_node_id)

        # Detect schema drift for transformed data
        if drift_detector:
            for df_name, df in [("transformed", transformed_df), ("ledger", ledger_df)]:
                events, has_critical = drift_detector.detect_drift(df, df_name)
                if events:
                    LOGGER.warning("Schema drift detected in %s data: %d events", df_name, len(events))
                    if alert_manager:
                        for event in events:
                            severity = AlertSeverity.CRITICAL if event.severity == "critical" else AlertSeverity.WARNING
                            alert_manager.alert_schema_drift(
                                title=f"Schema Drift in {df_name}: {event.event_type}",
                                message=f"Column '{event.column_name}': {event.description}",
                                severity=severity,
                                details={"dataset": df_name, "event": event.__dict__}
                            )

        # Track processing latency
        if sla_monitor:
            sla_monitor.check_processing_latency(
                "transform_step",
                transform_start.to_pydatetime(),
                pd.Timestamp.now().to_pydatetime()
            )

        # Save checkpoint after transformation
        checkpoint.save_checkpoint(
            "transform",
            {"raw_df": raw_df, "transformed_df": transformed_df, "ledger_df": ledger_df},
            {"transformed_rows": len(transformed_df), "ledger_rows": len(ledger_df)}
        )

    # Calculate and export data quality metrics (always run, idempotent)
    quality_metrics = calculate_data_quality_metrics(raw_df, transformed_df, ledger_df)
    metrics_path = BASE_DIR / "data" / "processed" / "data_quality_metrics.json"
    export_data_quality_metrics(quality_metrics, metrics_path)

    # Generate rejection report (always run, idempotent)
    rejection_path = BASE_DIR / "data" / "processed" / "rejection_report.csv"
    generate_rejection_report(transformed_df, rejection_path)

    # STEP 3: RECONCILE
    if resume_state.is_step_completed("reconcile"):
        LOGGER.info("Skipping reconcile step - using checkpointed data")
        recon_df = resume_state.recon_df
        if recon_df is None:
            raise ValueError("Resume state missing recon_df for reconcile step")
    else:
        recon_start = pd.Timestamp.now()
        tolerance = config.reconciliation.tolerance_amount
        recon_df = reconcile_raw_vs_ledger(transformed_df, ledger_df, tolerance)
        LOGGER.info("Reconciliation passed – %d dates compared ✔", len(recon_df))

        # Register lineage for reconciliation
        if lineage_tracker:
            recon_node_id = lineage_tracker.register_transformation(
                name="reconcile",
                operation="validate_raw_vs_ledger",
                input_df=transformed_df,
                output_df=recon_df,
                parent_node_id=ledger_node_id if 'ledger_node_id' in locals() else None,
                metadata={
                    "dates_compared": len(recon_df),
                    "tolerance": tolerance,
                    "mismatches": len(recon_df[recon_df["diff"].abs() > tolerance]) if not recon_df.empty else 0
                }
            )
            LOGGER.debug("Registered lineage reconciliation node: %s", recon_node_id)

        # Check for reconciliation mismatches and alert
        if not recon_df.empty and alert_manager:
            mismatches = recon_df[recon_df["diff"].abs() > tolerance]
            if not mismatches.empty:
                LOGGER.error("Reconciliation mismatches detected: %d", len(mismatches))
                alert_manager.alert_quality_issue(
                    title="Reconciliation Mismatch",
                    message=f"{len(mismatches)} dates have reconciliation mismatches exceeding tolerance",
                    severity=AlertSeverity.ERROR,
                    details={
                        "mismatches": len(mismatches),
                        "tolerance": tolerance,
                        "sample_mismatches": mismatches.head(5).to_dict()
                    }
                )

        # Track processing latency
        if sla_monitor:
            sla_monitor.check_processing_latency(
                "reconcile_step",
                recon_start.to_pydatetime(),
                pd.Timestamp.now().to_pydatetime()
            )

        # Save checkpoint after reconciliation
        checkpoint.save_checkpoint(
            "reconcile",
            {
                "raw_df": raw_df,
                "transformed_df": transformed_df,
                "ledger_df": ledger_df,
                "recon_df": recon_df
            },
            {"recon_dates": len(recon_df), "tolerance": tolerance}
        )

    # STEP 4: LOAD (LEDGER)
    if resume_state.is_step_completed("load"):
        LOGGER.info("Skipping load step - already completed")
        # Don't re-load the ledger file, but make sure
        # the aggregation step has access to ledger_df
        if ledger_df is None:
            raise ValueError("Resume state missing ledger_df for load step")
        ledger_file = ledger_path  # Assume file exists from previous run
    else:
        load_start = pd.Timestamp.now()
        ledger_file = load_ledger(ledger_df, str(ledger_path))
        load_duration = (pd.Timestamp.now() - load_start).total_seconds()
        LOGGER.info("Ledger persisted at %s ✔", ledger_file)

        # Register lineage for sink
        if lineage_tracker:
            sink_node_id = lineage_tracker.register_sink(
                name="ledger_output",
                file_path=ledger_path,
                df=ledger_df,
                parent_node_id=recon_node_id if 'recon_node_id' in locals() else None,
                metadata={
                    "file_format": "parquet",
                    "load_duration_seconds": load_duration
                }
            )
            LOGGER.debug("Registered lineage sink node: %s", sink_node_id)

            # Export lineage graph
            lineage_export_path = lineage_tracker.export_lineage()
            LOGGER.info("Data lineage exported to %s", lineage_export_path)

        # Track processing latency
        if sla_monitor:
            sla_monitor.check_processing_latency(
                "load_step",
                load_start.to_pydatetime(),
                pd.Timestamp.now().to_pydatetime()
            )

        # Save checkpoint after loading
        checkpoint.save_checkpoint(
            "load",
            {
                "raw_df": raw_df,
                "transformed_df": transformed_df,
                "ledger_df": ledger_df,
                "recon_df": recon_df
            },
            {"ledger_file": str(ledger_file)}
        )

    # STEP 5: DAILY BALANCE AGGREGATION (always run - idempotent)
    daily_balance_df = (
        ledger_df.groupby(["account_id", "txn_date"], observed=True)
        .agg(
            total_amount=pd.NamedAgg(column="amount", aggfunc="sum"),
            txn_count=pd.NamedAgg(column="txn_id", aggfunc="count"),
        )
        .reset_index()
    )

    # Deterministic ordering makes the parquet file stable for diff‑testing
    daily_balance_df = daily_balance_df.sort_values(
        ["account_id", "txn_date"]
    ).reset_index(drop=True)

    # Ensure the destination directory exists before writing.
    agg_path.parent.mkdir(parents=True, exist_ok=True)
    daily_balance_df.to_parquet(agg_path, index=False)
    LOGGER.info("📊 Daily account‑balance generated at %s", agg_path)

    # Calculate total pipeline duration
    pipeline_end_time = pd.Timestamp.now()
    total_duration = (pipeline_end_time - pipeline_start).total_seconds()

    LOGGER.info("Pipeline completed successfully in %.2f seconds ✔", total_duration)

    # Generate SLA summary
    if sla_monitor:
        sla_summary = sla_monitor.get_sla_summary()
        LOGGER.info("SLA Summary: %s", sla_summary)

        # Export SLA report
        sla_report_path = sla_monitor.generate_sla_report()
        LOGGER.info("SLA report exported to %s", sla_report_path)

        # Alert on SLA violations
        if sla_summary["violation_count"] > 0 and alert_manager:
            alert_manager.alert_sla_violation(
                title="Pipeline SLA Violations",
                message=f"{sla_summary['violation_count']} SLA violations detected during pipeline run",
                details=sla_summary
            )

    # Generate drift report if drift detector is active
    if drift_detector:
        drift_summary = drift_detector.get_drift_summary()
        LOGGER.info("Schema Drift Summary: %s", drift_summary)
        if drift_summary["critical_count"] > 0:
            LOGGER.error("Critical schema drift events detected: %d", drift_summary["critical_count"])

    # Send pipeline success alert
    if alert_manager:
        alert_manager.send_alert(
            severity=AlertSeverity.INFO,
            category="pipeline",
            title="Pipeline Completed Successfully",
            message=f"Pipeline completed in {total_duration:.2f} seconds",
            source="pipeline",
            details={
                "duration_seconds": total_duration,
                "rows_processed": len(raw_df) if 'raw_df' in locals() else 0,
                "ledger_rows": len(ledger_df) if 'ledger_df' in locals() else 0
            }
        )

    # Clear checkpoints on successful completion
    checkpoint.clear_checkpoint()


# Entrypoint – catch any unexpected exception, log it and exit with code 1
if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as exc:  # pragma: no cover – exercised in integration tests
        LOGGER.exception("Pipeline failed: %s ✖", exc)

        # Send pipeline failure alert if alerting is available
        try:
            alert_mgr = get_alert_manager()
            if alert_mgr:
                alert_mgr.alert_pipeline_failure(
                    title="Pipeline Execution Failed",
                    message=f"Pipeline failed with error: {exc}",
                    details={"error": str(exc), "error_type": type(exc).__name__}
                )
        except Exception:
            pass  # Don't fail on alert error

        sys.exit(1)