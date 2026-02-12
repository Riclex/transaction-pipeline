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
        success_statuses = set(config.business_rules.success_statuses)
        transformed_df, ledger_df = transform_transactions(raw_df, success_statuses)
        LOGGER.info(
            "Transformed – %d rows in cleaned data, %d rows in ledger ✔",
            len(transformed_df),
            len(ledger_df),
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
        tolerance = config.reconciliation.tolerance_amount
        recon_df = reconcile_raw_vs_ledger(transformed_df, ledger_df, tolerance)
        LOGGER.info("Reconciliation passed – %d dates compared ✔", len(recon_df))

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
        ledger_file = load_ledger(ledger_df, str(ledger_path))
        LOGGER.info("Ledger persisted at %s ✔", ledger_file)

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

    LOGGER.info("Pipeline completed successfully ✔")

    # Clear checkpoints on successful completion
    checkpoint.clear_checkpoint()

# Entrypoint – catch any unexpected exception, log it and exit with code 1
if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as exc:  # pragma: no cover – exercised in integration tests
        LOGGER.exception("Pipeline failed: %s ✖", exc)
        sys.exit(1)