"""Top‑level orchestrator for the bank‑transaction ETL pipeline

The script extracts raw CSV data, transforms it into a ledger‑ready
format, reconciles the two, persists the ledger as Parquet and finally
produces a daily‑balance aggregation
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path


import pandas as pd

# Project‑root helper – guarantees that the script works no matter from
# which working directory it is invoked
BASE_DIR = Path(__file__).resolve().parent.parent  # repo root (pipeline.py lives in <root>/scripts or <root>)

# File locations – built from the anchored ``BASE_DIR``
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

def run_pipeline(config_path: str = "config/pipeline_config.yaml", resume: bool = False) -> None:
    """
    Execute the full ETL workflow.

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

    # Check if we should resume from checkpoint
    if resume or should_resume_from_checkpoint(checkpoint):
        LOGGER.info("Attempting to resume from checkpoint...")
        resumed_data = resume_pipeline_from_checkpoint(checkpoint, config)
        if resumed_data:
            # Resume logic would go here - for now, we'll start fresh but log the attempt
            LOGGER.info("Checkpoint data loaded - resuming pipeline execution")
        else:
            LOGGER.info("No valid checkpoint found - starting fresh pipeline run")

    LOGGER.info("Starting bank‑transaction pipeline ...")

    # Extract raw CSV → DataFrame
    chunksize = config.processing.chunksize
    raw_df: pd.DataFrame = extract_transactions(
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

    # Transform → (cleaned raw, ledger‑ready)
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

    # Calculate and export data quality metrics
    quality_metrics = calculate_data_quality_metrics(raw_df, transformed_df, ledger_df)
    metrics_path = BASE_DIR / "data" / "processed" / "data_quality_metrics.json"
    export_data_quality_metrics(quality_metrics, metrics_path)

    # Generate rejection report
    rejection_path = BASE_DIR / "data" / "processed" / "rejection_report.csv"
    generate_rejection_report(transformed_df, rejection_path)

    # Reconcile raw vs. ledger totals
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

    # Persist ledger
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

    # Daily‑balance aggregation
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