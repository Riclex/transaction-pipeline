"""Data quality metrics and reporting utilities

This module provides functions to calculate and export data quality metrics
including row counts, rejection reasons, and other quality indicators.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, Any, List
import pandas as pd

# Logging
LOGGER = logging.getLogger(__name__)


def calculate_data_quality_metrics(
    raw_df: pd.DataFrame,
    transformed_df: pd.DataFrame,
    ledger_df: pd.DataFrame,
) -> Dict[str, Any]:
    """
    Calculate comprehensive data quality metrics for the pipeline.

    Parameters
    raw_df : pd.DataFrame
        The original raw data
    transformed_df : pd.DataFrame
        The transformed data with cleaning flags
    ledger_df : pd.DataFrame
        The final ledger data (successful transactions only)

    Returns
    Dict[str, Any]
        Dictionary containing various data quality metrics
    """
    metrics = {
        "row_counts": {
            "raw_total": len(raw_df),
            "transformed_total": len(transformed_df),
            "ledger_total": len(ledger_df),
            "skipped_total": len(transformed_df) - len(ledger_df),
        },
        "rejection_reasons": {},
        "data_quality_flags": {},
    }

    # Calculate rejection reasons based on status_clean
    if "status_clean" in transformed_df.columns:
        status_counts = transformed_df["status_clean"].value_counts().to_dict()
        metrics["rejection_reasons"]["by_status"] = status_counts

        # Calculate rejection rate
        total_failed = sum(count for status, count in status_counts.items() if status != "SUCCESS")
        metrics["rejection_reasons"]["total_failed"] = total_failed
        metrics["rejection_reasons"]["failure_rate"] = (
            total_failed / len(transformed_df) if len(transformed_df) > 0 else 0
        )

    # Check for data quality issues
    quality_flags = {}

    # Check for missing values in critical columns
    critical_columns = ["txn_id", "account_id", "txn_date", "amount_clean"]
    for col in critical_columns:
        if col in transformed_df.columns:
            missing_count = transformed_df[col].isna().sum()
            quality_flags[f"{col}_missing"] = missing_count
            if missing_count > 0:
                LOGGER.warning(f"Found {missing_count} missing values in {col}")

    # Check for duplicate transaction IDs
    if "txn_id" in transformed_df.columns:
        dup_count = transformed_df["txn_id"].duplicated().sum()
        quality_flags["duplicate_txn_ids"] = dup_count
        if dup_count > 0:
            LOGGER.warning(f"Found {dup_count} duplicate transaction IDs")

    # Check for late arrivals
    if "is_late" in transformed_df.columns:
        late_count = transformed_df["is_late"].sum()
        quality_flags["late_arrivals"] = late_count
        if late_count > 0:
            LOGGER.info(f"Found {late_count} late-arriving transactions")

    # Check amount distributions
    if "amount_clean" in transformed_df.columns:
        amount_stats = transformed_df["amount_clean"].describe().to_dict()
        quality_flags["amount_statistics"] = amount_stats

        # Flag potential outliers (amounts > 3 standard deviations from mean)
        mean_amount = transformed_df["amount_clean"].mean()
        std_amount = transformed_df["amount_clean"].std()
        outlier_threshold = 3 * std_amount
        outlier_count = (
            (transformed_df["amount_clean"] - mean_amount).abs() > outlier_threshold
        ).sum()
        quality_flags["amount_outliers"] = outlier_count

    metrics["data_quality_flags"] = quality_flags

    return metrics


def export_data_quality_metrics(
    metrics: Dict[str, Any],
    output_path: str | Path,
) -> Path:
    """
    Export data quality metrics to a JSON file.

    Parameters
    metrics : Dict[str, Any]
        The metrics dictionary from calculate_data_quality_metrics
    output_path : str | Path
        Path where to save the metrics JSON file

    Returns
    Path
        The absolute path of the exported file
    """
    output_path = Path(output_path)

    # Ensure parent directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Export to JSON with nice formatting
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, default=str)

    LOGGER.info("📊 Data quality metrics exported to %s", output_path)
    return output_path


def generate_rejection_report(
    transformed_df: pd.DataFrame,
    output_path: str | Path,
) -> Path:
    """
    Generate a detailed rejection report showing why transactions were rejected.

    Parameters
    transformed_df : pd.DataFrame
        The transformed dataframe with status_clean and other flags
    output_path : str | Path
        Path where to save the rejection report

    Returns
    Path
        The absolute path of the exported file
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Create rejection report for failed transactions
    failed_transactions = transformed_df[transformed_df["status_clean"] != "SUCCESS"].copy()

    if len(failed_transactions) > 0:
        # Group by rejection reason (status)
        rejection_summary = (
            failed_transactions.groupby("status_clean")
            .agg({
                "txn_id": "count",
                "account_id": lambda x: x.nunique(),
                "amount_clean": ["sum", "mean"],
            })
            .round(2)
        )

        # Flatten column names
        rejection_summary.columns = [
            f"{col[0]}_{col[1]}" if col[1] else col[0]
            for col in rejection_summary.columns
        ]
        rejection_summary = rejection_summary.reset_index()

        # Export to CSV
        rejection_summary.to_csv(output_path, index=False)
        LOGGER.info("Rejection report exported to %s", output_path)
    else:
        # Create empty report if no rejections
        pd.DataFrame(columns=["status_clean", "txn_id_count", "account_id_nunique", "amount_clean_sum", "amount_clean_mean"]).to_csv(output_path, index=False)
        LOGGER.info("No rejections found - empty rejection report created at %s", output_path)

    return output_path