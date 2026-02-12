"""AML (Anti-Money Laundering) Detection Module.

This module provides post-processing AML detection on the ledger output
from the main transaction pipeline. It consumes the ledger Parquet file,
computes risk features, applies rules, and generates alerts.

Example
Standalone usage::

    from src.aml import run_aml_detection
    from src.config import load_config_validated

    config = load_config_validated("config/pipeline_config.yaml")
    run_aml_detection(config)

Integrated pipeline usage::

    # After main pipeline completes
    from src.aml import run_aml_detection
    run_aml_detection(config)  # Uses ledger output path from config
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, Dict, Any

import pandas as pd

from src.aml.loader import load_ledger_for_aml, AmlSchemaError
from src.aml.features import build_all_features
from src.aml.rules import AmlRuleEngine
from src.aml.alerts import AlertManager

# Logging
LOGGER = logging.getLogger(__name__)


class AmlDetectionError(Exception):
    """Raised when AML detection fails."""
    pass


def run_aml_detection(
    config,
    input_path: Optional[str] = None,
    output_dir: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Run full AML detection pipeline on ledger output.

    This is the main entry point for AML analysis. It reads the ledger
    Parquet file, computes features, applies rules, and outputs scored
    transactions and alerts.

    Parameters
    config : PipelineConfig or dict
        Pipeline configuration object or dict with aml_detection section
    input_path : str, optional
        Override path to ledger Parquet file. If not provided, uses
        config.aml_detection.input_path
    output_dir : str, optional
        Override output directory. If not provided, uses paths from config

    Returns
    dict
        Results dictionary containing:
        - scored_transactions_path: Path to scored transactions Parquet
        - alerts_path: Path to alerts Parquet
        - alert_count: Number of alerts generated
        - high_risk_count: Number of high-risk transactions
        - metrics: AML-specific metrics dict

    Raises
    AmlDetectionError
        If AML detection fails
    AmlSchemaError
        If ledger schema validation fails
    """
    try:
        # Extract AML config
        if hasattr(config, "aml_detection"):
            aml_config = config.aml_detection
            aml_dict = aml_config.dict() if hasattr(aml_config, "dict") else aml_config
        else:
            aml_dict = config.get("aml_detection", {})

        if not aml_dict.get("enabled", True):
            LOGGER.info("AML detection is disabled in configuration")
            return {"status": "disabled"}

        # Resolve paths
        base_dir = Path(__file__).parent.parent.parent

        if input_path:
            ledger_path = Path(input_path)
        else:
            ledger_path = base_dir / aml_dict.get("input_path", "data/processed/ledger_transactions.parquet")

        output_paths = aml_dict.get("output_paths", {})

        if output_dir:
            out_dir = Path(output_dir)
            scored_path = out_dir / "aml_scored_transactions.parquet"
            alerts_path = out_dir / "aml_alerts.parquet"
        else:
            scored_path = base_dir / output_paths.get(
                "scored_transactions", "data/processed/aml_scored_transactions.parquet"
            )
            alerts_path = base_dir / output_paths.get(
                "alerts", "data/processed/aml_alerts.parquet"
            )

        LOGGER.info("Starting AML detection on %s", ledger_path)

        # Step 1: Load ledger
        ledger_df = load_ledger_for_aml(ledger_path)

        # Step 2: Feature engineering
        featured_df = build_all_features(ledger_df, aml_dict)

        # Step 3: Apply rules and score
        engine = AmlRuleEngine(aml_dict)
        scored_df = engine.apply_all_rules(featured_df)

        # Step 4: Generate alerts
        alert_manager = AlertManager()
        alerts_df = alert_manager.generate_alerts(scored_df)

        # Step 5: Save outputs
        scored_path.parent.mkdir(parents=True, exist_ok=True)
        scored_df.to_parquet(scored_path, index=False)
        LOGGER.info("Scored transactions saved to %s", scored_path)

        alert_manager.save_alerts(alerts_df, alerts_path)

        # Calculate metrics
        metrics = {
            "total_transactions": len(scored_df),
            "alerts_generated": len(alerts_df),
            "high_risk_transactions": (scored_df["severity"] == "high").sum(),
            "medium_risk_transactions": (scored_df["severity"] == "medium").sum(),
            "low_risk_transactions": (scored_df["severity"] == "low").sum(),
            "rule_trigger_counts": {
                "velocity_count": int(scored_df["rule_velocity_count_flag"].sum()),
                "velocity_daily_amount": int(scored_df["rule_velocity_daily_amount_flag"].sum()),
                "velocity_7d_amount": int(scored_df["rule_velocity_7d_amount_flag"].sum()),
                "structuring": int(scored_df["rule_structuring_flag"].sum()),
                "round_number": int(scored_df["rule_round_number_flag"].sum()),
            },
        }

        LOGGER.info("AML detection complete: %d alerts from %d transactions",
                    metrics["alerts_generated"], metrics["total_transactions"])

        return {
            "status": "success",
            "scored_transactions_path": str(scored_path),
            "alerts_path": str(alerts_path),
            "alert_count": metrics["alerts_generated"],
            "high_risk_count": int(metrics["high_risk_transactions"]),
            "metrics": metrics,
        }

    except AmlSchemaError:
        raise
    except Exception as exc:
        LOGGER.exception("AML detection failed")
        raise AmlDetectionError(f"AML detection failed: {exc}") from exc


# Export public API
__all__ = [
    "run_aml_detection",
    "AmlRuleEngine",
    "AlertManager",
    "load_ledger_for_aml",
    "build_all_features",
    "AmlDetectionError",
    "AmlSchemaError",
]
