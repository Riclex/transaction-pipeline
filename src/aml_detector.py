#!/usr/bin/env python3
"""Standalone AML detection script.

Usage:
    python src/aml_detector.py
    python src/aml_detector.py --config config/pipeline_config.yaml
    python src/aml_detector.py --ledger data/processed/ledger_transactions.parquet
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Path setup
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.config import load_config_validated
from src.aml import run_aml_detection, AmlDetectionError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
LOGGER = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="AML Detection on Ledger Output")
    parser.add_argument(
        "--config",
        default="config/pipeline_config.yaml",
        help="Config file path (default: config/pipeline_config.yaml)",
    )
    parser.add_argument(
        "--ledger",
        help="Override ledger input path",
    )
    parser.add_argument(
        "--output-dir",
        help="Override output directory",
    )
    args = parser.parse_args()

    try:
        config = load_config_validated(args.config)
        result = run_aml_detection(
            config,
            input_path=args.ledger,
            output_dir=args.output_dir,
        )

        if result.get("status") == "disabled":
            LOGGER.info("AML detection is disabled in configuration")
            return 0

        LOGGER.info("=" * 60)
        LOGGER.info("AML Detection Complete")
        LOGGER.info("=" * 60)
        LOGGER.info("Alerts generated: %d", result["alert_count"])
        LOGGER.info("High-risk transactions: %d", result["high_risk_count"])
        LOGGER.info("Scored transactions: %s", result["scored_transactions_path"])
        LOGGER.info("Alerts file: %s", result["alerts_path"])

        if result["metrics"]:
            LOGGER.info("-" * 60)
            LOGGER.info("Rule Trigger Counts:")
            for rule, count in result["metrics"]["rule_trigger_counts"].items():
                LOGGER.info("  %s: %d", rule, count)

        return 0

    except AmlDetectionError as exc:
        LOGGER.error("AML detection failed: %s", exc)
        return 1
    except Exception as exc:
        LOGGER.exception("Unexpected error")
        return 1


if __name__ == "__main__":
    sys.exit(main())
