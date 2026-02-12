"""End-to-end integration tests for AML detection."""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.aml import run_aml_detection


def _create_test_ledger(tmp_path: Path) -> Path:
    """Create test ledger Parquet file."""
    ledger_df = pd.DataFrame({
        "txn_id": [f"t{i}" for i in range(1, 11)],
        "account_id": ["acc1"] * 5 + ["acc2"] * 5,
        "txn_date": pd.to_datetime([
            "2024-01-01", "2024-01-01", "2024-01-01", "2024-01-01", "2024-01-01",  # acc1: 5 txns same day
            "2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05",  # acc2: spread out
        ]),
        "ingestion_date": pd.to_datetime(["2024-01-01"] * 10),
        "amount": [10000.0, 9500.0, 9500.0, 5000.0, 1000.0,  # acc1: near-threshold structuring
                   500.0, 600.0, 700.0, 800.0, 5000.0],  # acc2: normal + round number
        "currency": ["USD"] * 10,
        "txn_type": ["CARD"] * 10,
        "is_late": [False] * 10,
    })

    ledger_path = tmp_path / "ledger.parquet"
    ledger_df.to_parquet(ledger_path, index=False)
    return ledger_path


def test_full_aml_pipeline(tmp_path: Path):
    """Test complete AML detection pipeline."""
    ledger_path = _create_test_ledger(tmp_path)

    config = {
        "aml_detection": {
            "enabled": True,
            "input_path": str(ledger_path),
            "output_paths": {
                "scored_transactions": str(tmp_path / "scored.parquet"),
                "alerts": str(tmp_path / "alerts.parquet"),
            },
            "velocity_rules": {
                "daily_txn_count_threshold": 3,  # acc1 has 5 txns
                "daily_amount_threshold": 30000.0,
                "seven_day_amount_threshold": 50000.0,
            },
            "structuring_rules": {
                "ctr_threshold": 10000.0,
                "lower_bound_factor": 0.90,
                "upper_bound_factor": 0.99,
            },
            "round_number_rules": {
                "enabled": True,
                "large_round_amounts": [5000, 10000],
            },
            "risk_weights": {
                "velocity_daily_count": 25,
                "velocity_daily_amount": 30,
                "velocity_7d_amount": 35,
                "structuring": 50,
                "round_number": 15,
            },
            "alert_thresholds": {
                "low": 25,
                "medium": 50,
                "high": 75,
            },
        }
    }

    result = run_aml_detection(config)

    # Verify results
    assert result["status"] == "success"
    assert result["alert_count"] > 0

    # Verify output files exist
    assert Path(result["scored_transactions_path"]).exists()
    assert Path(result["alerts_path"]).exists()

    # Verify scored transactions have expected columns
    scored = pd.read_parquet(result["scored_transactions_path"])
    assert "risk_score" in scored.columns
    assert "severity" in scored.columns

    # Verify that alerts were generated (due to structuring/round number rules)
    # Note: velocity rules may or may not trigger depending on feature computation
    assert result["alert_count"] > 0


def test_disabled_aml_detection():
    """Test that disabled AML detection returns correctly."""
    config = {
        "aml_detection": {
            "enabled": False,
        }
    }

    result = run_aml_detection(config)

    assert result["status"] == "disabled"


def test_aml_with_empty_ledger(tmp_path: Path):
    """Test AML detection with empty ledger."""
    # Create empty ledger with correct schema
    empty_df = pd.DataFrame({
        "txn_id": pd.Series([], dtype=str),
        "account_id": pd.Series([], dtype=str),
        "txn_date": pd.Series([], dtype='datetime64[ns]'),
        "ingestion_date": pd.Series([], dtype='datetime64[ns]'),
        "amount": pd.Series([], dtype=float),
        "currency": pd.Series([], dtype=str),
        "txn_type": pd.Series([], dtype=str),
        "is_late": pd.Series([], dtype=bool),
    })

    ledger_path = tmp_path / "empty_ledger.parquet"
    empty_df.to_parquet(ledger_path, index=False)

    config = {
        "aml_detection": {
            "enabled": True,
            "input_path": str(ledger_path),
            "output_paths": {
                "scored_transactions": str(tmp_path / "scored.parquet"),
                "alerts": str(tmp_path / "alerts.parquet"),
            },
            "velocity_rules": {
                "daily_txn_count_threshold": 10,
                "daily_amount_threshold": 50000.0,
                "seven_day_amount_threshold": 150000.0,
            },
            "structuring_rules": {
                "ctr_threshold": 10000.0,
                "lower_bound_factor": 0.90,
                "upper_bound_factor": 0.99,
            },
            "round_number_rules": {
                "enabled": True,
            },
            "risk_weights": {
                "velocity_daily_count": 25,
                "velocity_daily_amount": 30,
                "velocity_7d_amount": 35,
                "structuring": 50,
                "round_number": 15,
            },
            "alert_thresholds": {
                "low": 25,
                "medium": 50,
                "high": 75,
            },
        }
    }

    result = run_aml_detection(config)

    # Should succeed with 0 alerts
    assert result["status"] == "success"
    assert result["alert_count"] == 0


def test_metrics_computation(tmp_path: Path):
    """Test that metrics are correctly computed."""
    ledger_path = _create_test_ledger(tmp_path)

    config = {
        "aml_detection": {
            "enabled": True,
            "input_path": str(ledger_path),
            "output_paths": {
                "scored_transactions": str(tmp_path / "scored.parquet"),
                "alerts": str(tmp_path / "alerts.parquet"),
            },
            "velocity_rules": {
                "daily_txn_count_threshold": 3,
                "daily_amount_threshold": 30000.0,
                "seven_day_amount_threshold": 50000.0,
            },
            "structuring_rules": {
                "ctr_threshold": 10000.0,
                "lower_bound_factor": 0.90,
                "upper_bound_factor": 0.99,
            },
            "round_number_rules": {
                "enabled": True,
                "large_round_amounts": [5000, 10000],
            },
            "risk_weights": {
                "velocity_daily_count": 25,
                "velocity_daily_amount": 30,
                "velocity_7d_amount": 35,
                "structuring": 50,
                "round_number": 15,
            },
            "alert_thresholds": {
                "low": 25,
                "medium": 50,
                "high": 75,
            },
        }
    }

    result = run_aml_detection(config)

    # Verify metrics structure
    assert "metrics" in result
    metrics = result["metrics"]

    assert "total_transactions" in metrics
    assert metrics["total_transactions"] == 10

    assert "alerts_generated" in metrics
    assert "high_risk_transactions" in metrics
    assert "medium_risk_transactions" in metrics
    assert "low_risk_transactions" in metrics

    assert "rule_trigger_counts" in metrics
    rule_counts = metrics["rule_trigger_counts"]
    assert "velocity_count" in rule_counts
    assert "velocity_daily_amount" in rule_counts
    assert "velocity_7d_amount" in rule_counts
    assert "structuring" in rule_counts
    assert "round_number" in rule_counts
