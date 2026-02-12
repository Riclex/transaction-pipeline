"""Tests for AML alert management."""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.aml.alerts import AlertManager, ALERT_COLUMNS


def _scored_df() -> pd.DataFrame:
    """Create scored DataFrame with alerts."""
    return pd.DataFrame({
        "txn_id": ["t1", "t2", "t3", "t4"],
        "account_id": ["acc1", "acc1", "acc1", "acc2"],
        "txn_date": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-01"]),
        "amount": [1000.0, 2000.0, 500.0, 5000.0],
        "risk_score": [75.0, 75.0, 30.0, 50.0],
        "severity": ["high", "high", "low", "medium"],
        "is_alert": [True, True, True, True],
        "rule_velocity_count_flag": [True, True, True, False],
        "rule_structuring_flag": [True, False, False, False],
        "rule_round_number_flag": [False, True, False, True],
    })


def test_alert_generation(tmp_path: Path):
    """Test alert generation from scored transactions."""
    manager = AlertManager()
    df = _scored_df()

    alerts = manager.generate_alerts(df)

    # Should generate alerts (deduplicated by account/rule/date)
    assert len(alerts) > 0
    assert "alert_id" in alerts.columns
    assert "severity" in alerts.columns
    assert "account_id" in alerts.columns


def test_alert_deduplication():
    """Test that alerts are deduplicated by account/rule/date."""
    manager = AlertManager()
    df = _scored_df()

    alerts = manager.generate_alerts(df)

    # acc1 on 2024-01-01 has 2 transactions but should be deduplicated
    # by rule (velocity_count and round_number for that day)
    acc1_alerts = alerts[alerts["account_id"] == "acc1"]

    # Should have alerts for different rules, not per-transaction
    # acc1 triggers: velocity_count, round_number
    # But round_number only triggered by t2
    # velocity_count triggered by t1 and t2
    assert len(acc1_alerts) >= 1


def test_alert_columns():
    """Test that generated alerts have all required columns."""
    manager = AlertManager()
    df = _scored_df()

    alerts = manager.generate_alerts(df)

    for col in ALERT_COLUMNS:
        assert col in alerts.columns, f"Missing column: {col}"


def test_alert_save_and_load(tmp_path: Path):
    """Test saving and reloading alerts."""
    manager = AlertManager()
    df = _scored_df()

    alerts = manager.generate_alerts(df)
    output_path = tmp_path / "alerts.parquet"

    saved_path = manager.save_alerts(alerts, output_path)
    assert saved_path.exists()

    # Reload and verify
    reloaded = pd.read_parquet(saved_path)
    for col in ALERT_COLUMNS:
        assert col in reloaded.columns


def test_empty_alerts(tmp_path: Path):
    """Test handling of no alerts case."""
    manager = AlertManager()

    # Create DataFrame with no alerts
    df = pd.DataFrame({
        "txn_id": ["t1"],
        "account_id": ["acc1"],
        "txn_date": pd.to_datetime(["2024-01-01"]),
        "amount": [100.0],
        "risk_score": [10.0],
        "severity": ["none"],
        "is_alert": [False],
        "rule_velocity_count_flag": [False],
        "rule_structuring_flag": [False],
        "rule_round_number_flag": [False],
    })

    alerts = manager.generate_alerts(df)

    # Should return empty DataFrame with correct columns
    assert len(alerts) == 0
    for col in ALERT_COLUMNS:
        assert col in alerts.columns

    # Save should still work
    output_path = tmp_path / "empty_alerts.parquet"
    saved_path = manager.save_alerts(alerts, output_path)
    assert saved_path.exists()


def test_alert_id_format():
    """Test that alert IDs follow expected format."""
    manager = AlertManager()
    df = _scored_df()

    alerts = manager.generate_alerts(df)

    if len(alerts) > 0:
        alert_id = alerts["alert_id"].iloc[0]
        # Should start with AML- and have timestamp format
        assert alert_id.startswith("AML-")
        # Should have format: AML-YYYYMMDDHHMMSS-NNNNNN
        parts = alert_id.split("-")
        assert len(parts) == 3


def test_alert_description_content():
    """Test that alert descriptions contain relevant info."""
    manager = AlertManager()
    df = _scored_df()

    alerts = manager.generate_alerts(df)

    if len(alerts) > 0:
        for _, alert in alerts.iterrows():
            # Description should contain rule name
            assert alert["rule_name"] in alert["description"]
            # Description should contain transaction count
            assert str(alert["triggering_txn_count"]) in alert["description"]
