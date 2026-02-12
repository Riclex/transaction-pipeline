"""Tests for AML rule engine."""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.aml.rules import AmlRuleEngine


def _base_config() -> dict:
    """Create base AML configuration."""
    return {
        "velocity_rules": {
            "daily_txn_count_threshold": 2,
            "daily_amount_threshold": 5000.0,
            "seven_day_amount_threshold": 10000.0,
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


def _featured_df() -> pd.DataFrame:
    """Create DataFrame with pre-computed features."""
    return pd.DataFrame({
        "txn_id": ["t1", "t2", "t3"],
        "account_id": ["acc1", "acc1", "acc2"],
        "txn_count_1d": [3, 3, 1],  # acc1 exceeds threshold of 2
        "amount_sum_1d": [6000.0, 6000.0, 1000.0],  # acc1 exceeds 5000
        "amount_sum_7d": [15000.0, 15000.0, 1000.0],  # acc1 exceeds 10000
        "is_near_threshold": [True, False, False],
        "is_large_round": [False, True, False],
    })


def test_velocity_count_rule():
    """Test daily transaction count threshold rule."""
    config = _base_config()
    engine = AmlRuleEngine(config)
    df = _featured_df()

    result = engine.evaluate_velocity_daily_count(df)

    # acc1 has txn_count_1d=3 which exceeds threshold of 2
    assert result.iloc[0] == True
    assert result.iloc[2] == False  # acc2 has count 1


def test_velocity_daily_amount_rule():
    """Test daily amount threshold rule."""
    config = _base_config()
    engine = AmlRuleEngine(config)
    df = _featured_df()

    result = engine.evaluate_velocity_daily_amount(df)

    # acc1 has amount_sum_1d=6000 which exceeds threshold of 5000
    assert result.iloc[0] == True
    assert result.iloc[2] == False  # acc2 has amount 1000


def test_velocity_7d_amount_rule():
    """Test 7-day amount threshold rule."""
    config = _base_config()
    engine = AmlRuleEngine(config)
    df = _featured_df()

    result = engine.evaluate_velocity_7d_amount(df)

    # acc1 has amount_sum_7d=15000 which exceeds threshold of 10000
    assert result.iloc[0] == True
    assert result.iloc[2] == False  # acc2 has amount 1000


def test_structuring_rule():
    """Test structuring detection rule."""
    config = _base_config()
    engine = AmlRuleEngine(config)
    df = _featured_df()

    result = engine.evaluate_structuring(df)

    # t1 has is_near_threshold=True
    assert result.iloc[0] == True
    assert result.iloc[1] == False


def test_round_number_rule():
    """Test round number detection rule."""
    config = _base_config()
    engine = AmlRuleEngine(config)
    df = _featured_df()

    result = engine.evaluate_round_numbers(df)

    # t2 has is_large_round=True
    assert result.iloc[0] == False
    assert result.iloc[1] == True


def test_risk_score_calculation():
    """Test composite risk score calculation."""
    config = _base_config()
    engine = AmlRuleEngine(config)
    df = _featured_df()

    scores = engine.calculate_risk_score(df)

    # Row 0: velocity_count (25) + velocity_daily_amount (30) + velocity_7d_amount (35) + structuring (50) = 140, capped at 100
    assert scores.iloc[0] == 100.0

    # Row 1: velocity_count (25) + velocity_daily_amount (30) + velocity_7d_amount (35) + round_number (15) = 105, capped at 100
    assert scores.iloc[1] == 100.0

    # Row 2: no triggers = 0
    assert scores.iloc[2] == 0.0


def test_severity_mapping():
    """Test severity level assignment."""
    config = _base_config()
    engine = AmlRuleEngine(config)

    scores = pd.Series([10, 30, 55, 80])
    severity = engine.determine_severity(scores)

    assert severity.iloc[0] == "none"   # 10 < 25 (low threshold)
    assert severity.iloc[1] == "low"    # 30 >= 25
    assert severity.iloc[2] == "medium" # 55 >= 50
    assert severity.iloc[3] == "high"   # 80 >= 75


def test_apply_all_rules():
    """Test applying all rules together."""
    config = _base_config()
    engine = AmlRuleEngine(config)
    df = _featured_df()

    result = engine.apply_all_rules(df)

    # Should have rule flag columns
    assert "rule_velocity_count_flag" in result.columns
    assert "rule_velocity_daily_amount_flag" in result.columns
    assert "rule_velocity_7d_amount_flag" in result.columns
    assert "rule_structuring_flag" in result.columns
    assert "rule_round_number_flag" in result.columns

    # Should have score and severity
    assert "risk_score" in result.columns
    assert "severity" in result.columns
    assert "is_alert" in result.columns


def test_disabled_round_number_rules():
    """Test that disabled round number rules don't trigger."""
    config = _base_config()
    config["round_number_rules"]["enabled"] = False
    engine = AmlRuleEngine(config)
    df = _featured_df()

    result = engine.evaluate_round_numbers(df)

    # All should be False when disabled
    assert result.iloc[0] == False
    assert result.iloc[1] == False
    assert result.iloc[2] == False
