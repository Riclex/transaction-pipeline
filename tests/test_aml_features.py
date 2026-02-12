"""Tests for AML feature engineering."""

import sys
from pathlib import Path

import pandas as pd
import pytest
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.aml.features import (
    compute_velocity_features,
    compute_time_features,
    compute_structuring_features,
    compute_round_number_features,
    build_all_features,
)


def _base_ledger_df() -> pd.DataFrame:
    """Create base ledger DataFrame for testing."""
    return pd.DataFrame({
        "txn_id": ["t1", "t2", "t3", "t4", "t5"],
        "account_id": ["acc1", "acc1", "acc1", "acc2", "acc2"],
        "txn_date": pd.to_datetime([
            "2024-01-01", "2024-01-01", "2024-01-02",
            "2024-01-01", "2024-01-15"
        ]),
        "ingestion_date": pd.to_datetime(["2024-01-01"] * 5),
        "amount": [1000.0, 2000.0, -500.0, 9500.0, 5000.0],
        "currency": ["USD"] * 5,
        "txn_type": ["CARD", "CARD", "REFUND", "CASH", "CARD"],
        "is_late": [False] * 5,
    })


def test_velocity_features_computed():
    """Test that velocity features are computed correctly."""
    df = _base_ledger_df()
    result = compute_velocity_features(df)

    # Should have velocity columns
    assert "txn_count_1d" in result.columns
    assert "amount_sum_7d" in result.columns

    # Verify columns exist
    assert "txn_count_1d" in result.columns
    assert "txn_count_7d" in result.columns
    assert "txn_count_30d" in result.columns
    assert "amount_sum_1d" in result.columns
    assert "amount_sum_7d" in result.columns
    assert "amount_sum_30d" in result.columns


def test_time_features_computed():
    """Test time-based feature extraction."""
    df = _base_ledger_df()
    result = compute_time_features(df)

    assert "hour_of_day" in result.columns
    assert "day_of_week" in result.columns
    assert "is_weekend" in result.columns
    assert "is_late_night" in result.columns

    # Check values
    assert result["day_of_week"].iloc[0] == 0  # Monday = 0
    assert result["is_weekend"].iloc[0] == False


def test_structuring_detection():
    """Test structuring (near-threshold) detection."""
    df = _base_ledger_df()
    result = compute_structuring_features(df, ctr_threshold=10000.0)

    # t4 has amount 9500 which is 95% of 10000 threshold
    # With default 0.90-0.99 range, this should flag
    t4_row = result[result["txn_id"] == "t4"].iloc[0]
    assert t4_row["is_near_threshold"] == True
    assert t4_row["pct_of_threshold"] == 95.0

    # t1 has amount 1000, should not flag
    t1_row = result[result["txn_id"] == "t1"].iloc[0]
    assert t1_row["is_near_threshold"] == False


def test_round_number_detection():
    """Test round number pattern detection."""
    df = _base_ledger_df()
    result = compute_round_number_features(df, round_amounts=[5000, 10000])

    # t5 has amount 5000 which matches
    t5_row = result[result["txn_id"] == "t5"].iloc[0]
    assert t5_row["is_large_round"] == True
    assert t5_row["round_number_match"] == 5000.0

    # t1 has amount 1000, should not match
    t1_row = result[result["txn_id"] == "t1"].iloc[0]
    assert t1_row["is_large_round"] == False
    assert np.isnan(t1_row["round_number_match"])


def test_build_all_features():
    """Test building all features together."""
    df = _base_ledger_df()
    config = {
        "structuring_rules": {
            "ctr_threshold": 10000.0,
            "lower_bound_factor": 0.90,
            "upper_bound_factor": 0.99,
        },
        "round_number_rules": {
            "enabled": True,
            "large_round_amounts": [5000, 10000],
        },
    }

    result = build_all_features(df, config)

    # Should have all feature columns
    assert "txn_count_1d" in result.columns
    assert "hour_of_day" in result.columns
    assert "is_near_threshold" in result.columns
    assert "is_large_round" in result.columns


def test_empty_dataframe():
    """Test handling of empty DataFrame."""
    empty_df = _base_ledger_df().iloc[0:0]
    result = compute_velocity_features(empty_df)
    assert len(result) == 0
    assert "txn_count_1d" in result.columns
