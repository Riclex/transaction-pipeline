"""Feature engineering for AML detection.

Computes rolling window aggregations and velocity features per account.
All features are designed to be computed efficiently using vectorized
pandas operations.
"""

from __future__ import annotations

import logging
from typing import Dict, Any, Optional

import pandas as pd
import numpy as np

LOGGER = logging.getLogger(__name__)


def compute_velocity_features(
    df: pd.DataFrame,
    windows: Optional[list] = None,
) -> pd.DataFrame:
    """
    Compute transaction velocity features per account using rolling windows.

    Uses optimized groupby().rolling() approach which is 2-5x faster than
    transform(lambda) for large datasets. For production workloads with
    millions of rows, consider pre-aggregated tables or database window functions.

    Parameters
    ----------
    df : pd.DataFrame
        Ledger DataFrame with columns: account_id, txn_date, amount, txn_id
    windows : list, optional
        List of window strings for rolling calculations (e.g., ['1D', '7D'])
        Defaults to ['1D', '7D', '30D']

    Returns
    -------
    pd.DataFrame
        DataFrame with added velocity columns:
        - txn_count_1d, txn_count_7d, txn_count_30d
        - amount_sum_1d, amount_sum_7d, amount_sum_30d
        - amount_avg_1d, amount_avg_7d, amount_avg_30d
    """
    if windows is None:
        windows = ['1D', '7D', '30D']

    df = df.copy()

    # Ensure datetime
    df["txn_date"] = pd.to_datetime(df["txn_date"])

    # Pre-compute absolute amount to avoid repeated abs() calls
    df["_amount_abs"] = df["amount"].abs()

    # Sort once by account and date for efficient rolling
    df = df.sort_values(["account_id", "txn_date"]).reset_index(drop=True)

    for window in windows:
        window_label = window.lower().replace('d', 'd')
        days = int(window.replace('D', '').replace('d', ''))

        # Use groupby().rolling() which is significantly faster than
        # groupby().transform(lambda x: x.rolling()) because it avoids
        # Python-level iteration over groups
        # Result is a Series with MultiIndex (account_id, row_number)

        # Count: rolling count of transactions
        count_result = (
            df.groupby("account_id")["txn_id"]
            .rolling(window=days, min_periods=1)
            .count()
            .reset_index(level=0, drop=True)
        )
        df[f"txn_count_{window_label}"] = count_result.values

        # Sum: rolling sum of absolute amounts
        sum_result = (
            df.groupby("account_id")["_amount_abs"]
            .rolling(window=days, min_periods=1)
            .sum()
            .reset_index(level=0, drop=True)
        )
        df[f"amount_sum_{window_label}"] = sum_result.values

        # Average: rolling mean of absolute amounts
        avg_result = (
            df.groupby("account_id")["_amount_abs"]
            .rolling(window=days, min_periods=1)
            .mean()
            .reset_index(level=0, drop=True)
        )
        df[f"amount_avg_{window_label}"] = avg_result.values

    # Clean up temporary column
    df = df.drop(columns=["_amount_abs"])

    LOGGER.info("Computed velocity features for %d transactions", len(df))
    return df


def compute_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute time-based features for pattern detection.

    Parameters
    ----------
    df : pd.DataFrame
        Ledger DataFrame with txn_date column

    Returns
    -------
    pd.DataFrame
        DataFrame with added time feature columns:
        - hour_of_day: Hour component (0-23)
        - day_of_week: Day of week (0=Monday, 6=Sunday)
        - is_weekend: Boolean flag for Saturday/Sunday
        - is_late_night: Boolean for transactions between 00:00-05:00
    """
    df = df.copy()

    # Ensure datetime
    txn_datetime = pd.to_datetime(df["txn_date"])

    df["hour_of_day"] = txn_datetime.dt.hour
    df["day_of_week"] = txn_datetime.dt.dayofweek
    df["is_weekend"] = df["day_of_week"].isin([5, 6])  # Saturday, Sunday
    df["is_late_night"] = (txn_datetime.dt.hour >= 0) & (txn_datetime.dt.hour < 5)

    return df


def compute_structuring_features(
    df: pd.DataFrame,
    ctr_threshold: float = 10000.0,
    lower_factor: float = 0.90,
    upper_factor: float = 0.99,
) -> pd.DataFrame:
    """
    Compute structuring (threshold avoidance) detection features.

    Flags transactions that fall within a suspicious range just below
the Currency Transaction Report (CTR) threshold.

    Parameters
    ----------
    df : pd.DataFrame
        Ledger DataFrame with amount column
    ctr_threshold : float, default 10000.0
        The reporting threshold (e.g., $10,000)
    lower_factor : float, default 0.90
        Lower bound as fraction of threshold (e.g., 0.90 = $9,000)
    upper_factor : float, default 0.99
        Upper bound as fraction of threshold (e.g., 0.99 = $9,900)

    Returns
    -------
    pd.DataFrame
        DataFrame with added structuring feature columns:
        - is_near_threshold: Boolean flag for amounts in suspicious range
        - pct_of_threshold: Amount as percentage of CTR threshold
    """
    df = df.copy()

    lower_bound = ctr_threshold * lower_factor
    upper_bound = ctr_threshold * upper_factor

    amount_abs = df["amount"].abs()

    df["is_near_threshold"] = (amount_abs >= lower_bound) & (amount_abs <= upper_bound)
    df["pct_of_threshold"] = (amount_abs / ctr_threshold * 100).round(2)

    return df


def compute_round_number_features(
    df: pd.DataFrame,
    round_amounts: Optional[list] = None,
) -> pd.DataFrame:
    """
    Compute round number pattern detection features.

    Large round numbers can indicate structuring or suspicious activity.

    Parameters
    ----------
    df : pd.DataFrame
        Ledger DataFrame with amount column
    round_amounts : list, optional
        List of round amounts to flag (e.g., [10000, 5000, 1000])
        Defaults to [10000, 5000, 1000, 500]

    Returns
    -------
    pd.DataFrame
        DataFrame with added round number feature columns:
        - is_large_round: Boolean for amounts matching flagged round numbers
        - round_number_match: The matched round amount (or NaN)
    """
    df = df.copy()
    if round_amounts is None:
        round_amounts = [10000, 5000, 1000, 500]

    amount_abs = df["amount"].abs()

    # Check if amount matches any of the flagged round numbers
    df["is_large_round"] = amount_abs.isin(round_amounts)
    df["round_number_match"] = np.where(
        df["is_large_round"],
        amount_abs,
        np.nan
    )

    return df


def build_all_features(
    df: pd.DataFrame,
    config: Dict[str, Any],
) -> pd.DataFrame:
    """
    Build all AML features in sequence.

    Parameters
    ----------
    df : pd.DataFrame
        Input ledger DataFrame
    config : dict
        Configuration dictionary containing feature parameters

    Returns
    -------
    pd.DataFrame
        DataFrame with all AML features added
    """
    # Velocity features
    df = compute_velocity_features(df)

    # Time-based features
    df = compute_time_features(df)

    # Structuring features
    structuring_config = config.get("structuring_rules", {})
    df = compute_structuring_features(
        df,
        ctr_threshold=structuring_config.get("ctr_threshold", 10000.0),
        lower_factor=structuring_config.get("lower_bound_factor", 0.90),
        upper_factor=structuring_config.get("upper_bound_factor", 0.99),
    )

    # Round number features
    round_config = config.get("round_number_rules", {})
    if round_config.get("enabled", True):
        df = compute_round_number_features(
            df,
            round_amounts=round_config.get("large_round_amounts", [10000, 5000, 1000, 500]),
        )

    LOGGER.info("Feature engineering complete: %d features added",
                len([c for c in df.columns if c.startswith(("txn_count_", "amount_", "is_", "hour_", "day_"))]))

    return df
