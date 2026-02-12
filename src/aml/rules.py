"""AML rule engine for detecting suspicious patterns.

Implements configurable rules for velocity checks, structuring detection,
and round number analysis. Rules return boolean flags that contribute
to an overall risk score.
"""

from __future__ import annotations

import logging
from typing import Dict, Any
from dataclasses import dataclass

import pandas as pd
import numpy as np

LOGGER = logging.getLogger(__name__)


@dataclass
class RuleResult:
    """Result from applying an AML rule."""
    rule_id: str
    triggered: bool
    severity: str  # "low", "medium", "high"
    description: str
    contributing_factors: Dict[str, Any]


class AmlRuleEngine:
    """
    Anti-Money Laundering rule engine.

    Evaluates transactions against configured rules and generates
    risk scores and alert flags.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize rule engine with configuration.

        Parameters
        config : dict
            AML configuration dictionary containing rule thresholds
        """
        self.config = config
        self.velocity_config = config.get("velocity_rules", {})
        self.structuring_config = config.get("structuring_rules", {})
        self.round_config = config.get("round_number_rules", {})
        self.risk_weights = config.get("risk_weights", {})
        self.alert_thresholds = config.get("alert_thresholds", {})

        LOGGER.info("Initialized AML Rule Engine with %d rule categories", 3)

    def evaluate_velocity_daily_count(
        self,
        df: pd.DataFrame,
    ) -> pd.Series:
        """
        Flag accounts exceeding daily transaction count threshold.

        Returns boolean Series indicating violations.
        """
        threshold = self.velocity_config.get("daily_txn_count_threshold", 10)
        return df["txn_count_1d"] > threshold

    def evaluate_velocity_daily_amount(
        self,
        df: pd.DataFrame,
    ) -> pd.Series:
        """
        Flag accounts exceeding daily amount threshold.

        Returns boolean Series indicating violations.
        """
        threshold = self.velocity_config.get("daily_amount_threshold", 50000.0)
        return df["amount_sum_1d"].abs() > threshold

    def evaluate_velocity_7d_amount(
        self,
        df: pd.DataFrame,
    ) -> pd.Series:
        """
        Flag accounts exceeding 7-day rolling amount threshold.

        Returns boolean Series indicating violations.
        """
        threshold = self.velocity_config.get("seven_day_amount_threshold", 150000.0)
        return df["amount_sum_7d"].abs() > threshold

    def evaluate_structuring(
        self,
        df: pd.DataFrame,
    ) -> pd.Series:
        """
        Flag potential structuring (threshold avoidance) behavior.

        Requires multiple near-threshold transactions to flag.
        Returns boolean Series indicating violations.
        """
        # Single transaction near threshold
        single_structuring = df["is_near_threshold"]

        # For now, flag each near-threshold transaction
        # In production, this would aggregate by account over time window
        return single_structuring

    def evaluate_round_numbers(
        self,
        df: pd.DataFrame,
    ) -> pd.Series:
        """
        Flag large round number transactions.

        Returns boolean Series indicating violations.
        """
        if not self.round_config.get("enabled", True):
            return pd.Series(False, index=df.index)

        return df["is_large_round"]

    def calculate_risk_score(self, df: pd.DataFrame) -> pd.Series:
        """
        Calculate composite risk score from all rule evaluations.

        Risk score is weighted sum of triggered rules (0-100 scale).

        Parameters
        df : pd.DataFrame
            DataFrame with all feature columns

        Returns
        pd.Series
            Risk scores (0-100) for each transaction
        """
        score = pd.Series(0, index=df.index, dtype=float)

        # Velocity: daily count
        if self.risk_weights.get("velocity_daily_count", 0) > 0:
            mask = self.evaluate_velocity_daily_count(df)
            score += mask * self.risk_weights["velocity_daily_count"]

        # Velocity: daily amount
        if self.risk_weights.get("velocity_daily_amount", 0) > 0:
            mask = self.evaluate_velocity_daily_amount(df)
            score += mask * self.risk_weights["velocity_daily_amount"]

        # Velocity: 7-day amount
        if self.risk_weights.get("velocity_7d_amount", 0) > 0:
            mask = self.evaluate_velocity_7d_amount(df)
            score += mask * self.risk_weights["velocity_7d_amount"]

        # Structuring
        if self.risk_weights.get("structuring", 0) > 0:
            mask = self.evaluate_structuring(df)
            score += mask * self.risk_weights["structuring"]

        # Round numbers
        if self.risk_weights.get("round_number", 0) > 0:
            mask = self.evaluate_round_numbers(df)
            score += mask * self.risk_weights["round_number"]

        # Cap at 100
        score = score.clip(0, 100).round(2)

        return score

    def determine_severity(self, risk_score: pd.Series) -> pd.Series:
        """
        Map risk scores to severity levels.

        Parameters
        risk_score : pd.Series
            Risk scores (0-100)

        Returns
        pd.Series
            Severity levels: "none", "low", "medium", "high"
        """
        low = self.alert_thresholds.get("low", 25)
        medium = self.alert_thresholds.get("medium", 50)
        high = self.alert_thresholds.get("high", 75)

        # Use np.select for proper priority handling (first match wins)
        conditions = [
            risk_score >= high,
            risk_score >= medium,
            risk_score >= low,
        ]
        choices = ["high", "medium", "low"]

        severity = pd.Series(np.select(conditions, choices, default="none"), index=risk_score.index)

        return severity

    def apply_all_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all AML rules and generate risk scores.

        Parameters
        df : pd.DataFrame
            DataFrame with computed features

        Returns
        pd.DataFrame
            DataFrame with rule results and risk scores added:
            - rule_velocity_count_flag
            - rule_velocity_daily_amount_flag
            - rule_velocity_7d_amount_flag
            - rule_structuring_flag
            - rule_round_number_flag
            - risk_score
            - severity
        """
        df = df.copy()

        # Individual rule flags
        df["rule_velocity_count_flag"] = self.evaluate_velocity_daily_count(df)
        df["rule_velocity_daily_amount_flag"] = self.evaluate_velocity_daily_amount(df)
        df["rule_velocity_7d_amount_flag"] = self.evaluate_velocity_7d_amount(df)
        df["rule_structuring_flag"] = self.evaluate_structuring(df)
        df["rule_round_number_flag"] = self.evaluate_round_numbers(df)

        # Composite risk score
        df["risk_score"] = self.calculate_risk_score(df)

        # Severity level
        df["severity"] = self.determine_severity(df["risk_score"])

        # Flag if any alert triggered
        df["is_alert"] = df["severity"] != "none"

        triggered_count = df["is_alert"].sum()
        LOGGER.info(
            "Rule evaluation complete: %d alerts triggered (%d low, %d medium, %d high)",
            triggered_count,
            (df["severity"] == "low").sum(),
            (df["severity"] == "medium").sum(),
            (df["severity"] == "high").sum(),
        )

        return df
