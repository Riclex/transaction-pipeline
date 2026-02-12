"""Alert management for AML detection.

Handles alert generation, deduplication, severity assignment, and
persistence to Parquet format.
"""

from __future__ import annotations

import logging
from typing import Dict, Any, Union
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np

LOGGER = logging.getLogger(__name__)

# Alert record schema
ALERT_COLUMNS = [
    "alert_id",
    "alert_timestamp",
    "account_id",
    "rule_id",
    "rule_name",
    "severity",
    "risk_score",
    "triggering_txn_ids",
    "triggering_txn_count",
    "triggering_amount_sum",
    "description",
    "status",  # "open", "investigating", "closed_false_positive", "confirmed_suspicious"
]


@dataclass
class AmlAlert:
    """Single AML alert record."""
    alert_id: str
    alert_timestamp: str
    account_id: str
    rule_id: str
    rule_name: str
    severity: str
    risk_score: float
    triggering_txn_ids: list
    triggering_txn_count: int
    triggering_amount_sum: float
    description: str
    status: str = "open"

    def to_dict(self) -> Dict[str, Any]:
        """Convert alert to dictionary."""
        return asdict(self)


class AlertManager:
    """
    Manages AML alert lifecycle: generation, deduplication, storage.
    """

    RULE_NAMES = {
        "velocity_count": "Daily Transaction Count Exceeded",
        "velocity_daily_amount": "Daily Amount Threshold Exceeded",
        "velocity_7d_amount": "7-Day Amount Threshold Exceeded",
        "structuring": "Potential Structuring Detected",
        "round_number": "Large Round Number Transaction",
    }

    def __init__(self):
        """Initialize alert manager."""
        self.alerts = []
        self._alert_counter = 0

    def _generate_alert_id(self) -> str:
        """Generate unique alert ID."""
        self._alert_counter += 1
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"AML-{timestamp}-{self._alert_counter:06d}"

    def _get_triggered_rules(self, row: pd.Series):
        """
        Extract triggered rules from a transaction row.

        Returns list of (rule_id, rule_name, contribution_weight) tuples.
        """
        triggered = []

        if row.get("rule_velocity_count_flag", False):
            triggered.append(("velocity_count", self.RULE_NAMES["velocity_count"], 25))

        if row.get("rule_velocity_daily_amount_flag", False):
            triggered.append(("velocity_daily_amount", self.RULE_NAMES["velocity_daily_amount"], 30))

        if row.get("rule_velocity_7d_amount_flag", False):
            triggered.append(("velocity_7d_amount", self.RULE_NAMES["velocity_7d_amount"], 35))

        if row.get("rule_structuring_flag", False):
            triggered.append(("structuring", self.RULE_NAMES["structuring"], 50))

        if row.get("rule_round_number_flag", False):
            triggered.append(("round_number", self.RULE_NAMES["round_number"], 15))

        return triggered

    def generate_alerts(self, scored_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate deduplicated alerts from scored transactions.

        Deduplication strategy: One alert per (account_id, rule_id) combination
        per day, aggregating all triggering transactions.

        Parameters
        scored_df : pd.DataFrame
            DataFrame with rule flags and risk scores

        Returns
        pd.DataFrame
            Alert records DataFrame
        """
        if scored_df.empty:
            return pd.DataFrame(columns=ALERT_COLUMNS)

        # Filter to alerted transactions only
        alerts_df = scored_df[scored_df["is_alert"] == True].copy()

        if alerts_df.empty:
            LOGGER.info("No alerts to generate")
            return pd.DataFrame(columns=ALERT_COLUMNS)

        # Extract date component for grouping
        alerts_df["alert_date"] = pd.to_datetime(alerts_df["txn_date"]).dt.date

        alert_records = []

        # Group by account, date, and determine which rules triggered
        for (account_id, alert_date), group in alerts_df.groupby(["account_id", "alert_date"]):
            # Get all triggered rules for this group
            all_rules = set()
            for _, row in group.iterrows():
                rules = self._get_triggered_rules(row)
                all_rules.update([r[0] for r in rules])

            # Create one alert per triggered rule (deduplication)
            for rule_id in all_rules:
                rule_name = self.RULE_NAMES.get(rule_id, rule_id)

                # Filter transactions that triggered this specific rule
                rule_col = f"rule_{rule_id}_flag"
                if rule_col in group.columns:
                    rule_txns = group[group[rule_col] == True]
                else:
                    # For rules without specific flag column, include all
                    rule_txns = group

                if rule_txns.empty:
                    continue

                # Aggregate transaction info
                txn_ids = rule_txns["txn_id"].tolist()
                txn_count = len(txn_ids)
                amount_sum = rule_txns["amount"].abs().sum()
                max_risk = rule_txns["risk_score"].max()

                # Determine severity based on max risk in group
                severity = rule_txns.loc[rule_txns["risk_score"].idxmax(), "severity"]

                # Create alert
                alert = AmlAlert(
                    alert_id=self._generate_alert_id(),
                    alert_timestamp=datetime.now().isoformat(),
                    account_id=account_id,
                    rule_id=rule_id,
                    rule_name=rule_name,
                    severity=severity,
                    risk_score=round(max_risk, 2),
                    triggering_txn_ids=txn_ids,
                    triggering_txn_count=txn_count,
                    triggering_amount_sum=round(amount_sum, 2),
                    description=f"{rule_name}: {txn_count} transactions totaling {amount_sum:,.2f}",
                    status="open",
                )

                alert_records.append(alert.to_dict())

        alerts_output_df = pd.DataFrame(alert_records, columns=ALERT_COLUMNS)

        LOGGER.info(
            "Generated %d deduplicated alerts for %d accounts",
            len(alerts_output_df),
            alerts_output_df["account_id"].nunique() if not alerts_output_df.empty else 0
        )

        return alerts_output_df

    def save_alerts(
        self,
        alerts_df: pd.DataFrame,
        output_path: Union[str, Path],
    ) -> Path:
        """
        Save alerts to Parquet file.

        Parameters
        alerts_df : pd.DataFrame
            Alerts DataFrame to save
        output_path : str | Path
            Output file path

        Returns
        Path
            Path to saved file
        """
        path = Path(output_path).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)

        # Handle empty alerts
        if alerts_df.empty:
            # Write empty DataFrame with correct schema
            empty_df = pd.DataFrame(columns=ALERT_COLUMNS)
            empty_df.to_parquet(path, index=False)
            LOGGER.info("No alerts generated - empty alert file created at %s", path)
            return path

        # Convert list columns to string for Parquet compatibility
        alerts_df = alerts_df.copy()
        if "triggering_txn_ids" in alerts_df.columns:
            alerts_df["triggering_txn_ids"] = alerts_df["triggering_txn_ids"].apply(
                lambda x: ",".join(x) if isinstance(x, list) else x
            )

        alerts_df.to_parquet(path, index=False)
        LOGGER.info("Saved %d alerts to %s", len(alerts_df), path)
        return path
