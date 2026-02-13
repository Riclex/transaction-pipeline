"""Configuration validation schema using Pydantic

This module provides validation for the pipeline configuration YAML file
to ensure all required fields are present and have valid values.

Supports environment variable substitution for sensitive values using ${VAR_NAME} syntax.
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import List, Optional, Union, Any
from pydantic import BaseModel, Field, ValidationError, validator

# Logging
LOGGER = logging.getLogger(__name__)


class PathsConfig(BaseModel):
    """Configuration for file paths."""
    raw_transactions: str = Field(..., description="Path to raw transaction CSV file")
    ledger_output: str = Field(..., description="Path for ledger output parquet file")
    daily_balance_output: str = Field(..., description="Path for daily balance output parquet file")


class SchemaConfig(BaseModel):
    """Configuration for data schema requirements."""
    required_columns: List[str] = Field(
        default=[
            "txn_id", "account_id", "txn_date", "ingestion_date",
            "amount", "currency", "txn_type", "status"
        ],
        description="List of required columns in raw data"
    )


class BusinessRulesConfig(BaseModel):
    """Configuration for business rules."""
    success_statuses: List[str] = Field(
        default=["COMPLETED", "SETTLED", "OK"],
        description="Transaction statuses that indicate success"
    )
    refund_types: List[str] = Field(
        default=["REFUND"],
        description="Transaction types that indicate refunds"
    )
    valid_txn_types: List[str] = Field(
        default=["CARD", "CASH", "REFUND", "DEBIT", "CREDIT"],
        description="Valid transaction types"
    )
    late_arrival_days_threshold: int = Field(
        default=0,
        ge=0,
        description="Number of days threshold for late arrival detection"
    )


class ReconciliationConfig(BaseModel):
    """Configuration for reconciliation settings."""
    tolerance_amount: float = Field(
        default=0.01,
        ge=0,
        description="Tolerance for amount differences in reconciliation"
    )
    fail_on_mismatch: bool = Field(
        default=True,
        description="Whether to fail pipeline on reconciliation mismatch"
    )


class ProcessingConfig(BaseModel):
    """Configuration for data processing options."""
    chunksize: Optional[int] = Field(
        default=None,
        gt=0,
        description="Number of rows to read at a time for large files"
    )


class AggregationConfig(BaseModel):
    """Configuration for aggregation settings."""
    daily_balance: dict = Field(
        default={
            "group_by": ["account_id", "txn_date"],
            "metrics": {"total_amount": "sum", "txn_count": "count"}
        },
        description="Configuration for daily balance aggregation"
    )


class AmlOutputPathsConfig(BaseModel):
    """Output file paths for AML detection."""
    scored_transactions: str = Field(
        default="data/processed/aml_scored_transactions.parquet",
        description="Path for scored transactions output"
    )
    alerts: str = Field(
        default="data/processed/aml_alerts.parquet",
        description="Path for alerts output"
    )


class VelocityRulesConfig(BaseModel):
    """Configuration for velocity-based AML rules."""
    daily_txn_count_threshold: int = Field(
        default=10,
        ge=1,
        description="Maximum allowed daily transaction count per account"
    )
    daily_amount_threshold: float = Field(
        default=50000.0,
        ge=0,
        description="Daily amount threshold for flagging"
    )
    seven_day_amount_threshold: float = Field(
        default=150000.0,
        ge=0,
        description="7-day rolling amount threshold"
    )


class StructuringRulesConfig(BaseModel):
    """Configuration for structuring (avoidance) detection."""
    ctr_threshold: float = Field(
        default=10000.0,
        ge=0,
        description="Currency Transaction Report threshold"
    )
    lower_bound_factor: float = Field(
        default=0.90,
        ge=0,
        le=1,
        description="Lower bound as fraction of CTR threshold"
    )
    upper_bound_factor: float = Field(
        default=0.99,
        ge=0,
        le=1,
        description="Upper bound as fraction of CTR threshold"
    )
    min_structuring_txns: int = Field(
        default=2,
        ge=1,
        description="Minimum structuring transactions to flag"
    )


class RoundNumberRulesConfig(BaseModel):
    """Configuration for round number pattern detection."""
    enabled: bool = Field(default=True)
    large_round_amounts: List[float] = Field(
        default=[10000, 5000, 1000, 500],
        description="Round amounts that trigger flags when matched"
    )


class RiskWeightsConfig(BaseModel):
    """Risk score weights for each rule type."""
    velocity_daily_count: int = Field(default=25, ge=0, le=100)
    velocity_daily_amount: int = Field(default=30, ge=0, le=100)
    velocity_7d_amount: int = Field(default=35, ge=0, le=100)
    structuring: int = Field(default=50, ge=0, le=100)
    round_number: int = Field(default=15, ge=0, le=100)


class AlertThresholdsConfig(BaseModel):
    """Severity thresholds for alert generation."""
    low: int = Field(default=25, ge=0, le=100)
    medium: int = Field(default=50, ge=0, le=100)
    high: int = Field(default=75, ge=0, le=100)

    @validator('medium')
    def medium_above_low(cls, v, values):
        if 'low' in values and v < values['low']:
            raise ValueError('medium threshold must be >= low threshold')
        return v

    @validator('high')
    def high_above_medium(cls, v, values):
        if 'medium' in values and v < values['medium']:
            raise ValueError('high threshold must be >= medium threshold')
        return v


class AmlDetectionConfig(BaseModel):
    """Main AML detection configuration."""
    enabled: bool = Field(default=True)
    input_path: str = Field(
        default="data/processed/ledger_transactions.parquet",
        description="Path to ledger input file"
    )
    output_paths: AmlOutputPathsConfig = Field(
        default_factory=AmlOutputPathsConfig
    )
    velocity_rules: VelocityRulesConfig = Field(
        default_factory=VelocityRulesConfig
    )
    structuring_rules: StructuringRulesConfig = Field(
        default_factory=StructuringRulesConfig
    )
    round_number_rules: RoundNumberRulesConfig = Field(
        default_factory=RoundNumberRulesConfig
    )
    risk_weights: RiskWeightsConfig = Field(
        default_factory=RiskWeightsConfig
    )
    alert_thresholds: AlertThresholdsConfig = Field(
        default_factory=AlertThresholdsConfig
    )


# -----------------------------------------------------------------------------
# Data Trust Layer Configuration
# -----------------------------------------------------------------------------

class AlertChannelConfig(BaseModel):
    """Configuration for alert channels."""
    console: bool = Field(default=True)
    webhook: bool = Field(default=False)
    webhook_url: Optional[str] = Field(default=None)
    email: bool = Field(default=False)
    email_smtp_host: Optional[str] = Field(default=None)
    email_smtp_port: int = Field(default=587)
    email_username: Optional[str] = Field(default=None)
    email_password: Optional[str] = Field(default=None)
    email_from: Optional[str] = Field(default=None)
    email_to: List[str] = Field(default_factory=list)
    file: bool = Field(default=False)
    file_path: Optional[str] = Field(default=None)

    @validator('webhook_url', 'email_password', 'email_username', pre=True, always=True)
    def expand_env_vars(cls, v: Optional[str]) -> Optional[str]:
        """Expand environment variables in sensitive string fields.

        Supports ${VAR_NAME} syntax for environment variable substitution.
        Example: ${EMAIL_PASSWORD} will be replaced with the value of
        the EMAIL_PASSWORD environment variable.
        """
        if v is None or not isinstance(v, str):
            return v

        # Match ${VAR_NAME} pattern
        env_pattern = re.compile(r'\$\{([^}]+)\}')

        def replace_var(match: re.Match) -> str:
            var_name = match.group(1)
            env_value = os.environ.get(var_name)
            if env_value is None:
                LOGGER.warning("Environment variable %s not found, keeping placeholder", var_name)
                return match.group(0)  # Keep original if not found
            return env_value

        return env_pattern.sub(replace_var, v)


class TrustLayerConfig(BaseModel):
    """Data trust layer configuration for lineage, SLA, drift detection, and alerting."""
    enabled: bool = Field(
        default=True,
        description="Enable the data trust layer"
    )
    lineage_dir: str = Field(
        default="data/lineage",
        description="Directory for lineage data storage"
    )
    sla_check_freshness: bool = Field(
        default=True,
        description="Enable data freshness SLA checking"
    )
    sla_max_data_age_hours: float = Field(
        default=24.0,
        ge=0,
        description="Maximum allowed data age in hours"
    )
    sla_max_processing_time_minutes: float = Field(
        default=30.0,
        ge=0,
        description="Maximum allowed processing time in minutes"
    )
    drift_detection_dir: str = Field(
        default="data/schema_baselines",
        description="Directory for schema drift baselines"
    )
    drift_track_additions: bool = Field(default=True)
    drift_track_removals: bool = Field(default=True)
    drift_track_type_changes: bool = Field(default=True)
    drift_critical_columns: List[str] = Field(default_factory=list)
    alerting: AlertChannelConfig = Field(default_factory=AlertChannelConfig)
    alert_throttle_minutes: int = Field(
        default=5,
        ge=0,
        description="Minutes to throttle duplicate alerts"
    )
    contracts_dir: str = Field(
        default="data/contracts",
        description="Directory for data contract storage"
    )


class PipelineConfig(BaseModel):
    """Main pipeline configuration schema."""
    pipeline: dict = Field(
        default={"name": "bank_transaction_pipeline", "environment": "local"},
        description="General pipeline metadata"
    )
    paths: PathsConfig
    data_schema: SchemaConfig = Field(default_factory=SchemaConfig)
    business_rules: BusinessRulesConfig = Field(default_factory=BusinessRulesConfig)
    reconciliation: ReconciliationConfig = Field(default_factory=ReconciliationConfig)
    processing: ProcessingConfig = Field(default_factory=ProcessingConfig)
    aggregation: AggregationConfig = Field(default_factory=AggregationConfig)
    aml_detection: AmlDetectionConfig = Field(default_factory=AmlDetectionConfig)
    trust_layer: TrustLayerConfig = Field(default_factory=TrustLayerConfig)

    @validator('paths')
    def validate_paths(cls, v):
        """Validate that paths are properly formatted."""
        if isinstance(v, dict):
            # Convert dict to PathsConfig if needed
            return PathsConfig(**v)
        return v

    class Config:
        """Pydantic configuration."""
        validate_assignment = True
        extra = "allow"  # Allow extra fields for future extensibility


def validate_config(config_dict: dict) -> PipelineConfig:
    """
    Validate a configuration dictionary against the schema.

    Parameters
    ----------
    config_dict : dict
        The configuration dictionary loaded from YAML

    Returns
    -------
    PipelineConfig
        Validated configuration object

    Raises
    ------
    ValidationError
        If configuration is invalid
    """
    try:
        validated_config = PipelineConfig(**config_dict)
        LOGGER.info("Configuration validation passed ✔")
        return validated_config
    except ValidationError as e:
        LOGGER.error("Configuration validation failed ✖: %s", e)
        raise


def load_and_validate_config(config_path: str = "config/pipeline_config.yaml") -> PipelineConfig:
    """
    Load and validate configuration from YAML file.

    Parameters
    ----------
    config_path : str
        Path to the configuration YAML file

    Returns
    -------
    PipelineConfig
        Validated configuration object

    Raises
    ------
    FileNotFoundError
        If config file doesn't exist
    ValidationError
        If configuration is invalid
    """
    import yaml
    from pathlib import Path

    config_file = Path(config_path)
    if not config_file.exists():
        # Try relative to project root
        project_root = Path(__file__).parent.parent
        config_file = project_root / config_path
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_file, "r") as f:
        raw_config = yaml.safe_load(f)

    # Validate against schema
    validated_config = validate_config(raw_config)

    return validated_config