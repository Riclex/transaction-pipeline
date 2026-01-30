"""Configuration validation schema using Pydantic

This module provides validation for the pipeline configuration YAML file
to ensure all required fields are present and have valid values.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional, Union
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