"""Data contract enforcement for producer-consumer agreements.

Defines and enforces contracts between data producers and consumers,
ensuring data meets agreed-upon specifications.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable

import pandas as pd
from pydantic import BaseModel, Field, validator

LOGGER = logging.getLogger(__name__)


class ConstraintType(str, Enum):
    """Types of data constraints."""

    NOT_NULL = "not_null"
    UNIQUE = "unique"
    RANGE = "range"
    ENUM = "enum"
    REGEX = "regex"
    REFERENTIAL = "referential"
    CUSTOM = "custom"


class ContractStatus(str, Enum):
    """Contract validation status."""

    VALID = "valid"
    VIOLATED = "violated"
    WARNING = "warning"


class ColumnContract(BaseModel):
    """Contract for a single column."""

    name: str
    dtype: str
    nullable: bool = False
    unique: bool = False
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    allowed_values: Optional[List[str]] = None
    regex_pattern: Optional[str] = None
    description: str = ""

    @validator("max_value")
    def max_greater_than_min(cls, v, values):
        if v is not None and values.get("min_value") is not None:
            if v < values["min_value"]:
                raise ValueError("max_value must be greater than min_value")
        return v


class DataContract(BaseModel):
    """Complete data contract definition."""

    name: str
    version: str = "1.0.0"
    description: str = ""
    producer: str
    consumers: List[str] = Field(default_factory=list)
    columns: List[ColumnContract]
    row_count_min: Optional[int] = None
    row_count_max: Optional[int] = None
    freshness_hours: Optional[float] = None
    custom_validators: List[str] = Field(default_factory=list)
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class ContractViolation:
    """Single contract violation."""

    constraint_type: ConstraintType
    column_name: str
    message: str
    severity: str  # 'error', 'warning'
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ContractValidationResult:
    """Result of contract validation."""

    contract_name: str
    version: str
    timestamp: str
    status: ContractStatus
    violations: List[ContractViolation]
    row_count: int
    column_count: int
    validation_duration_ms: float

    @property
    def is_valid(self) -> bool:
        return self.status == ContractStatus.VALID

    @property
    def error_count(self) -> int:
        return sum(1 for v in self.violations if v.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for v in self.violations if v.severity == "warning")


class DataContractEnforcer:
    """Enforces data contracts between producers and consumers."""

    def __init__(self, contracts_dir: Optional[Path] = None):
        """Initialize contract enforcer.

        Args:
            contracts_dir: Directory to store/load contracts
        """
        self.contracts_dir = contracts_dir or Path("data/contracts")
        self.contracts_dir.mkdir(parents=True, exist_ok=True)
        self._contracts: Dict[str, DataContract] = {}
        self._custom_validators: Dict[str, Callable[[pd.DataFrame], List[ContractViolation]]] = {}

    def register_contract(self, contract: DataContract) -> None:
        """Register a data contract.

        Args:
            contract: Contract to register
        """
        self._contracts[contract.name] = contract
        LOGGER.info("Registered contract: %s v%s", contract.name, contract.version)

    def save_contract(self, contract: DataContract) -> Path:
        """Save contract to file.

        Args:
            contract: Contract to save

        Returns:
            Path to saved contract
        """
        filepath = self.contracts_dir / f"{contract.name}_contract.json"

        with open(filepath, "w") as f:
            json.dump(contract.model_dump(), f, indent=2)

        LOGGER.info("Saved contract to %s", filepath)
        return filepath

    def load_contract(self, name: str) -> Optional[DataContract]:
        """Load contract from file.

        Args:
            name: Contract name

        Returns:
            Contract if found, None otherwise
        """
        filepath = self.contracts_dir / f"{name}_contract.json"

        if not filepath.exists():
            return None

        with open(filepath) as f:
            data = json.load(f)

        contract = DataContract(**data)
        self.register_contract(contract)
        return contract

    def register_custom_validator(
        self,
        name: str,
        validator_func: Callable[[pd.DataFrame], List[ContractViolation]],
    ) -> None:
        """Register a custom validation function.

        Args:
            name: Validator name
            validator_func: Function that takes DataFrame and returns violations
        """
        self._custom_validators[name] = validator_func
        LOGGER.info("Registered custom validator: %s", name)

    def validate(
        self,
        df: pd.DataFrame,
        contract_name: str,
    ) -> ContractValidationResult:
        """Validate DataFrame against a contract.

        Args:
            df: DataFrame to validate
            contract_name: Name of contract to validate against

        Returns:
            Validation result with status and violations
        """
        import time

        start_time = time.time()

        contract = self._contracts.get(contract_name) or self.load_contract(contract_name)
        if not contract:
            raise ValueError(f"Contract not found: {contract_name}")

        violations: List[ContractViolation] = []

        # Check columns exist
        df_columns = set(df.columns)
        contract_columns = {c.name for c in contract.columns}

        missing_cols = contract_columns - df_columns
        for col in missing_cols:
            violations.append(ContractViolation(
                constraint_type=ConstraintType.NOT_NULL,
                column_name=col,
                message=f"Required column '{col}' is missing",
                severity="error",
            ))

        # Validate each column
        for col_contract in contract.columns:
            if col_contract.name not in df.columns:
                continue

            col_violations = self._validate_column(df, col_contract)
            violations.extend(col_violations)

        # Check row count constraints
        if contract.row_count_min is not None and len(df) < contract.row_count_min:
            violations.append(ContractViolation(
                constraint_type=ConstraintType.CUSTOM,
                column_name="*",
                message=f"Row count {len(df)} below minimum {contract.row_count_min}",
                severity="error",
                details={"actual": len(df), "minimum": contract.row_count_min},
            ))

        if contract.row_count_max is not None and len(df) > contract.row_count_max:
            violations.append(ContractViolation(
                constraint_type=ConstraintType.CUSTOM,
                column_name="*",
                message=f"Row count {len(df)} exceeds maximum {contract.row_count_max}",
                severity="error",
                details={"actual": len(df), "maximum": contract.row_count_max},
            ))

        # Run custom validators
        for validator_name in contract.custom_validators:
            if validator_name in self._custom_validators:
                custom_violations = self._custom_validators[validator_name](df)
                violations.extend(custom_violations)

        # Determine status
        errors = [v for v in violations if v.severity == "error"]
        status = ContractStatus.VALID if not errors else ContractStatus.VIOLATED

        duration_ms = (time.time() - start_time) * 1000

        result = ContractValidationResult(
            contract_name=contract.name,
            version=contract.version,
            timestamp=datetime.now(timezone.utc).isoformat(),
            status=status,
            violations=violations,
            row_count=len(df),
            column_count=len(df.columns),
            validation_duration_ms=duration_ms,
        )

        LOGGER.info(
            "Contract validation: %s %s (%d errors, %d warnings)",
            contract.name,
            status.value,
            result.error_count,
            result.warning_count,
        )

        return result

    def _validate_column(
        self,
        df: pd.DataFrame,
        col_contract: ColumnContract,
    ) -> List[ContractViolation]:
        """Validate a single column against its contract."""
        violations = []
        col_name = col_contract.name
        series = df[col_name]

        # Check nullability
        if not col_contract.nullable and series.isna().any():
            null_count = series.isna().sum()
            violations.append(ContractViolation(
                constraint_type=ConstraintType.NOT_NULL,
                column_name=col_name,
                message=f"Column '{col_name}' has {null_count} null values",
                severity="error",
                details={"null_count": int(null_count)},
            ))

        # Check uniqueness
        if col_contract.unique and series.duplicated().any():
            dup_count = series.duplicated().sum()
            violations.append(ContractViolation(
                constraint_type=ConstraintType.UNIQUE,
                column_name=col_name,
                message=f"Column '{col_name}' has {dup_count} duplicate values",
                severity="error",
                details={"duplicate_count": int(dup_count)},
            ))

        # Check range (for numeric columns)
        if col_contract.min_value is not None or col_contract.max_value is not None:
            try:
                numeric_series = pd.to_numeric(series, errors="coerce")
                if col_contract.min_value is not None:
                    below_min = (numeric_series < col_contract.min_value).sum()
                    if below_min > 0:
                        violations.append(ContractViolation(
                            constraint_type=ConstraintType.RANGE,
                            column_name=col_name,
                            message=f"{below_min} values below minimum {col_contract.min_value}",
                            severity="error",
                            details={"min": col_contract.min_value, "count": int(below_min)},
                        ))

                if col_contract.max_value is not None:
                    above_max = (numeric_series > col_contract.max_value).sum()
                    if above_max > 0:
                        violations.append(ContractViolation(
                            constraint_type=ConstraintType.RANGE,
                            column_name=col_name,
                            message=f"{above_max} values above maximum {col_contract.max_value}",
                            severity="error",
                            details={"max": col_contract.max_value, "count": int(above_max)},
                        ))
            except Exception:
                pass  # Skip range check for non-numeric columns

        # Check allowed values (enum)
        if col_contract.allowed_values is not None:
            invalid_values = set(series.dropna().unique()) - set(col_contract.allowed_values)
            if invalid_values:
                violations.append(ContractViolation(
                    constraint_type=ConstraintType.ENUM,
                    column_name=col_name,
                    message=f"Invalid values in '{col_name}': {invalid_values}",
                    severity="error",
                    details={"invalid_values": list(invalid_values)},
                ))

        return violations

    def generate_contract_report(
        self,
        validation_result: ContractValidationResult,
        output_path: Optional[Path] = None,
    ) -> Path:
        """Generate validation report.

        Args:
            validation_result: Result to report
            output_path: Output file path (optional)

        Returns:
            Path to generated report
        """
        if output_path is None:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            output_path = self.contracts_dir / f"validation_report_{timestamp}.json"

        report = {
            **asdict(validation_result),
            "violations": [asdict(v) for v in validation_result.violations],
        }

        with open(output_path, "w") as f:
            json.dump(report, f, indent=2, default=str)

        LOGGER.info("Contract report generated: %s", output_path)
        return output_path


# Global contract enforcer instance
_contract_enforcer: Optional[DataContractEnforcer] = None


def init_contract_enforcer(contracts_dir: Optional[Path] = None) -> DataContractEnforcer:
    """Initialize global contract enforcer."""
    global _contract_enforcer
    _contract_enforcer = DataContractEnforcer(contracts_dir)
    return _contract_enforcer


def get_contract_enforcer() -> Optional[DataContractEnforcer]:
    """Get global contract enforcer instance."""
    return _contract_enforcer
