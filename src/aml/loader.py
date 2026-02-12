"""Ledger input reader with schema validation for AML processing.

Reads the ledger Parquet file output by the main pipeline and validates
it contains the required columns for AML feature engineering.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Union

import pandas as pd

LOGGER = logging.getLogger(__name__)

# Required columns from ledger output
REQUIRED_LEDGER_COLUMNS = {
    "txn_id",
    "account_id",
    "txn_date",
    "ingestion_date",
    "amount",
    "currency",
    "txn_type",
    "is_late",
}


class AmlSchemaError(ValueError):
    """Raised when ledger data does not meet AML processing requirements."""
    pass


def load_ledger_for_aml(
    input_path: Union[str, Path],
    validate_schema: bool = True,
) -> pd.DataFrame:
    """
    Load ledger transactions from Parquet file for AML analysis.

    Parameters
    input_path : str | Path
        Path to the ledger Parquet file (typically
        data/processed/ledger_transactions.parquet)
    validate_schema : bool, default True
        Whether to validate required columns are present

    Returns
    pd.DataFrame
        Ledger DataFrame with validated schema

    Raises
    FileNotFoundError
        If input file does not exist
    AmlSchemaError
        If required columns are missing or file cannot be read
    """
    path = Path(input_path).expanduser().resolve()

    if not path.exists():
        raise FileNotFoundError(f"Ledger file not found: {path}")

    if not path.suffix == ".parquet":
        raise AmlSchemaError(f"Expected Parquet file, got: {path.suffix}")

    try:
        df = pd.read_parquet(path)
    except Exception as exc:
        raise AmlSchemaError(f"Failed to read ledger Parquet: {exc}") from exc

    if validate_schema:
        missing = REQUIRED_LEDGER_COLUMNS - set(df.columns)
        if missing:
            raise AmlSchemaError(
                f"Ledger missing required columns for AML: {sorted(missing)}"
            )

    # Ensure datetime types
    df["txn_date"] = pd.to_datetime(df["txn_date"])
    df["ingestion_date"] = pd.to_datetime(df["ingestion_date"])

    LOGGER.info("Loaded %d ledger transactions for AML analysis", len(df))
    return df
