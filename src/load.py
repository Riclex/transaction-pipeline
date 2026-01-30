"""Utilities for persisting the ledger dataframe

The function :func:`load_ledger` writes a parquet file in an
*atomic* (write‑to‑temp‑then‑rename) fashion and guarantees that the
ledger contains a unique ``txn_id`` column with no missing values
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import List, Union

import pandas as pd

# Logging 
LOGGER = logging.getLogger(__name__)

class SchemaValidationError(ValueError):
    """Raised when the ledger dataframe does not meet the required schema"""
    pass

def choose_parquet_engine() -> Union[str, None]:
    """
    Select the Parquet engine to use based on availability

    Returns
        The name of the Parquet engine to use (``"pyarrow"`` or ``"fastparquet"``)
        or ``None`` if neither is available
    """
    try:
        import pyarrow  # noqa: F401
        return "pyarrow"
    except ImportError:
        pass

    try:
        import fastparquet  # noqa: F401
        return "fastparquet"
    except ImportError:
        return None

# Public API
def load_ledger(
    ledger_df: pd.DataFrame,
    output_path: Union[str, os.PathLike, Path] = "data/processed/ledger_transactions.parquet",
    *,
    compression: str = "snappy",
    engine: str = "pyarrow",
) -> Path:
    """
    Persist a ledger dataframe to a Parquet file

    Parameters
    ledger_df : pandas.DataFrame
        The ledger to be saved.  It **must** contain a column named
        ``"txn_id"`` with **no nulls** and **unique** values
    output_path : str | os.PathLike | pathlib.Path, optional
        Destination file.  Parent directories are created automatically
        Defaults to ``"data/processed/ledger_transactions.parquet"`
    compression : str, optional
        Parquet compression algorithm (e.g. ``"snappy"``, ``"gzip"``, ``"brotli"``)
        ``"snappy"`` is the default because it balances speed and size
    engine : str, optional
        Parquet engine to use – ``"pyarrow"`` (default) or ``"fastparquet"``
        Explicitly setting it avoids pandas falling back to an unexpected engine

    Returns
    pathlib.Path
        The absolute path of the file that was written
    Raises
    TypeError
        If ``ledger_df`` is not a ``pandas.DataFrame``
    ValueError
        If ``txn_id`` column is missing, contains nulls, or contains duplicates
    IOError
        If the parquet file cannot be written (e.g. permission denied,
        disk full, etc.)
    """
    # Check txn_id column exists first to give a clear error message
    if "txn_id" not in ledger_df.columns:
        raise ValueError("Ledger is missing required column: txn_id")

    # null check for txn_id in load_ledger() raise a ValueError with the count of null values
    # if any are found, before proceeding to the duplicate check
    if ledger_df["txn_id"].isna().any():
        null_count = ledger_df["txn_id"].isna().sum()
        raise ValueError(f"Ledger contains {null_count} null txn_id values")

    if ledger_df["txn_id"].duplicated().any():
        dups = (
            ledger_df.loc[ledger_df["txn_id"].duplicated(), "txn_id"]
            .drop_duplicates()
            .tolist()
        )
        raise ValueError(f"Duplicate txn_id detected: {dups}")

    # Ensure the output directory exists. ``Path.resolve()`` makes the
    # path absolute which helps with debugging and with atomic write later
    path = Path(output_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)

    if engine is None:
        engine = choose_parquet_engine()
    
    if engine:
        temp_path = path.with_suffix(".tmp.parquet")
        try:
            ledger_df.to_parquet(
                temp_path,
                index=False,
                compression=compression,
                engine=engine,
            )
        except Exception as exc:            # pragma: no cover - unexpected I/O error
            raise IOError(f"Failed to write ledger parquet to {temp_path}") from exc
        
        # Atomic replace, guarantees either the whole file appears or nothing
        try:
            temp_path.replace(path)
        except OSError as exc:
            # Clean up the half-written temp file, then raise a clear error
            try:
                temp_path.unlink(missing_ok=True)
            finally:
                raise IOError(f"Could not atomically move {temp_path} -> {path}") from exc

        LOGGER.info(
            "Ledger persisted: %d rows written to %s",
            len(ledger_df),
            path,
        )
        return path
    else:
        # Fallback → CSV (only runs when no parquet engine available)
        csv_path = path.with_suffix(".csv")
        temp_csv = csv_path.with_suffix(".tmp.csv")
        try:
            ledger_df.to_csv(temp_csv, index=False)
            temp_csv.replace(csv_path)      # atomic rename
        except Exception as exc:            # pragma: no cover
            raise IOError(f"Failed to write ledger CSV to {temp_csv}") from exc

        LOGGER.warning(
            "No Parquet engine available – ledger written as CSV to %s (fallback mode)",
            csv_path,
        )
        return csv_path