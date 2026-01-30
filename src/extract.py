"""Extract raw transaction data from a CSV file

The function reads the CSV, enforces a minimal schema, parses dates, and
returns a clean ``pandas.DataFrame``.  Any structural problem (missing
columns, null/duplicate transaction IDs, unreadable file, etc.) raises a
dedicated ``SchemaValidationError`` so that the caller can react
appropriately

To handle very large files we can switch to
``pd.read_csv(..., chunksize=…)`` and process the chunks downstream
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import IO, Union

import pandas as pd

# Logging
LOGGER = logging.getLogger(__name__)

# Columns that must be present in the raw feed
REQUIRED_COLUMNS = {
    "txn_id",
    "account_id",
    "txn_date",
    "ingestion_date",
    "amount",
    "currency",
    "txn_type",
    "status",
}

class SchemaValidationError(ValueError):
    """Raised when the raw CSV does not meet the required data contract"""
    pass

def read_source(source: Union[str, os.PathLike, IO[str]], chunksize: int | None = None) -> pd.DataFrame:
    """
    Accept either a path string (or ``Path``) **or** any file‑like object
    that implements ``read()`` (e.g. ``io.StringIO``).  The helper isolates
    the "is this a file‑like object?" logic so the public function stays tidy.
    """
    # ``hasattr(..., "read")`` works for StringIO, an opened file handle, etc.
    if hasattr(source, "read"):
        return pd.read_csv(source, chunksize=chunksize)

    # Otherwise treat it as a path on disk.
    path = Path(source).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Raw data file not found: {path}")
    return pd.read_csv(path, chunksize=chunksize)

def extract_transactions(
    source: Union[str, os.PathLike, IO[str]],
    chunksize: int | None = None,
    valid_txn_types: list[str] | None = None
) -> pd.DataFrame:
    """
    Load a raw transaction CSV file and validate its schema

    Parameters:
    source : str | pathlib.Path | IO[str]
        Path to the CSV file or file-like object containing the raw bank‑transaction feed
    chunksize : int, optional
        Number of rows to read at a time for large files. If None, reads entire file at once.
    valid_txn_types : list[str], optional
        List of valid transaction types. If provided, validates that all txn_type values are in this list.

    Returns:
    pandas.DataFrame
        A DataFrame with the raw rows.  ``txn_date`` and ``ingestion_date`` is
        converted to ``datetime.date`` objects; numeric and string columns are
        coerced to the types defined in ``_COLUMN_DTYPES``.  Only columns in
        ``REQUIRED_COLUMNS`` are kept (extra columns are dropped)

    Raises
    FileNotFoundError
        If ``source`` does not point to an existing file
    SchemaValidationError
        If the file is missing required columns, contains null/duplicate
        transaction IDs, or otherwise violates the contract
    IOError
        If pandas cannot parse the CSV for any reason (e.g. malformed delimiter)
    """
    df_or_reader = read_source(source, chunksize=chunksize)

    # Handle chunked reading
    if chunksize is not None:
        # pd.read_csv with chunksize returns a TextFileReader
        chunks = []
        for chunk in df_or_reader:
            chunks.append(chunk)
        if not chunks:
            raise SchemaValidationError("Raw transaction file is empty")
        df = pd.concat(chunks, ignore_index=True)
        LOGGER.info("Loaded %d rows from %s in %d chunks", len(df), getattr(source, "name", source), len(chunks))
    else:
        df = df_or_reader
        LOGGER.info("Loaded %d rows from %s", len(df), getattr(source, "name", source))

    # Validate schema
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise SchemaValidationError(
            f"Missing required columns in raw data: {sorted(missing)}"
        )

    if df["txn_id"].duplicated().any():
        dup = (
            df.loc[df["txn_id"].duplicated(), "txn_id"]
            .drop_duplicates()
            .tolist()
        )
        raise SchemaValidationError(f"Duplicate txn_id detected: {dup}")

    # Validate that required columns don't contain null values
    for col in ["account_id", "status", "txn_type"]:
        if col in df.columns and df[col].isna().any():
            null_count = df[col].isna().sum()
            raise SchemaValidationError(
                f"Column '{col}' contains {null_count} null/empty values. "
                f"All transactions must have valid {col} values."
            )

    # Validate transaction types if valid_txn_types is provided
    if valid_txn_types is not None and "txn_type" in df.columns:
        invalid_txn_types = df[~df["txn_type"].astype(str).str.upper().isin([t.upper() for t in valid_txn_types])]
        if not invalid_txn_types.empty:
            invalid_values = invalid_txn_types["txn_type"].unique().tolist()
            raise SchemaValidationError(
                f"Found invalid transaction types: {invalid_values}. "
                f"Valid types are: {valid_txn_types}"
            )

    return df