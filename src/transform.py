"""Transformation utilities for raw bank‑transaction records

The public entry point is :func:`transform_transactions`, which receives a
``pandas.DataFrame`` that must contain at least the columns listed in
``REQUIRED_RAW_COLUMNS``.  The function returns a tuple:

* ``clean_df`` – the original data enriched with normalized,
  audit‑ready columns
* ``ledger_df`` – a subset of rows that are considered successful
  and shaped for downstream accounting

Both DataFrames are copies, so the caller may safely mutate them without
affecting the original input
"""

from __future__ import annotations

import logging
from typing import Tuple
import numpy as np
import pandas as pd

# Logging 
LOGGER = logging.getLogger(__name__)

# Constants that drive the transformation logic
SUCCESS_STATUSES = {"COMPLETED", "SETTLED", "OK"}

# Columns we must find in the raw data before we start transforming
REQUIRED_RAW_COLUMNS = {
    "txn_id",
    "account_id",
    "txn_date",
    "ingestion_date",
    "amount",
    "currency",
    "txn_type",
    "status",
}

# Columns that a ledger‑ready record must expose (order is important for
# downstream downstream imports such as ``load_ledger``)
LEDGER_COLUMNS = [
    "txn_id",
    "account_id",
    "txn_date",
    "ingestion_date",
    "amount_clean",          # renamed to "amount" after cleaning
    "currency",
    "txn_type",
    "is_late",
]

# Helper functions – kept deliberately small and vectorised where possible
def validate_input_columns(df: pd.DataFrame) -> None:
    """Raise a clear error if any required column is missing"""
    missing = REQUIRED_RAW_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Raw dataframe is missing required columns: {sorted(missing)}")


def normalize_status(status: object) -> str:
    """Map raw status values to a canonical ``SUCCESS``/``FAILED`` flag

    * Missing / NaN → ``"FAILED"``
    * Case‑insensitive match against :data:`SUCCESS_STATUSES` → ``"SUCCESS"``
    * Anything else → ``"FAILED"``
    """
    if pd.isna(status):
        return "FAILED"
    # ``str`` handles numbers, booleans etc.  ``upper`` is safe because we
    # now know the value is a string representation
    return "SUCCESS" if str(status).upper() in SUCCESS_STATUSES else "FAILED"

def normalize_amount(amount: object, txn_type: object) -> float:
    """
    Return a signed, normalised amount

    * ``NaN`` → ``0.0`` (so the row stays in the data set)
    * Cast to ``float`` (strings like "‑10.5" become a float automatically)
    * If the transaction type is a refund we *explicitly* make the amount negative
    * Otherwise we **keep the original sign** – this works for
      standard banking feeds where credits are positive and debits are negative
      If the source uses a different convention you can extend the
      ``TXN_TYPE_SIGN`` mapping below

    Parameters
    amount : object
        The raw amount as read from the CSV (may be a float, int or string)
    txn_type : object
        Transaction type – used only for the special ``REFUND`` case

    Returns
    float
        A signed, clean amount ready for aggregation
    """
    if pd.isna(amount):
        return 0.0

    # Make sure we are dealing with a number.
    try:
        amount = float(amount)
    except Exception as exc:  # pragma: no cover – defensive
        raise ValueError(f"Unable to coerce amount {amount!r} to float") from exc

    # Optional mapping for transaction‑type‑based sign handling
    TXN_TYPE_SIGN = {
        "REFUND": -1,        # refunds are always a debit (negative)
        # "WITHDRAWAL": -1,   # uncomment if withdrawals are stored without sign
        # "DEBIT": -1,
        # "CREDIT": 1,
    }

    sign = TXN_TYPE_SIGN.get(str(txn_type).strip().upper(), 1)  # default = keep sign
    # ``sign`` is either -1 (force negative) or 1 (keep original sign).
    #return sign * amount if sign == -1 else amount
    return -abs(amount) if sign == -1 else amount

def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Parse and normalise the ``txn_date`` and ``ingestion_date`` columns

    The function returns the *same* DataFrame (mutated) for convenience
    """
    # ``errors="coerce"`` converts unparsable strings into NaT
    df["txn_date"] = pd.to_datetime(df["txn_date"], errors="coerce")
    df["ingestion_date"] = pd.to_datetime(df["ingestion_date"], errors="coerce")

    # At this point both columns are ``datetime64[ns]``.  ``normalize`` forces
    # the time component to midnight (00:00) which is useful for the
    # ``is_late`` flag later
    df["ingestion_date"] = df["ingestion_date"].dt.normalize()

    # Detect parsing failures early, a helpful message points to the column
    if df["txn_date"].isna().any():
        raise ValueError("Invalid txn_date found after parsing")
    if df["ingestion_date"].isna().any():
        raise ValueError("Invalid ingestion_date found after parsing")

    return df


def transform_transactions(
    raw_df: pd.DataFrame,
    success_statuses: set[str] | None = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Clean and enrich a raw transaction DataFrame

    Parameters
    raw_df : pandas.DataFrame
        The raw, extracted transaction feed.  It **must** contain the columns
        listed in :data:`REQUIRED_RAW_COLUMNS`
    success_statuses : set[str], optional
        Set of status values that indicate successful transactions.
        Defaults to {"COMPLETED", "SETTLED", "OK"}

    Returns
    tuple[pandas.DataFrame, pandas.DataFrame]
        ``clean_df`` – the input enriched with normalized columns
        (``status_clean``, ``amount_clean``, ``is_late``) and all original
        fields retained

        ``ledger_df`` – a filtered view containing only rows whose cleaned
        status is ``"SUCCESS"`` and limited to the columns defined in
        :data:`LEDGER_COLUMNS`.  The ``amount`` column in this subset is the
        signed, cleaned amount

    Raises
    ValueError
        If required columns are missing or any date fails to parse
    """
    # Use provided success statuses or default
    effective_success_statuses = success_statuses or SUCCESS_STATUSES

    # Defensive copy – we do not want to mutate the caller's DataFrame
    df = raw_df.copy()

    # Validate schema before we start touching the data
    validate_input_columns(df)

    # Parse date columns, making both datetime64[ns] with midnight time
    df = parse_dates(df)

    # Normalise status – vectorised string handling is faster than
    #    ``apply`` per row
    # ``fillna`` ensures we do not get NaN propagating into the string
    # operation; ``str.upper`` works on a pandas Series directly
    df["status_clean"] = (
        df["status"]
        .fillna("")
        .astype(str)
        .str.upper()
        .isin(effective_success_statuses)
        .map({True: "SUCCESS", False: "FAILED"})
    )

    # Normalise amount, fully vectorised (no ``apply``)
    # Missing amounts become 0.0, then we take the absolute value
    amount_series = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)

    # Apply the “refund → negative” rule
    refund_mask = df["txn_type"].astype(str).str.upper() == "REFUND"
    amount_series.loc[refund_mask] = -amount_series.loc[refund_mask].abs()

    df["amount_clean"] = amount_series.astype(float)

    # Late‑arrival detection. Both columns are ``datetime64[ns]`` so the
    # comparison is efficient
    df["is_late"] = (df["txn_date"] < df["ingestion_date"]).apply(bool)

    # Build the ledger‑only view (SUCCESS rows only) and keep just the
    # columns required downstream
    ledger_df = (
        df.loc[df["status_clean"] == "SUCCESS", LEDGER_COLUMNS]
        .copy()
        .rename(columns={"amount_clean": "amount"})
    )


    # Quick sanity‑check (optional).  If something went wrong we log a
    # warning – the explicit reconciliation step will raise a clear error
    # later if the mismatch is real
    raw_success = df[df["status_clean"] == "SUCCESS"]
    raw_sum = raw_success["amount_clean"].sum()
    ledger_sum = ledger_df["amount"].sum()
    if not np.isclose(raw_sum, ledger_sum, atol=1e-6):
        LOGGER.warning(
            "Sanity check failed - raw_success sum (%.6f) != ledger sum (%.6f)",
            raw_sum,
            ledger_sum,
        )

    # Log a quick summary – helpful when the pipeline runs under an
    # orchestrator (Airflow, Prefect, etc.)
    skipped_rows = len(df) - len(ledger_df)
    LOGGER.info(
        "Transformation complete: %d rows total, %d rows in ledger, %d rows skipped",
        len(df),
        len(ledger_df),
        skipped_rows,
    )

    # Return the enriched (audit‑ready) DataFrame and the ledger subset
    return df, ledger_df