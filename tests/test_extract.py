import io
import pytest

from src.extract import extract_transactions, SchemaValidationError

CSV = """txn_id,account_id,txn_date,ingestion_date,amount,currency,txn_type,status
t1,acc1,2024-01-01,2024-01-01,100,EUR,CARD,COMPLETED
t2,acc2,2024-01-02,2024-01-02,200,EUR,CARD,COMPLETED
"""

def test_extract_success():
    """CSV supplied via a StringIO must be read correctly and keep column order."""
    df = extract_transactions(io.StringIO(CSV))
    assert len(df) == 2
    assert list(df.columns) == [
        "txn_id",
        "account_id",
        "txn_date",
        "ingestion_date",
        "amount",
        "currency",
        "txn_type",
        "status",
    ]


def test_extract_duplicate():
    """A duplicate ``txn_id`` should raise ``SchemaValidationError``."""
    dup_csv = CSV + "t1,acc3,2024-01-03,2024-01-04,50,EUR,CARD,COMPLETED\n"
    with pytest.raises(SchemaValidationError, match="Duplicate txn_id"):
        extract_transactions(io.StringIO(dup_csv))


def test_extract_missing_columns():
    """Missing required columns should raise SchemaValidationError with details."""
    # CSV missing 'status' column
    incomplete_csv = """txn_id,account_id,txn_date,ingestion_date,amount,currency,txn_type
    t1,acc1,2024-01-01,2024-01-01,100,EUR,CARD
    """
    with pytest.raises(SchemaValidationError, match="Missing required columns"):
        extract_transactions(io.StringIO(incomplete_csv))


def test_extract_null_txn_id():
    """CSV with null txn_id should be handled appropriately."""
    null_csv = """txn_id,account_id,txn_date,ingestion_date,amount,currency,txn_type,status
    ,acc1,2024-01-01,2024-01-01,100,EUR,CARD,COMPLETED
    t2,acc2,2024-01-02,2024-01-02,200,EUR,CARD,COMPLETED
    """
    # Currently extract allows null txn_ids - this documents the behavior
    df = extract_transactions(io.StringIO(null_csv))
    assert len(df) == 2


def test_extract_empty_dataframe():
    """Empty CSV (only header) should return an empty DataFrame."""
    empty_csv = """txn_id,account_id,txn_date,ingestion_date,amount,currency,txn_type,status
    """
    df = extract_transactions(io.StringIO(empty_csv))
    assert len(df) == 0
    assert list(df.columns) == [
        "txn_id",
        "account_id",
        "txn_date",
        "ingestion_date",
        "amount",
        "currency",
        "txn_type",
        "status",
    ]
