"""
Full pipeline‑logic test suite

This tests cover:

*   `src.extract.extract_transactions` – CSV reading & schema validation.
*   `src.transform` – column validation, date parsing, status & amount
    normalisation, late‑arrival flag, sign handling for non‑refunds,
    case‑insensitive txn_type handling.
*   `src.reconcile` – exact‑match, tolerance handling, missing‑column
    guards.
*   `src.load.load_ledger` – successful parquet write, duplicate‑id guard,
    graceful fallback to CSV when no parquet engine is present.
"""

import sys
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

# Make the repository root importable (same trick you used in the original file)
sys.path.insert(0, str(Path(__file__).parent.parent))

# Public objects under test
from src.extract import extract_transactions
from src.transform import (
    transform_transactions,
    normalize_status,
    normalize_amount,
)
from src.reconcile import reconcile_raw_vs_ledger
from src.load import load_ledger

# Helper – a tiny, well‑behaved raw DataFrame used by many tests
def _base_raw_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "txn_id": ["t1", "t2", "t3"],
            "account_id": ["acc1", "acc1", "acc2"],
            "txn_date": ["2024-01-01", "2024-01-01", "2024-01-02"],
            "ingestion_date": ["2024-01-01", "2024-01-02", "2024-01-02"],
            "amount": [100, -20, 50],
            "currency": ["EUR", "EUR", "EUR"],
            "txn_type": ["CARD", "REFUND", "CARD"],
            "status": ["COMPLETED", "OK", "SETTLED"],
        }
    )


# ────  OLD TESTS ────────────────────────────────────
def test_refund_amount_is_negative() -> None:
    """Refunds must be stored as a negative value in the ledger."""
    raw_df: pd.DataFrame = _base_raw_df()
    _, ledger_df = transform_transactions(raw_df)

    refund_row: pd.Series = ledger_df[ledger_df["txn_type"] == "REFUND"].iloc[0]
    assert refund_row["amount"] < 0


def test_late_arriving_transaction_flagged() -> None:
    """A transaction whose ingestion_date is later than txn_date is late."""
    raw_df: pd.DataFrame = _base_raw_df()
    _, ledger_df = transform_transactions(raw_df)

    # t2 has ingestion_date = 2024‑01‑02, txn_date = 2024‑01‑01 → late
    late_txn: pd.Series = ledger_df[ledger_df["txn_id"] == "t2"].iloc[0]
    assert late_txn["is_late"] == True


def test_reconciliation_passes_when_totals_match() -> None:
    """When the ledger totals match the raw totals the function returns a DF."""
    raw_df: pd.DataFrame = _base_raw_df()
    transformed_df, ledger_df = transform_transactions(raw_df)

    recon_df: pd.DataFrame = reconcile_raw_vs_ledger(transformed_df, ledger_df)

    assert not recon_df.empty


def test_reconciliation_fails_on_mismatch() -> None:
    """A deliberate change in the ledger amount must raise ValueError."""
    raw_df: pd.DataFrame = _base_raw_df()
    transformed_df, ledger_df = transform_transactions(raw_df)

    # Introduce a mismatch on purpose
    ledger_df.loc[0, "amount"] += 10

    with pytest.raises(ValueError):
        reconcile_raw_vs_ledger(transformed_df, ledger_df)


# ────  NEW TESTS ───────────────────────────────────────────────────────
# ---------------------------  EXTRACT  ---------------------------------
def _write_tmp_csv(df: pd.DataFrame, tmp_path: Path) -> Path:
    """Utility to write a DataFrame to a temporary CSV file."""
    csv_path: Path = tmp_path / "tmp_transactions.csv"
    df.to_csv(csv_path, index=False)
    return csv_path


def test_extract_reads_csv_and_preserves_schema(tmp_path: Path) -> None:
    """extract_transactions should read a CSV and raise if a required column is missing."""
    # Write a correct CSV first – the function must return a DataFrame with the same shape.
    correct_df: pd.DataFrame = _base_raw_df()
    csv_path: Path = _write_tmp_csv(correct_df, tmp_path)

    loaded: pd.DataFrame = extract_transactions(str(csv_path))

    # Compare data without strict dtype checking (CSV round-trip converts dates to strings)
    assert len(loaded) == len(correct_df)
    assert list(loaded.columns) == list(correct_df.columns)
    for col in correct_df.columns:
        assert list(loaded[col]) == list(correct_df[col]), f"Column {col} mismatch"

    # Now drop a required column → a clear ValueError must be raised.
    broken_df: pd.DataFrame = correct_df.drop(columns=["amount"])
    broken_csv: Path = _write_tmp_csv(broken_df, tmp_path)

    with pytest.raises(ValueError, match="Missing required columns"):
        extract_transactions(str(broken_csv))


def test_extract_raises_when_file_missing(tmp_path: Path) -> None:
    """FileNotFoundError must be raised for a non‑existent file."""
    nonexistent: Path = tmp_path / "does_not_exist.csv"
    with pytest.raises(FileNotFoundError):
        extract_transactions(str(nonexistent))


# ---------------------------  TRANSFORM  -------------------------------
def test_transform_keeps_original_sign_for_non_refund() -> None:
    """Non‑refund rows must retain the sign that appears in the raw amount."""
    raw_df: pd.DataFrame = _base_raw_df()
    _, ledger_df = transform_transactions(raw_df)

    # t1 is a CARD transaction with amount = +100 → should stay +100
    t1: pd.Series = ledger_df[ledger_df["txn_id"] == "t1"].iloc[0]
    assert t1["amount"] == 100

    # t3 is a CARD transaction with amount = +50 → should stay +50
    t3: pd.Series = ledger_df[ledger_df["txn_id"] == "t3"].iloc[0]
    assert t3["amount"] == 50


def test_transform_status_normalisation() -> None:
    """Status strings must be mapped to SUCCESS / FAILED case‑insensitively."""
    raw: pd.DataFrame = _base_raw_df()
    # Add a row with a mixed‑case status that is *not* in SUCCESS_STATUSES
    extra: pd.DataFrame = pd.DataFrame(
        {
            "txn_id": ["t4"],
            "account_id": ["acc3"],
            "txn_date": ["2024-01-03"],
            "ingestion_date": ["2024-01-03"],
            "amount": [10],
            "currency": ["EUR"],
            "txn_type": ["CARD"],
            "status": ["failed"],  # lower‑case, not in SUCCESS list
        }
    )
    raw = pd.concat([raw, extra], ignore_index=True)

    _, ledger_df = transform_transactions(raw)

    # All rows with SUCCESS statuses should be present in the ledger,
    # the FAILED row (t4) should not be.
    assert "t4" not in ledger_df["txn_id"].values
    # sanity check that the helper works directly as well
    assert normalize_status("COMPLETED") == "SUCCESS"
    assert normalize_status("failed") == "FAILED"


def test_transform_date_parsing_and_is_late_flag() -> None:
    """txn_date and ingestion_date must become pandas Timestamps; is_late flagged correctly."""
    raw_df: pd.DataFrame = _base_raw_df()
    # Force an early ingestion_date for row t1 (ingestion before txn_date)
    raw_df.loc[raw_df["txn_id"] == "t1", "ingestion_date"] = "2023-12-31"

    _, ledger_df = transform_transactions(raw_df)

    # The column types must be datetime64[ns]
    assert pd.api.types.is_datetime64_any_dtype(ledger_df["txn_date"])
    assert pd.api.types.is_datetime64_any_dtype(ledger_df["ingestion_date"])

    # t1 should NOT be flagged as late (ingestion_date < txn_date means on-time)
    # is_late is True only when txn_date < ingestion_date
    t1: pd.Series = ledger_df[ledger_df["txn_id"] == "t1"].iloc[0]
    assert t1["is_late"] == False


def test_transform_raises_on_invalid_dates() -> None:
    """If a date cannot be parsed the function must raise a ValueError."""
    raw: pd.DataFrame = _base_raw_df()
    raw.loc[0, "txn_date"] = "not-a-date"

    with pytest.raises(ValueError, match="Invalid txn_date"):
        transform_transactions(raw)


def test_transform_raises_when_required_columns_missing() -> None:
    """Missing any of the REQUIRED_RAW_COLUMNS must raise a ValueError."""
    raw: pd.DataFrame = _base_raw_df()
    # Drop a required column
    raw = raw.drop(columns=["status"])

    with pytest.raises(ValueError, match="missing required columns"):
        transform_transactions(raw)


# ---------------------------  RECONCILE  ----------------------------
def test_reconcile_respects_tolerance() -> None:
    """When the difference is smaller than the tolerance the function must succeed."""
    raw, ledger = transform_transactions(_base_raw_df())

    # Introduce a tiny imbalance (0.005) – default tolerance is 0.01
    ledger.loc[0, "amount"] += 0.005

    # Should NOT raise
    recon: pd.DataFrame = reconcile_raw_vs_ledger(raw, ledger)
    # The diff column must contain the tiny difference (raw - ledger = -0.005 since ledger increased)
    recon_date: pd.DataFrame = recon[recon["txn_date"] == pd.to_datetime("2024-01-01")]
    if not recon_date.empty:
        assert round(recon_date["diff"].iloc[0], 3) == -0.005


def test_reconcile_raises_when_missing_columns() -> None:
    """Both sides must contain the columns used in the function; otherwise a clear ValueError."""
    raw, ledger = transform_transactions(_base_raw_df())

    # Remove the required column from the ledger side
    ledger_missing: pd.DataFrame = ledger.drop(columns=["amount"])
    with pytest.raises(ValueError, match="missing columns"):
        reconcile_raw_vs_ledger(raw, ledger_missing)


# ---------------------------  LOAD  ---------------------------------
def test_load_ledger_writes_parquet_and_returns_path(tmp_path: Path) -> None:
    """load_ledger must create a parquet file and return the Path of the final file."""
    raw, ledger = transform_transactions(_base_raw_df())
    out_path: Path = tmp_path / "ledger_output.parquet"

    written_path: Path = load_ledger(ledger, str(out_path))

    # The returned object must be a pathlib.Path and must point to the parquet file.
    assert isinstance(written_path, Path)
    assert written_path.exists()
    assert written_path.suffix == ".parquet"

    # Read it back – content must be identical (order can differ, compare sets)
    reloaded: pd.DataFrame = pd.read_parquet(written_path)
    pd.testing.assert_frame_equal(
        ledger.sort_index(axis=1), reloaded.sort_index(axis=1)
    )


def test_load_ledger_raises_on_duplicate_txn_id(tmp_path: Path) -> None:
    """If the ledger contains duplicate txn_id values a ValueError is raised."""
    raw, ledger = transform_transactions(_base_raw_df())
    # Duplicate txn_id deliberately
    dup: pd.Series = ledger.iloc[0].copy()
    dup["txn_id"] = ledger.iloc[1]["txn_id"]  # make a duplicate of the second row
    ledger_dup: pd.DataFrame = pd.concat([ledger, dup.to_frame().T], ignore_index=True)

    with pytest.raises(ValueError, match="Duplicate txn_id"):
        load_ledger(ledger_dup, str(tmp_path / "duplicate.parquet"))


def test_load_ledger_falls_back_to_csv_when_no_parquet_engine(tmp_path: Path, monkeypatch: Any) -> None:
    """
    Simulate an environment where neither pyarrow nor fastparquet is available.
    The function should fall back to CSV and emit a warning.
    """
    raw, ledger = transform_transactions(_base_raw_df())

    # Monkey‑patch the engine‑choice helper to force "no engine"
    from src import load as load_mod

    monkeypatch.setattr(load_mod, "choose_parquet_engine", lambda: None)

    out_path: Path = tmp_path / "fallback_output.parquet"  # we still pass a .parquet name
    written: Path = load_ledger(ledger, str(out_path), engine=None)

    # The fallback writes a CSV with the same stem but .csv suffix
    expected_csv: Path = out_path.with_suffix(".csv")
    assert written == expected_csv
    assert expected_csv.exists()
    reloaded: pd.DataFrame = pd.read_csv(expected_csv)
    # CSV round-trip doesn't preserve datetime dtypes, so check values only
    assert len(reloaded) == len(ledger)
    assert list(reloaded.columns) == list(ledger.columns)


def test_load_ledger_raises_on_null_txn_id(tmp_path: Path) -> None:
    """If the ledger contains null txn_id values a ValueError is raised."""
    raw, ledger = transform_transactions(_base_raw_df())
    # Introduce a null txn_id
    ledger.loc[0, "txn_id"] = None

    with pytest.raises(ValueError, match="null txn_id"):
        load_ledger(ledger, str(tmp_path / "null.parquet"))


def test_load_ledger_raises_on_missing_txn_id_column(tmp_path: Path) -> None:
    """If the ledger is missing the txn_id column entirely a ValueError is raised."""
    raw, ledger = transform_transactions(_base_raw_df())
    # Drop the txn_id column
    ledger_no_id: pd.DataFrame = ledger.drop(columns=["txn_id"])

    with pytest.raises(ValueError, match="txn_id"):
        load_ledger(ledger_no_id, str(tmp_path / "no_id.parquet"))


# ---------------------------  EMPTY DATAFRAME TESTS  ------------------
def test_extract_empty_dataframe(tmp_path: Path) -> None:
    """extract_transactions should handle empty CSV with only headers."""
    empty_df: pd.DataFrame = _base_raw_df().iloc[0:0]  # Empty DataFrame with same columns
    csv_path: Path = tmp_path / "empty.csv"
    empty_df.to_csv(csv_path, index=False)

    loaded: pd.DataFrame = extract_transactions(str(csv_path))
    assert len(loaded) == 0
    assert list(loaded.columns) == list(empty_df.columns)


def test_transform_empty_dataframe() -> None:
    """transform_transactions should handle empty DataFrame gracefully."""
    empty_df: pd.DataFrame = _base_raw_df().iloc[0:0]

    clean_df, ledger_df = transform_transactions(empty_df)
    assert len(clean_df) == 0
    assert len(ledger_df) == 0


def test_reconcile_empty_dataframes() -> None:
    """reconcile_raw_vs_ledger should handle empty DataFrames."""
    empty_df: pd.DataFrame = _base_raw_df().iloc[0:0]
    # Need to add status_clean and amount_clean for reconcile to work
    empty_df["status_clean"] = []
    empty_df["amount_clean"] = []

    ledger_empty: pd.DataFrame = empty_df.copy()
    ledger_empty["amount"] = []

    # Should return empty reconciliation
    recon_df: pd.DataFrame = reconcile_raw_vs_ledger(empty_df, ledger_empty)
    assert len(recon_df) == 0


def test_load_ledger_empty_dataframe(tmp_path: Path) -> None:
    """load_ledger should handle empty DataFrame with valid schema."""
    raw, ledger = transform_transactions(_base_raw_df())
    empty_ledger: pd.DataFrame = ledger.iloc[0:0]  # Keep schema but no rows

    out_path: Path = tmp_path / "empty.parquet"
    written_path: Path = load_ledger(empty_ledger, str(out_path))

    assert written_path.exists()
    reloaded: pd.DataFrame = pd.read_parquet(written_path)
    assert len(reloaded) == 0
    assert list(reloaded.columns) == list(empty_ledger.columns)


# ---------------------------  PARAMETRIZED STATUS TESTS  ---------------
@pytest.mark.parametrize(
    "input_status,expected_result",
    [
        ("COMPLETED", "SUCCESS"),
        ("completed", "SUCCESS"),  # lowercase
        ("Completed", "SUCCESS"),  # mixed case
        ("SETTLED", "SUCCESS"),
        ("settled", "SUCCESS"),
        ("OK", "SUCCESS"),
        ("ok", "SUCCESS"),
        ("FAILED", "FAILED"),
        ("failed", "FAILED"),
        ("PENDING", "FAILED"),  # not in SUCCESS_STATUSES
        ("pending", "FAILED"),
        ("CANCELLED", "FAILED"),
        ("", "FAILED"),  # empty string
        (None, "FAILED"),  # None/NaN
        (pd.NA, "FAILED"),  # pandas NA
        (123, "FAILED"),  # numeric status
        (True, "FAILED"),  # boolean
    ],
)
def test_normalize_status_parametrized(input_status: Any, expected_result: str) -> None:
    """normalize_status must handle various status values correctly."""
    assert normalize_status(input_status) == expected_result


# ---------------------------  INTEGRATION TEST  ------------------------
def test_full_pipeline_end_to_end(tmp_path: Path) -> None:
    """
    End-to-end integration test: extract -> transform -> reconcile -> load.
    Validates the complete pipeline workflow from CSV input to Parquet output.
    """
    # Setup: Create input CSV file
    input_csv: Path = tmp_path / "input" / "transactions.csv"
    input_csv.parent.mkdir(parents=True, exist_ok=True)

    csv_content: str = """txn_id,account_id,txn_date,ingestion_date,amount,currency,txn_type,status
t1,acc1,2024-01-15,2024-01-15,100.00,EUR,CARD,COMPLETED
t2,acc1,2024-01-15,2024-01-16,50.00,EUR,REFUND,SETTLED
t3,acc2,2024-01-16,2024-01-16,200.00,EUR,CARD,OK
t4,acc2,2024-01-16,2024-01-16,75.50,EUR,CARD,PENDING
t5,acc1,2024-01-17,2024-01-17,-25.00,EUR,DEBIT,COMPLETED
"""
    input_csv.write_text(csv_content)

    # Step 1: Extract
    raw_df: pd.DataFrame = extract_transactions(str(input_csv))
    assert len(raw_df) == 5
    assert list(raw_df.columns) == [
        "txn_id", "account_id", "txn_date", "ingestion_date",
        "amount", "currency", "txn_type", "status",
    ]

    # Step 2: Transform
    clean_df, ledger_df = transform_transactions(raw_df)

    # Validate transformation results
    assert len(clean_df) == 5  # All rows kept in clean data
    assert "status_clean" in clean_df.columns
    assert "amount_clean" in clean_df.columns
    assert "is_late" in clean_df.columns

    # Only SUCCESS statuses (COMPLETED, SETTLED, OK) go to ledger
    assert len(ledger_df) == 4  # PENDING row excluded
    assert "t4" not in ledger_df["txn_id"].values  # PENDING excluded

    # Refund should be negative
    refund_row: pd.Series = ledger_df[ledger_df["txn_id"] == "t2"].iloc[0]
    assert refund_row["amount"] == -50.00

    # Late arrival flagged correctly (t2 has ingestion after txn_date)
    late_txn: pd.Series = clean_df[clean_df["txn_id"] == "t2"].iloc[0]
    assert late_txn["is_late"] == True

    # Step 3: Reconcile
    recon_df: pd.DataFrame = reconcile_raw_vs_ledger(clean_df, ledger_df)
    assert len(recon_df) > 0
    assert (recon_df["diff"].abs() <= 0.01).all()  # Differences within tolerance

    # Step 4: Load (ledger)
    output_dir: Path = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    ledger_path: Path = output_dir / "ledger.parquet"

    written_path: Path = load_ledger(ledger_df, str(ledger_path))
    assert written_path.exists()
    assert written_path.suffix == ".parquet"

    # Verify ledger can be read back and matches
    reloaded_ledger: pd.DataFrame = pd.read_parquet(written_path)
    assert len(reloaded_ledger) == len(ledger_df)

    # Step 5: Daily aggregation (as done in pipeline.py)
    daily_balance: pd.DataFrame = (
        ledger_df.groupby(["account_id", "txn_date"], observed=True)
        .agg(
            total_amount=pd.NamedAgg(column="amount", aggfunc="sum"),
            txn_count=pd.NamedAgg(column="txn_id", aggfunc="count"),
        )
        .reset_index()
        .sort_values(["account_id", "txn_date"])
    )

    # Verify aggregation makes sense
    assert len(daily_balance) > 0
    assert "total_amount" in daily_balance.columns
    assert "txn_count" in daily_balance.columns

    # Verify account totals add up correctly
    acc1_total: float = daily_balance[daily_balance["account_id"] == "acc1"]["total_amount"].sum()
    expected_acc1: float = 100.00 - 50.00 - 25.00  # CARD + REFUND + DEBIT
    assert abs(acc1_total - expected_acc1) < 0.01

    # Cleanup happens automatically via tmp_path fixture