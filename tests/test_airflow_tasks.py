"""Tests for Airflow task functions.

These tests validate that the task functions in src/airflow_tasks.py
work correctly with mocked Airflow context.
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Ensure src is on path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.airflow_tasks import (
    alert_reconcile_failure_task,
    branch_on_reconcile,
    cleanup_intermediate_task,
    extract_task,
    load_task,
    transform_task,
)


class MockTaskInstance:
    """Mock Airflow TaskInstance for testing."""

    def __init__(self) -> None:
        self.xcom_data: dict[str, Any] = {}

    def xcom_push(self, key: str, value: Any) -> None:
        """Mock XCom push."""
        self.xcom_data[key] = value

    def xcom_pull(self, task_ids: str, key: str, default: Any = None) -> Any:
        """Mock XCom pull."""
        # For simplicity, return from internal storage
        # In real Airflow, this would query the metadata DB
        return self.xcom_data.get(key, default)


@pytest.fixture
def mock_ti() -> MockTaskInstance:
    """Fixture for mock TaskInstance."""
    return MockTaskInstance()


@pytest.fixture
def mock_context(mock_ti: MockTaskInstance) -> dict[str, Any]:
    """Fixture for mock Airflow context."""
    return {
        "ti": mock_ti,
        "run_id": "test_run_20240101",
        "ts": "2024-01-01T06:00:00+00:00",
    }


@pytest.fixture
def sample_raw_df() -> pd.DataFrame:
    """Sample raw transaction data."""
    return pd.DataFrame({
        "txn_id": ["txn-001", "txn-002", "txn-003"],
        "account_id": ["acc-001", "acc-001", "acc-002"],
        "txn_date": [date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 2)],
        "ingestion_date": [datetime(2024, 1, 1), datetime(2024, 1, 1), datetime(2024, 1, 2)],
        "amount": [100.0, -50.0, 200.0],
        "currency": ["USD", "USD", "USD"],
        "txn_type": ["CARD", "REFUND", "DEBIT"],
        "status": ["COMPLETED", "COMPLETED", "SETTLED"],
    })


class TestExtractTask:
    """Tests for extract_task function."""

    @patch("src.airflow_tasks.extract_transactions")
    @patch("src.airflow_tasks._get_config")
    @patch("pathlib.Path.exists")
    def test_extract_success(
        self,
        mock_exists: MagicMock,
        mock_get_config: MagicMock,
        mock_extract: MagicMock,
        mock_context: dict[str, Any],
        sample_raw_df: pd.DataFrame,
    ) -> None:
        """Test successful extraction."""
        # Setup
        mock_exists.return_value = True
        mock_extract.return_value = sample_raw_df

        mock_config = MagicMock()
        mock_config.paths.raw_transactions = "data/raw/test.csv"
        mock_config.processing.chunksize = None
        mock_config.business_rules.valid_txn_types = ["CARD", "DEBIT", "REFUND"]
        mock_get_config.return_value = mock_config

        # Execute
        result = extract_task(mock_context["ti"], **mock_context)

        # Verify
        assert isinstance(result, str)
        assert "raw_extracted_" in result
        assert mock_context["ti"].xcom_data["row_count"] == 3
        assert mock_context["ti"].xcom_data["extract_status"] == "success"

    @patch("src.airflow_tasks._get_config")
    @patch("pathlib.Path.exists")
    def test_extract_file_not_found(
        self,
        mock_exists: MagicMock,
        mock_get_config: MagicMock,
        mock_context: dict[str, Any],
    ) -> None:
        """Test extraction with missing file."""
        # Setup
        mock_exists.return_value = False

        mock_config = MagicMock()
        mock_config.paths.raw_transactions = "data/raw/nonexistent.csv"
        mock_get_config.return_value = mock_config

        # Execute and verify
        with pytest.raises(Exception) as exc_info:
            extract_task(mock_context["ti"], **mock_context)

        assert "not found" in str(exc_info.value) or "AirflowFailException" in str(exc_info.value)


class TestTransformTask:
    """Tests for transform_task function."""

    @patch("src.airflow_tasks._get_config")
    @patch("src.airflow_tasks.pd.read_parquet")
    @patch("src.airflow_tasks.transform_transactions")
    def test_transform_success(
        self,
        mock_transform: MagicMock,
        mock_read_parquet: MagicMock,
        mock_get_config: MagicMock,
        mock_context: dict[str, Any],
        sample_raw_df: pd.DataFrame,
    ) -> None:
        """Test successful transformation."""
        # Setup
        mock_read_parquet.return_value = sample_raw_df

        transformed_df = sample_raw_df.copy()
        transformed_df["status"] = "SUCCESS"
        ledger_df = transformed_df[transformed_df["status"] == "SUCCESS"]

        mock_transform.return_value = (transformed_df, ledger_df)

        mock_config = MagicMock()
        mock_config.business_rules.success_statuses = ["COMPLETED", "SETTLED", "OK"]
        mock_get_config.return_value = mock_config

        # Pre-populate XCom with raw_path
        mock_context["ti"].xcom_data["raw_path"] = "/tmp/test_raw.parquet"
        mock_context["ti"].xcom_data["run_id"] = "test_run"

        # Execute
        result = transform_task(mock_context["ti"], **mock_context)

        # Verify
        assert isinstance(result, dict)
        assert "transformed_path" in result
        assert "ledger_path" in result
        assert mock_context["ti"].xcom_data["transformed_count"] == len(transformed_df)
        assert mock_context["ti"].xcom_data["ledger_count"] == len(ledger_df)

    def test_transform_missing_raw_path(self, mock_context: dict[str, Any]) -> None:
        """Test transformation with missing upstream data."""
        from airflow.exceptions import AirflowSkipException

        # Execute and verify
        with pytest.raises(AirflowSkipException) as exc_info:
            transform_task(mock_context["ti"], **mock_context)

        assert "No raw data" in str(exc_info.value) or "Missing" in str(exc_info.value)


class TestBranchOnReconcile:
    """Tests for branch_on_reconcile function."""

    def test_branch_success(self, mock_context: dict[str, Any]) -> None:
        """Test branching when reconciliation passes."""
        # Setup
        mock_context["ti"].xcom_data["reconcile_passed"] = True

        # Execute
        result = branch_on_reconcile(mock_context["ti"], **mock_context)

        # Verify
        assert result == "load"

    def test_branch_failure(self, mock_context: dict[str, Any]) -> None:
        """Test branching when reconciliation fails."""
        # Setup
        mock_context["ti"].xcom_data["reconcile_passed"] = False

        # Execute
        result = branch_on_reconcile(mock_context["ti"], **mock_context)

        # Verify
        assert result == "alert_reconcile_failure"

    def test_branch_no_data(self, mock_context: dict[str, Any]) -> None:
        """Test branching with no reconcile data (should fail to alert)."""
        # Execute
        result = branch_on_reconcile(mock_context["ti"], **mock_context)

        # Verify - should return alert task when no data
        assert result == "alert_reconcile_failure"


class TestCleanupTask:
    """Tests for cleanup_intermediate_task function."""

    @patch("src.airflow_tasks.INTERMEDIATE_DIR")
    def test_cleanup_success(self, mock_intermediate_dir: MagicMock, mock_context: dict[str, Any]) -> None:
        """Test successful cleanup."""
        # Setup
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            mock_intermediate_dir.__str__ = MagicMock(return_value=tmpdir)
            mock_intermediate_dir.exists.return_value = True
            mock_intermediate_dir.glob.return_value = []

            # Execute
            result = cleanup_intermediate_task(mock_context["ti"], **mock_context)

            # Verify
            assert isinstance(result, dict)
            assert "deleted_count" in result
            assert "errors" in result

    @patch("src.airflow_tasks.INTERMEDIATE_DIR")
    def test_cleanup_nonexistent_dir(self, mock_intermediate_dir: MagicMock, mock_context: dict[str, Any]) -> None:
        """Test cleanup with non-existent directory."""
        # Setup
        mock_intermediate_dir.exists.return_value = False

        # Execute
        result = cleanup_intermediate_task(mock_context["ti"], **mock_context)

        # Verify
        assert result["deleted_count"] == 0
        assert result["errors"] == 0


class TestLoadTask:
    """Tests for load_task function."""

    @patch("src.airflow_tasks._get_config")
    @patch("src.airflow_tasks.pd.read_parquet")
    @patch("src.airflow_tasks.load_ledger")
    def test_load_success(
        self,
        mock_load_ledger: MagicMock,
        mock_read_parquet: MagicMock,
        mock_get_config: MagicMock,
        mock_context: dict[str, Any],
        sample_raw_df: pd.DataFrame,
    ) -> None:
        """Test successful load."""
        # Setup
        mock_read_parquet.return_value = sample_raw_df
        mock_load_ledger.return_value = "data/processed/ledger.parquet"

        mock_config = MagicMock()
        mock_config.paths.ledger_output = "data/processed/ledger.parquet"
        mock_get_config.return_value = mock_config

        # Pre-populate XCom with ledger_path
        mock_context["ti"].xcom_data["ledger_path"] = "/tmp/test_ledger.parquet"

        # Execute
        result = load_task(mock_context["ti"], **mock_context)

        # Verify
        assert isinstance(result, dict)
        assert result["output_path"] == "data/processed/ledger.parquet"
        assert result["row_count"] == len(sample_raw_df)
        assert "duration_seconds" in result

    def test_load_missing_ledger_path(self, mock_context: dict[str, Any]) -> None:
        """Test load with missing ledger path."""
        from airflow.exceptions import AirflowSkipException

        # Execute and verify
        with pytest.raises(AirflowSkipException):
            load_task(mock_context["ti"], **mock_context)


class TestAlertReconcileFailure:
    """Tests for alert_reconcile_failure_task function."""

    def test_alert_raises_exception(self, mock_context: dict[str, Any]) -> None:
        """Test that alert task raises AirflowFailException."""
        from airflow.exceptions import AirflowFailException

        # Setup
        mock_context["ti"].xcom_data["mismatch_count"] = 5
        mock_context["ti"].xcom_data["tolerance"] = 0.01

        # Execute and verify
        with pytest.raises(AirflowFailException) as exc_info:
            alert_reconcile_failure_task(mock_context["ti"], **mock_context)

        assert "Reconciliation failed" in str(exc_info.value)


class TestXComDataTypes:
    """Tests for XCom data type handling."""

    def test_numeric_values_preserved(self, mock_ti: MockTaskInstance) -> None:
        """Test that numeric values are preserved in XCom."""
        # Push various numeric types
        mock_ti.xcom_push("int_value", 42)
        mock_ti.xcom_push("float_value", 3.14)
        mock_ti.xcom_push("bool_value", True)

        # Verify types preserved
        assert mock_ti.xcom_data["int_value"] == 42
        assert mock_ti.xcom_data["float_value"] == 3.14
        assert mock_ti.xcom_data["bool_value"] is True

    def test_string_values(self, mock_ti: MockTaskInstance) -> None:
        """Test string XCom values."""
        mock_ti.xcom_push("path", "/tmp/test.parquet")
        mock_ti.xcom_push("status", "success")

        assert mock_ti.xcom_data["path"] == "/tmp/test.parquet"
        assert mock_ti.xcom_data["status"] == "success"