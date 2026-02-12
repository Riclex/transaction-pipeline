"""Tests for the data trust layer components.

Covers:
- Lineage tracking
- SLA monitoring
- Schema drift detection
- Alerting system
- Data contract enforcement
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.lineage import LineageTracker, DataLineageNode
from src.sla_monitor import SLAMonitor, SLAConfig, FreshnessReport
from src.schema_drift import SchemaDriftDetector, SchemaDriftConfig
from src.alerting import AlertManager, AlertConfig, AlertSeverity, AlertChannel
from src.data_contract import (
    DataContractEnforcer,
    DataContract,
    ColumnContract,
    ContractStatus,
)


# -----------------------------------------------------------------------------
# Lineage Tests
# -----------------------------------------------------------------------------

class TestLineageTracker:
    """Test data lineage tracking."""

    def test_register_source(self, tmp_path: Path) -> None:
        """Test registering a data source."""
        df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        csv_path = tmp_path / "test.csv"
        df.to_csv(csv_path, index=False)

        tracker = LineageTracker(tmp_path / "lineage")
        node_id = tracker.register_source("test_source", csv_path, df)

        assert node_id in tracker.nodes
        node = tracker.nodes[node_id]
        assert node.node_type == "source"
        assert node.row_count == 2
        assert node.file_hash is not None

    def test_register_transformation(self, tmp_path: Path) -> None:
        """Test registering a transformation."""
        input_df = pd.DataFrame({"col1": [1, 2, 3]})
        output_df = pd.DataFrame({"col1": [1, 2]})

        # Create source file
        source_path = tmp_path / "source.csv"
        input_df.to_csv(source_path, index=False)

        tracker = LineageTracker(tmp_path / "lineage")
        source_id = tracker.register_source("source", source_path, input_df)

        transform_id = tracker.register_transformation(
            "filter", "filter_rows", input_df, output_df, source_id
        )

        assert transform_id in tracker.nodes
        node = tracker.nodes[transform_id]
        assert node.operation == "filter_rows"
        assert node.parent_nodes == [source_id]

    def test_get_lineage_chain(self, tmp_path: Path) -> None:
        """Test retrieving lineage chain."""
        df = pd.DataFrame({"col1": [1, 2, 3]})

        # Create source file
        source_path = tmp_path / "source.csv"
        df.to_csv(source_path, index=False)

        tracker = LineageTracker(tmp_path / "lineage")
        source_id = tracker.register_source("source", source_path, df)
        transform_id = tracker.register_transformation(
            "transform", "op", df, df, source_id
        )
        sink_id = tracker.register_sink("sink", tmp_path / "sink.parquet", df, transform_id)

        chain = tracker.get_lineage_for_node(sink_id)

        assert len(chain) == 3
        assert chain[0].node_type == "source"
        assert chain[1].node_type == "transformation"
        assert chain[2].node_type == "sink"

    def test_export_lineage(self, tmp_path: Path) -> None:
        """Test exporting lineage to JSON."""
        df = pd.DataFrame({"col1": [1, 2]})

        # Create source file
        source_path = tmp_path / "source.csv"
        df.to_csv(source_path, index=False)

        tracker = LineageTracker(tmp_path / "lineage")
        tracker.register_source("source", source_path, df)

        output_path = tracker.export_lineage()

        assert output_path.exists()
        assert output_path.suffix == ".json"


# -----------------------------------------------------------------------------
# SLA Monitor Tests
# -----------------------------------------------------------------------------

class TestSLAMonitor:
    """Test SLA monitoring."""

    def test_check_freshness_fresh(self, tmp_path: Path) -> None:
        """Test freshness check for fresh data."""
        config = SLAConfig(max_data_age_hours=24)
        monitor = SLAMonitor(config, tmp_path)

        # Data updated 1 hour ago
        last_updated = datetime.utcnow() - timedelta(hours=1)
        report = monitor.check_data_freshness("test_table", last_updated)

        assert report.status == "fresh"
        assert len(report.violations) == 0

    def test_check_freshness_stale(self, tmp_path: Path) -> None:
        """Test freshness check for stale data."""
        config = SLAConfig(max_data_age_hours=24)
        monitor = SLAMonitor(config, tmp_path)

        # Data updated 48 hours ago
        last_updated = datetime.utcnow() - timedelta(hours=48)
        report = monitor.check_data_freshness("test_table", last_updated)

        assert report.status == "stale"
        assert len(report.violations) > 0

    def test_check_processing_latency(self, tmp_path: Path) -> None:
        """Test processing latency check."""
        config = SLAConfig(max_processing_time_minutes=30)
        monitor = SLAMonitor(config, tmp_path)

        start = datetime.utcnow() - timedelta(minutes=10)
        end = datetime.utcnow()

        metric = monitor.check_processing_latency("test_process", start, end)

        assert metric.status == "compliant"
        assert abs(metric.value - 10.0) < 0.1  # Approximate due to timing

    def test_sla_summary(self, tmp_path: Path) -> None:
        """Test SLA summary generation."""
        config = SLAConfig(max_data_age_hours=24)
        monitor = SLAMonitor(config, tmp_path)

        # Add some metrics
        monitor.check_data_freshness(
            "table1", datetime.utcnow() - timedelta(hours=1)
        )
        monitor.check_data_freshness(
            "table2", datetime.utcnow() - timedelta(hours=48)
        )

        summary = monitor.get_sla_summary()

        assert summary["total_checks"] == 2
        assert summary["violation_count"] == 1


# -----------------------------------------------------------------------------
# Schema Drift Tests
# -----------------------------------------------------------------------------

class TestSchemaDriftDetector:
    """Test schema drift detection."""

    def test_capture_schema(self, tmp_path: Path) -> None:
        """Test schema capture."""
        df = pd.DataFrame({
            "int_col": [1, 2],
            "str_col": ["a", "b"],
            "float_col": [1.0, 2.0],
        })

        detector = SchemaDriftDetector(SchemaDriftConfig(), tmp_path)
        version = detector.capture_schema(df, "test_source")

        assert version.column_count == 3
        assert "int_col" in version.columns
        assert version.hash is not None

    def test_no_drift_on_same_schema(self, tmp_path: Path) -> None:
        """Test no drift detected when schema unchanged."""
        df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})

        detector = SchemaDriftDetector(SchemaDriftConfig(), tmp_path)

        # First call creates baseline
        events, has_critical = detector.detect_drift(df, "test_source")
        assert len(events) == 0  # No events on initial baseline creation

        # Second call with same schema
        events, has_critical = detector.detect_drift(df, "test_source", auto_baseline=False)
        assert len(events) == 0
        assert not has_critical

    def test_detect_column_addition(self, tmp_path: Path) -> None:
        """Test detection of added columns."""
        df1 = pd.DataFrame({"col1": [1, 2]})
        df2 = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})

        config = SchemaDriftConfig(track_column_additions=True)
        detector = SchemaDriftDetector(config, tmp_path)

        # Create baseline
        detector.detect_drift(df1, "test_source")

        # Detect drift
        events, has_critical = detector.detect_drift(df2, "test_source", auto_baseline=False)

        added_events = [e for e in events if e.event_type == "column_added"]
        assert len(added_events) == 1
        assert added_events[0].column_name == "col2"

    def test_detect_column_removal(self, tmp_path: Path) -> None:
        """Test detection of removed columns."""
        df1 = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        df2 = pd.DataFrame({"col1": [1, 2]})

        config = SchemaDriftConfig(
            track_column_removals=True,
            critical_columns=["col2"],
        )
        detector = SchemaDriftDetector(config, tmp_path)

        # Create baseline
        detector.detect_drift(df1, "test_source")

        # Detect drift
        events, has_critical = detector.detect_drift(df2, "test_source", auto_baseline=False)

        removed_events = [e for e in events if e.event_type == "column_removed"]
        assert len(removed_events) == 1
        assert removed_events[0].severity == "critical"  # Because col2 is critical


# -----------------------------------------------------------------------------
# Alerting Tests
# -----------------------------------------------------------------------------

class TestAlertManager:
    """Test alerting system."""

    def test_send_console_alert(self, tmp_path: Path) -> None:
        """Test console alert sending."""
        config = AlertConfig(
            channels=[AlertChannel.CONSOLE],
            severity_filter=[AlertSeverity.WARNING, AlertSeverity.ERROR],
        )
        manager = AlertManager(config)

        alert = manager.send_alert(
            severity=AlertSeverity.WARNING,
            category="quality",
            title="Test Alert",
            message="Test message",
            source="test",
        )

        assert alert is not None
        assert alert.severity == AlertSeverity.WARNING

    def test_alert_severity_filtering(self, tmp_path: Path) -> None:
        """Test that alerts are filtered by severity."""
        config = AlertConfig(
            channels=[AlertChannel.CONSOLE],
            severity_filter=[AlertSeverity.ERROR],  # Only ERROR and above
        )
        manager = AlertManager(config)

        # INFO alert should be filtered
        alert = manager.send_alert(
            severity=AlertSeverity.INFO,
            category="test",
            title="Info Alert",
            message="Info message",
            source="test",
        )
        assert alert is None

        # ERROR alert should go through
        alert = manager.send_alert(
            severity=AlertSeverity.ERROR,
            category="test",
            title="Error Alert",
            message="Error message",
            source="test",
        )
        assert alert is not None

    def test_alert_throttling(self, tmp_path: Path) -> None:
        """Test alert throttling."""
        config = AlertConfig(
            channels=[AlertChannel.CONSOLE],
            throttle_minutes=60,  # 1 hour throttle
        )
        manager = AlertManager(config)

        # First alert
        alert1 = manager.send_alert(
            severity=AlertSeverity.WARNING,
            category="test",
            title="Same Alert",
            message="Message",
            source="test",
        )
        assert alert1 is not None

        # Second alert (same category/title) should be throttled
        alert2 = manager.send_alert(
            severity=AlertSeverity.WARNING,
            category="test",
            title="Same Alert",
            message="Message",
            source="test",
        )
        assert alert2 is None  # Throttled

    def test_acknowledge_alert(self, tmp_path: Path) -> None:
        """Test alert acknowledgment."""
        config = AlertConfig(channels=[AlertChannel.CONSOLE])
        manager = AlertManager(config)

        alert = manager.send_alert(
            severity=AlertSeverity.ERROR,
            category="test",
            title="Test Alert",
            message="Test",
            source="test",
        )

        # Acknowledge
        result = manager.acknowledge_alert(alert.alert_id, "test_user")
        assert result is True

        # Check alert is acknowledged
        active = manager.get_active_alerts()
        assert len(active) == 0

    def test_convenience_methods(self, tmp_path: Path) -> None:
        """Test convenience alert methods."""
        config = AlertConfig(channels=[AlertChannel.CONSOLE])
        manager = AlertManager(config)

        # Test quality alert
        alert = manager.alert_quality_issue(
            title="Quality Issue",
            message="Data quality problem",
            severity=AlertSeverity.ERROR,
        )
        assert alert is not None
        assert alert.category == "quality"

        # Test SLA alert
        alert = manager.alert_sla_violation(
            title="SLA Breach",
            message="SLA violated",
        )
        assert alert.category == "sla"

        # Test pipeline failure
        alert = manager.alert_pipeline_failure(
            title="Pipeline Failed",
            message="Critical failure",
        )
        assert alert.severity == AlertSeverity.CRITICAL


# -----------------------------------------------------------------------------
# Data Contract Tests
# -----------------------------------------------------------------------------

class TestDataContractEnforcer:
    """Test data contract enforcement."""

    def test_valid_contract(self, tmp_path: Path) -> None:
        """Test validation with compliant data."""
        df = pd.DataFrame({
            "txn_id": ["t1", "t2"],
            "amount": [100.0, 200.0],
            "status": ["SUCCESS", "FAILED"],
        })

        contract = DataContract(
            name="test_contract",
            producer="test_team",
            columns=[
                ColumnContract(name="txn_id", dtype="object", unique=True, nullable=False),
                ColumnContract(name="amount", dtype="float64", min_value=0),
                ColumnContract(name="status", dtype="object", allowed_values=["SUCCESS", "FAILED"]),
            ],
        )

        enforcer = DataContractEnforcer(tmp_path)
        enforcer.register_contract(contract)

        result = enforcer.validate(df, "test_contract")

        assert result.is_valid
        assert result.status == ContractStatus.VALID
        assert result.error_count == 0

    def test_null_violation(self, tmp_path: Path) -> None:
        """Test detection of null values in non-nullable columns."""
        df = pd.DataFrame({
            "txn_id": ["t1", None],  # Null not allowed
            "amount": [100.0, 200.0],
        })

        contract = DataContract(
            name="test_contract",
            producer="test_team",
            columns=[
                ColumnContract(name="txn_id", dtype="object", nullable=False),
                ColumnContract(name="amount", dtype="float64"),
            ],
        )

        enforcer = DataContractEnforcer(tmp_path)
        enforcer.register_contract(contract)

        result = enforcer.validate(df, "test_contract")

        assert not result.is_valid
        assert result.error_count > 0

    def test_unique_violation(self, tmp_path: Path) -> None:
        """Test detection of duplicate values in unique columns."""
        df = pd.DataFrame({
            "txn_id": ["t1", "t1"],  # Duplicate
            "amount": [100.0, 200.0],
        })

        contract = DataContract(
            name="test_contract",
            producer="test_team",
            columns=[
                ColumnContract(name="txn_id", dtype="object", unique=True),
            ],
        )

        enforcer = DataContractEnforcer(tmp_path)
        enforcer.register_contract(contract)

        result = enforcer.validate(df, "test_contract")

        assert not result.is_valid
        violations = [v for v in result.violations if v.constraint_type.value == "unique"]
        assert len(violations) > 0

    def test_range_violation(self, tmp_path: Path) -> None:
        """Test detection of values outside allowed range."""
        df = pd.DataFrame({
            "amount": [100.0, -50.0],  # Negative not allowed
        })

        contract = DataContract(
            name="test_contract",
            producer="test_team",
            columns=[
                ColumnContract(name="amount", dtype="float64", min_value=0),
            ],
        )

        enforcer = DataContractEnforcer(tmp_path)
        enforcer.register_contract(contract)

        result = enforcer.validate(df, "test_contract")

        assert not result.is_valid
        violations = [v for v in result.violations if v.constraint_type.value == "range"]
        assert len(violations) > 0

    def test_enum_violation(self, tmp_path: Path) -> None:
        """Test detection of invalid enum values."""
        df = pd.DataFrame({
            "status": ["SUCCESS", "INVALID_STATUS"],
        })

        contract = DataContract(
            name="test_contract",
            producer="test_team",
            columns=[
                ColumnContract(
                    name="status",
                    dtype="object",
                    allowed_values=["SUCCESS", "FAILED"],
                ),
            ],
        )

        enforcer = DataContractEnforcer(tmp_path)
        enforcer.register_contract(contract)

        result = enforcer.validate(df, "test_contract")

        assert not result.is_valid
        violations = [v for v in result.violations if v.constraint_type.value == "enum"]
        assert len(violations) > 0

    def test_missing_column(self, tmp_path: Path) -> None:
        """Test detection of missing required columns."""
        df = pd.DataFrame({
            "txn_id": ["t1", "t2"],
            # "amount" column is missing
        })

        contract = DataContract(
            name="test_contract",
            producer="test_team",
            columns=[
                ColumnContract(name="txn_id", dtype="object"),
                ColumnContract(name="amount", dtype="float64"),
            ],
        )

        enforcer = DataContractEnforcer(tmp_path)
        enforcer.register_contract(contract)

        result = enforcer.validate(df, "test_contract")

        assert not result.is_valid
        violations = [v for v in result.violations if v.column_name == "amount"]
        assert len(violations) > 0

    def test_save_and_load_contract(self, tmp_path: Path) -> None:
        """Test saving and loading contracts."""
        contract = DataContract(
            name="test_contract",
            producer="test_team",
            columns=[ColumnContract(name="col1", dtype="int64")],
        )

        enforcer = DataContractEnforcer(tmp_path)
        saved_path = enforcer.save_contract(contract)

        assert saved_path.exists()

        loaded = enforcer.load_contract("test_contract")
        assert loaded is not None
        assert loaded.name == "test_contract"
        assert len(loaded.columns) == 1
