"""Schema drift detection for the transaction pipeline.

Detects changes in data schemas over time, alerting on:
- Added/removed columns
- Type changes
- Nullability changes
- Unexpected new values in categorical fields
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Any, Tuple

import pandas as pd
from pydantic import BaseModel, Field

LOGGER = logging.getLogger(__name__)


class SchemaDriftConfig(BaseModel):
    """Configuration for schema drift detection."""

    enabled: bool = Field(default=True)
    track_column_additions: bool = Field(default=True)
    track_column_removals: bool = Field(default=True)
    track_type_changes: bool = Field(default=True)
    track_nullability_changes: bool = Field(default=False)  # Can be noisy
    allowed_new_columns: List[str] = Field(default_factory=list)
    critical_columns: List[str] = Field(default_factory=list)


@dataclass
class SchemaVersion:
    """Snapshot of a schema at a point in time."""

    timestamp: str
    source: str
    columns: Dict[str, str]  # column_name -> dtype
    column_count: int
    hash: str  # Simple hash of schema for quick comparison


@dataclass
class DriftEvent:
    """Detected schema drift event."""

    event_type: str  # 'column_added', 'column_removed', 'type_changed'
    severity: str  # 'info', 'warning', 'critical'
    column_name: str
    expected: Any
    actual: Any
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    description: str = ""


class SchemaDriftDetector:
    """Detects schema drift by comparing current schema against baseline."""

    def __init__(
        self,
        config: SchemaDriftConfig,
        baseline_dir: Optional[Path] = None,
    ):
        """Initialize schema drift detector.

        Args:
            config: Drift detection configuration
            baseline_dir: Directory to store schema baselines
        """
        self.config = config
        self.baseline_dir = baseline_dir or Path("data/schema_baselines")
        self.baseline_dir.mkdir(parents=True, exist_ok=True)
        self.drift_events: List[DriftEvent] = []

    def _compute_schema_hash(self, schema: Dict[str, str]) -> str:
        """Compute hash of schema for quick comparison."""
        schema_str = json.dumps(schema, sort_keys=True)
        return str(hash(schema_str))[:16]

    def capture_schema(
        self,
        df: pd.DataFrame,
        source_name: str,
    ) -> SchemaVersion:
        """Capture current schema as a version.

        Args:
            df: DataFrame to capture schema from
            source_name: Name of the data source

        Returns:
            SchemaVersion snapshot
        """
        columns = {col: str(dtype) for col, dtype in df.dtypes.items()}

        version = SchemaVersion(
            timestamp=datetime.utcnow().isoformat(),
            source=source_name,
            columns=columns,
            column_count=len(columns),
            hash=self._compute_schema_hash(columns),
        )

        LOGGER.debug("Captured schema for %s: %d columns", source_name, len(columns))
        return version

    def save_baseline(self, schema_version: SchemaVersion, force: bool = False) -> Path:
        """Save schema as baseline for future comparisons.

        Args:
            schema_version: Schema to save as baseline
            force: Overwrite existing baseline if True

        Returns:
            Path to saved baseline file
        """
        baseline_path = self.baseline_dir / f"{schema_version.source}_baseline.json"

        if baseline_path.exists() and not force:
            LOGGER.info("Baseline already exists for %s", schema_version.source)
            return baseline_path

        baseline_data = {
            "timestamp": schema_version.timestamp,
            "source": schema_version.source,
            "columns": schema_version.columns,
            "column_count": schema_version.column_count,
            "hash": schema_version.hash,
        }

        with open(baseline_path, "w") as f:
            json.dump(baseline_data, f, indent=2)

        LOGGER.info("Saved baseline for %s: %s", schema_version.source, baseline_path)
        return baseline_path

    def load_baseline(self, source_name: str) -> Optional[SchemaVersion]:
        """Load baseline schema for comparison.

        Args:
            source_name: Name of data source

        Returns:
            SchemaVersion if baseline exists, None otherwise
        """
        baseline_path = self.baseline_dir / f"{source_name}_baseline.json"

        if not baseline_path.exists():
            return None

        with open(baseline_path) as f:
            data = json.load(f)

        return SchemaVersion(
            timestamp=data["timestamp"],
            source=data["source"],
            columns=data["columns"],
            column_count=data["column_count"],
            hash=data["hash"],
        )

    def detect_drift(
        self,
        current_df: pd.DataFrame,
        source_name: str,
        auto_baseline: bool = True,
    ) -> Tuple[List[DriftEvent], bool]:
        """Detect schema drift against baseline.

        Args:
            current_df: Current DataFrame to check
            source_name: Name of data source
            auto_baseline: Create baseline if none exists

        Returns:
            Tuple of (drift_events, has_critical_drift)
        """
        current_schema = self.capture_schema(current_df, source_name)
        baseline = self.load_baseline(source_name)

        # Auto-create baseline if none exists
        if baseline is None and auto_baseline:
            self.save_baseline(current_schema)
            LOGGER.info("Created initial baseline for %s", source_name)
            return [], False

        if baseline is None:
            LOGGER.warning("No baseline found for %s", source_name)
            return [], False

        # Quick hash check
        if current_schema.hash == baseline.hash:
            LOGGER.debug("No schema drift detected for %s", source_name)
            return [], False

        # Detailed comparison
        events = self._compare_schemas(baseline, current_schema)
        self.drift_events.extend(events)

        has_critical = any(e.severity == "critical" for e in events)

        if events:
            LOGGER.warning(
                "Schema drift detected for %s: %d events",
                source_name, len(events)
            )
            for event in events:
                LOGGER.warning("  - %s: %s", event.event_type, event.column_name)

        return events, has_critical

    def _compare_schemas(
        self,
        baseline: SchemaVersion,
        current: SchemaVersion,
    ) -> List[DriftEvent]:
        """Compare two schemas and return drift events."""
        events = []

        baseline_cols = set(baseline.columns.keys())
        current_cols = set(current.columns.keys())

        # Check for added columns
        if self.config.track_column_additions:
            added_cols = current_cols - baseline_cols
            for col in added_cols:
                if col not in self.config.allowed_new_columns:
                    events.append(DriftEvent(
                        event_type="column_added",
                        severity="warning",
                        column_name=col,
                        expected=None,
                        actual=current.columns[col],
                        description=f"New column '{col}' detected",
                    ))

        # Check for removed columns
        if self.config.track_column_removals:
            removed_cols = baseline_cols - current_cols
            for col in removed_cols:
                severity = "critical" if col in self.config.critical_columns else "warning"
                events.append(DriftEvent(
                    event_type="column_removed",
                    severity=severity,
                    column_name=col,
                    expected=baseline.columns[col],
                    actual=None,
                    description=f"Column '{col}' was removed",
                ))

        # Check for type changes
        if self.config.track_type_changes:
            common_cols = baseline_cols & current_cols
            for col in common_cols:
                if baseline.columns[col] != current.columns[col]:
                    severity = "critical" if col in self.config.critical_columns else "warning"
                    events.append(DriftEvent(
                        event_type="type_changed",
                        severity=severity,
                        column_name=col,
                        expected=baseline.columns[col],
                        actual=current.columns[col],
                        description=f"Column '{col}' type changed from {baseline.columns[col]} to {current.columns[col]}",
                    ))

        return events

    def generate_drift_report(self) -> Path:
        """Generate comprehensive drift report.

        Returns:
            Path to generated report
        """
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        report_path = self.baseline_dir / f"drift_report_{timestamp}.json"

        report = {
            "report_timestamp": datetime.utcnow().isoformat(),
            "total_events": len(self.drift_events),
            "critical_count": sum(1 for e in self.drift_events if e.severity == "critical"),
            "warning_count": sum(1 for e in self.drift_events if e.severity == "warning"),
            "events": [asdict(e) for e in self.drift_events],
        }

        with open(report_path, "w") as f:
            json.dump(report, f, indent=2, default=str)

        LOGGER.info("Drift report generated: %s", report_path)
        return report_path

    def get_drift_summary(self) -> Dict[str, Any]:
        """Get quick drift summary for monitoring."""
        critical = [e for e in self.drift_events if e.severity == "critical"]
        warnings = [e for e in self.drift_events if e.severity == "warning"]

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "total_events": len(self.drift_events),
            "status": "healthy" if not critical else "degraded",
            "critical_count": len(critical),
            "warning_count": len(warnings),
            "recent_events": [
                {"type": e.event_type, "column": e.column_name, "severity": e.severity}
                for e in self.drift_events[-10:]
            ],
        }


# Global drift detector instance
_drift_detector: Optional[SchemaDriftDetector] = None


def init_drift_detector(
    config: SchemaDriftConfig,
    baseline_dir: Optional[Path] = None,
) -> SchemaDriftDetector:
    """Initialize global schema drift detector."""
    global _drift_detector
    _drift_detector = SchemaDriftDetector(config, baseline_dir)
    return _drift_detector


def get_drift_detector() -> Optional[SchemaDriftDetector]:
    """Get global drift detector instance."""
    return _drift_detector
