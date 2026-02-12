"""SLA (Service Level Agreement) monitoring for data freshness.

Tracks data freshness, processing latency, and SLA compliance.
Generates alerts when SLAs are violated.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

from pydantic import BaseModel, Field

LOGGER = logging.getLogger(__name__)


class SLAConfig(BaseModel):
    """SLA configuration for data freshness."""

    max_data_age_hours: float = Field(default=24.0, ge=0)
    max_processing_time_minutes: float = Field(default=30.0, ge=0)
    freshness_check_interval_hours: float = Field(default=1.0, ge=0)
    alert_on_sla_violation: bool = Field(default=True)


@dataclass
class SLAMetric:
    """Single SLA metric measurement."""

    metric_name: str
    timestamp: str
    value: float
    threshold: float
    unit: str
    status: str  # 'compliant', 'warning', 'violated'
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FreshnessReport:
    """Data freshness report."""

    report_timestamp: str
    table_name: str
    last_updated: Optional[str]
    data_age_hours: Optional[float]
    max_allowed_age_hours: float
    status: str  # 'fresh', 'stale', 'unknown'
    violations: List[str] = field(default_factory=list)


class SLAMonitor:
    """Monitors data freshness and SLA compliance."""

    def __init__(
        self,
        config: SLAConfig,
        output_dir: Optional[Path] = None,
    ):
        """Initialize SLA monitor.

        Args:
            config: SLA configuration
            output_dir: Directory for SLA reports
        """
        self.config = config
        self.output_dir = output_dir or Path("data/sla")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.metrics: List[SLAMetric] = []

    def check_data_freshness(
        self,
        table_name: str,
        last_updated: Optional[datetime],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> FreshnessReport:
        """Check if data meets freshness SLA.

        Args:
            table_name: Name of the table/file being checked
            last_updated: Last modification timestamp
            metadata: Additional metadata

        Returns:
            FreshnessReport with status and violations
        """
        now = datetime.utcnow()
        violations = []

        if last_updated is None:
            status = "unknown"
            data_age_hours = None
            violations.append("No timestamp available for freshness check")
        else:
            data_age_hours = (now - last_updated).total_seconds() / 3600

            if data_age_hours > self.config.max_data_age_hours:
                status = "stale"
                violations.append(
                    f"Data is {data_age_hours:.1f} hours old "
                    f"(max allowed: {self.config.max_data_age_hours} hours)"
                )
            elif data_age_hours > self.config.max_data_age_hours * 0.8:
                status = "warning"
            else:
                status = "fresh"

        report = FreshnessReport(
            report_timestamp=now.isoformat(),
            table_name=table_name,
            last_updated=last_updated.isoformat() if last_updated else None,
            data_age_hours=data_age_hours,
            max_allowed_age_hours=self.config.max_data_age_hours,
            status=status,
            violations=violations,
        )

        # Record metric
        metric = SLAMetric(
            metric_name="data_freshness",
            timestamp=now.isoformat(),
            value=data_age_hours if data_age_hours else -1,
            threshold=self.config.max_data_age_hours,
            unit="hours",
            status=status,
            details={"table": table_name, "violations": violations},
        )
        self.metrics.append(metric)

        if status == "stale":
            LOGGER.warning(
                "SLA violation: %s data is stale (%.1f hours old)",
                table_name, data_age_hours
            )
        elif status == "fresh":
            LOGGER.info("SLA compliant: %s data is fresh (%.1f hours old)",
                       table_name, data_age_hours)

        return report

    def check_processing_latency(
        self,
        process_name: str,
        start_time: datetime,
        end_time: datetime,
    ) -> SLAMetric:
        """Check if processing meets latency SLA.

        Args:
            process_name: Name of the process
            start_time: Process start time
            end_time: Process end time

        Returns:
            SLAMetric with latency status
        """
        duration_minutes = (end_time - start_time).total_seconds() / 60
        threshold = self.config.max_processing_time_minutes

        if duration_minutes > threshold:
            status = "violated"
            LOGGER.warning(
                "Processing SLA violated: %s took %.1f minutes (max: %.1f)",
                process_name, duration_minutes, threshold
            )
        elif duration_minutes > threshold * 0.8:
            status = "warning"
        else:
            status = "compliant"

        metric = SLAMetric(
            metric_name="processing_latency",
            timestamp=datetime.utcnow().isoformat(),
            value=duration_minutes,
            threshold=threshold,
            unit="minutes",
            status=status,
            details={"process": process_name},
        )
        self.metrics.append(metric)
        return metric

    def generate_sla_report(self) -> Path:
        """Generate comprehensive SLA report.

        Returns:
            Path to generated report
        """
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        report_path = self.output_dir / f"sla_report_{timestamp}.json"

        violations = [m for m in self.metrics if m.status == "violated"]
        warnings = [m for m in self.metrics if m.status == "warning"]
        compliant = [m for m in self.metrics if m.status == "compliant"]

        report = {
            "report_timestamp": datetime.utcnow().isoformat(),
            "summary": {
                "total_metrics": len(self.metrics),
                "compliant": len(compliant),
                "warnings": len(warnings),
                "violations": len(violations),
                "compliance_rate": len(compliant) / len(self.metrics) if self.metrics else 0,
            },
            "configuration": self.config.model_dump(),
            "metrics": [asdict(m) for m in self.metrics],
            "violations": [asdict(m) for m in violations],
        }

        with open(report_path, "w") as f:
            json.dump(report, f, indent=2, default=str)

        LOGGER.info("SLA report generated: %s", report_path)
        return report_path

    def get_sla_summary(self) -> Dict[str, Any]:
        """Get quick SLA summary for monitoring dashboards."""
        # Violations can be "violated" (processing) or "stale" (freshness)
        violation_statuses = {"violated", "stale"}
        violations = [m for m in self.metrics if m.status in violation_statuses]
        warnings = [m for m in self.metrics if m.status == "warning"]

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "total_checks": len(self.metrics),
            "status": "healthy" if not violations else "degraded",
            "violation_count": len(violations),
            "warning_count": len(warnings),
            "recent_violations": [
                {"metric": m.metric_name, "value": m.value, "threshold": m.threshold}
                for m in violations[-5:]  # Last 5 violations
            ],
        }


# Global SLA monitor instance
_sla_monitor: Optional[SLAMonitor] = None


def init_sla_monitor(
    config: SLAConfig,
    output_dir: Optional[Path] = None,
) -> SLAMonitor:
    """Initialize global SLA monitor."""
    global _sla_monitor
    _sla_monitor = SLAMonitor(config, output_dir)
    return _sla_monitor


def get_sla_monitor() -> Optional[SLAMonitor]:
    """Get global SLA monitor instance."""
    return _sla_monitor
