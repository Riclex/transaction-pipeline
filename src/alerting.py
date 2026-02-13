"""Automated alerting for data quality issues and pipeline events.

Supports multiple notification channels:
- Console/Logging (default)
- Webhook (for Slack, PagerDuty, etc.)
- Email (SMTP)
- File-based (for downstream processing)
"""

from __future__ import annotations

import json
import logging
import smtplib
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from email.mime.text import MIMEText
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable
from urllib import request, parse

from pydantic import BaseModel, Field

LOGGER = logging.getLogger(__name__)


class AlertSeverity(str, Enum):
    """Alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertChannel(str, Enum):
    """Supported alert channels."""

    CONSOLE = "console"
    WEBHOOK = "webhook"
    EMAIL = "email"
    FILE = "file"


class AlertConfig(BaseModel):
    """Configuration for alerting system."""

    enabled: bool = Field(default=True)
    channels: List[AlertChannel] = Field(default=[AlertChannel.CONSOLE])
    webhook_url: Optional[str] = None
    email_smtp_host: Optional[str] = None
    email_smtp_port: int = 587
    email_username: Optional[str] = None
    email_password: Optional[str] = None
    email_from: Optional[str] = None
    email_to: List[str] = Field(default_factory=list)
    alert_file_path: Optional[Path] = None
    throttle_minutes: int = 5  # Don't send same alert within this window
    severity_filter: List[AlertSeverity] = Field(
        default=[AlertSeverity.WARNING, AlertSeverity.ERROR, AlertSeverity.CRITICAL]
    )


@dataclass
class Alert:
    """Single alert instance."""

    alert_id: str
    timestamp: str
    severity: AlertSeverity
    category: str  # 'quality', 'sla', 'schema', 'pipeline'
    title: str
    message: str
    source: str
    details: Dict[str, Any] = field(default_factory=dict)
    acknowledged: bool = False
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[str] = None


class AlertManager:
    """Manages data quality alerts across multiple channels."""

    def __init__(self, config: AlertConfig):
        """Initialize alert manager.

        Args:
            config: Alert configuration
        """
        self.config = config
        self.alert_history: List[Alert] = []
        self.throttle_cache: Dict[str, datetime] = {}
        self._handlers: Dict[AlertChannel, Callable[[Alert], None]] = {
            AlertChannel.CONSOLE: self._send_console_alert,
            AlertChannel.WEBHOOK: self._send_webhook_alert,
            AlertChannel.EMAIL: self._send_email_alert,
            AlertChannel.FILE: self._send_file_alert,
        }

    def _generate_alert_id(self, category: str, title: str) -> str:
        """Generate unique alert ID."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M")
        return f"{category}_{title.replace(' ', '_')}_{timestamp}"

    def _is_throttled(self, alert_key: str) -> bool:
        """Check if alert is throttled (recently sent)."""
        if alert_key not in self.throttle_cache:
            return False

        last_sent = self.throttle_cache[alert_key]
        elapsed = (datetime.now(timezone.utc) - last_sent).total_seconds() / 60
        return elapsed < self.config.throttle_minutes

    def send_alert(
        self,
        severity: AlertSeverity,
        category: str,
        title: str,
        message: str,
        source: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> Optional[Alert]:
        """Send an alert through configured channels.

        Args:
            severity: Alert severity
            category: Alert category
            title: Alert title
            message: Alert message
            source: Source component
            details: Additional details

        Returns:
            Alert if sent, None if throttled or filtered
        """
        if not self.config.enabled:
            return None

        # Check severity filter
        if severity not in self.config.severity_filter:
            LOGGER.debug("Alert filtered by severity: %s", title)
            return None

        # Check throttling
        alert_key = f"{category}:{title}"
        if self._is_throttled(alert_key):
            LOGGER.debug("Alert throttled: %s", title)
            return None

        alert = Alert(
            alert_id=self._generate_alert_id(category, title),
            timestamp=datetime.now(timezone.utc).isoformat(),
            severity=severity,
            category=category,
            title=title,
            message=message,
            source=source,
            details=details or {},
        )

        # Send through configured channels
        for channel in self.config.channels:
            if channel in self._handlers:
                try:
                    self._handlers[channel](alert)
                except Exception as e:
                    LOGGER.error("Failed to send alert via %s: %s", channel, e)

        # Update cache and history
        self.throttle_cache[alert_key] = datetime.now(timezone.utc)
        self.alert_history.append(alert)

        LOGGER.info("Alert sent [%s]: %s", severity.value, title)
        return alert

    def _send_console_alert(self, alert: Alert) -> None:
        """Send alert to console/log."""
        log_func = {
            AlertSeverity.INFO: LOGGER.info,
            AlertSeverity.WARNING: LOGGER.warning,
            AlertSeverity.ERROR: LOGGER.error,
            AlertSeverity.CRITICAL: LOGGER.critical,
        }.get(alert.severity, LOGGER.info)

        log_func(
            "ALERT [%s] %s: %s - %s (from %s)",
            alert.severity.value.upper(),
            alert.category,
            alert.title,
            alert.message,
            alert.source,
        )

    def _send_webhook_alert(self, alert: Alert) -> None:
        """Send alert via webhook (e.g., Slack, PagerDuty)."""
        if not self.config.webhook_url:
            return

        payload = {
            "text": f"*{alert.severity.value.upper()}* - {alert.title}",
            "alert": asdict(alert),
        }

        data = json.dumps(payload).encode("utf-8")
        req = request.Request(
            self.config.webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        with request.urlopen(req, timeout=10) as response:
            if response.status >= 400:
                raise Exception(f"Webhook returned {response.status}")

    def _send_email_alert(self, alert: Alert) -> None:
        """Send alert via email."""
        if not all([
            self.config.email_smtp_host,
            self.config.email_username,
            self.config.email_to,
        ]):
            return

        msg = MIMEText(
            f"Alert: {alert.title}\n\n"
            f"Severity: {alert.severity.value}\n"
            f"Category: {alert.category}\n"
            f"Source: {alert.source}\n"
            f"Time: {alert.timestamp}\n\n"
            f"Message:\n{alert.message}\n\n"
            f"Details:\n{json.dumps(alert.details, indent=2)}"
        )

        msg["Subject"] = f"[Data Pipeline] {alert.severity.value.upper()}: {alert.title}"
        msg["From"] = self.config.email_from or self.config.email_username
        msg["To"] = ", ".join(self.config.email_to)

        with smtplib.SMTP(self.config.email_smtp_host, self.config.email_smtp_port) as server:
            server.starttls()
            server.login(self.config.email_username, self.config.email_password or "")
            server.send_message(msg)

    def _send_file_alert(self, alert: Alert) -> None:
        """Write alert to file for downstream processing."""
        if not self.config.alert_file_path:
            return

        alert_line = json.dumps(asdict(alert), default=str) + "\n"

        with open(self.config.alert_file_path, "a") as f:
            f.write(alert_line)

    def acknowledge_alert(self, alert_id: str, acknowledged_by: str) -> bool:
        """Acknowledge an alert.

        Args:
            alert_id: Alert to acknowledge
            acknowledged_by: Person acknowledging

        Returns:
            True if found and acknowledged
        """
        for alert in self.alert_history:
            if alert.alert_id == alert_id and not alert.acknowledged:
                alert.acknowledged = True
                alert.acknowledged_by = acknowledged_by
                alert.acknowledged_at = datetime.now(timezone.utc).isoformat()
                LOGGER.info("Alert %s acknowledged by %s", alert_id, acknowledged_by)
                return True
        return False

    def get_active_alerts(self, severity: Optional[AlertSeverity] = None) -> List[Alert]:
        """Get active (non-acknowledged) alerts.

        Args:
            severity: Filter by severity (optional)

        Returns:
            List of active alerts
        """
        alerts = [a for a in self.alert_history if not a.acknowledged]
        if severity:
            alerts = [a for a in alerts if a.severity == severity]
        return alerts

    def export_alerts(self, filepath: Path) -> None:
        """Export all alerts to JSON file.

        Args:
            filepath: Output file path
        """
        data = {
            "export_timestamp": datetime.now(timezone.utc).isoformat(),
            "total_alerts": len(self.alert_history),
            "active_alerts": len(self.get_active_alerts()),
            "alerts": [asdict(a) for a in self.alert_history],
        }

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2, default=str)

        LOGGER.info("Exported %d alerts to %s", len(self.alert_history), filepath)

    # Convenience methods for common alert types

    def alert_quality_issue(
        self,
        title: str,
        message: str,
        severity: AlertSeverity = AlertSeverity.WARNING,
        details: Optional[Dict[str, Any]] = None,
    ) -> Optional[Alert]:
        """Send data quality alert."""
        return self.send_alert(
            severity=severity,
            category="quality",
            title=title,
            message=message,
            source="data_quality",
            details=details,
        )

    def alert_sla_violation(
        self,
        title: str,
        message: str,
        severity: AlertSeverity = AlertSeverity.ERROR,
        details: Optional[Dict[str, Any]] = None,
    ) -> Optional[Alert]:
        """Send SLA violation alert."""
        return self.send_alert(
            severity=severity,
            category="sla",
            title=title,
            message=message,
            source="sla_monitor",
            details=details,
        )

    def alert_schema_drift(
        self,
        title: str,
        message: str,
        severity: AlertSeverity = AlertSeverity.WARNING,
        details: Optional[Dict[str, Any]] = None,
    ) -> Optional[Alert]:
        """Send schema drift alert."""
        return self.send_alert(
            severity=severity,
            category="schema",
            title=title,
            message=message,
            source="schema_drift",
            details=details,
        )

    def alert_pipeline_failure(
        self,
        title: str,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> Optional[Alert]:
        """Send pipeline failure alert."""
        return self.send_alert(
            severity=AlertSeverity.CRITICAL,
            category="pipeline",
            title=title,
            message=message,
            source="pipeline",
            details=details,
        )


# Global alert manager instance
_alert_manager: Optional[AlertManager] = None


def init_alert_manager(config: AlertConfig) -> AlertManager:
    """Initialize global alert manager."""
    global _alert_manager
    _alert_manager = AlertManager(config)
    return _alert_manager


def get_alert_manager() -> Optional[AlertManager]:
    """Get global alert manager instance."""
    return _alert_manager
