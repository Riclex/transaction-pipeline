"""Custom Airflow Operators for the Bank Transaction Pipeline.

This module contains custom operators that extend Airflow's functionality
for specific use cases in the transaction pipeline.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Sequence

try:
    from airflow.models import BaseOperator
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.utils.context import Context
except ImportError:
    # Fallback for local development without Airflow
    BaseOperator = object
    PostgresHook = object
    Context = Any

# Logger
LOGGER = logging.getLogger(__name__)


class DataFrameToPostgresOperator(BaseOperator):
    """Operator to load a DataFrame into a PostgreSQL table.

    This operator reads a Parquet file and loads it into a PostgreSQL table,
    with options for upsert behavior and batch loading.

    :param parquet_path: Path to the Parquet file
    :param postgres_conn_id: Airflow connection ID for PostgreSQL
    :param table_name: Target table name
    :param schema: Database schema (default: public)
    :param if_exists: Behavior if table exists ('fail', 'replace', 'append')
    :param batch_size: Number of rows to insert per batch
    """

    template_fields: Sequence[str] = ("parquet_path", "table_name", "schema")
    template_ext: Sequence[str] = (".parquet",)

    def __init__(
        self,
        *,
        parquet_path: str,
        postgres_conn_id: str = "postgres_default",
        table_name: str,
        schema: str = "public",
        if_exists: str = "append",
        batch_size: int = 10000,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.parquet_path = parquet_path
        self.postgres_conn_id = postgres_conn_id
        self.table_name = table_name
        self.schema = schema
        self.if_exists = if_exists
        self.batch_size = batch_size

    def execute(self, context: Context) -> None:
        """Execute the operator."""
        try:
            import pandas as pd
            from sqlalchemy import create_engine

            # Read Parquet file
            self.log.info("Reading Parquet file: %s", self.parquet_path)
            df = pd.read_parquet(self.parquet_path)
            row_count = len(df)
            self.log.info("Loaded %d rows from Parquet", row_count)

            # Get PostgresHook to build connection string
            pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            conn = pg_hook.get_connection(self.postgres_conn_id)

            # Build SQLAlchemy connection string
            conn_str = (
                f"postgresql://{conn.login}:{conn.password}"
                f"@{conn.host}:{conn.port}/{conn.schema}"
            )

            # Load to PostgreSQL
            self.log.info(
                "Loading to %s.%s (if_exists=%s)",
                self.schema, self.table_name, self.if_exists
            )

            engine = create_engine(conn_str)
            df.to_sql(
                name=self.table_name,
                con=engine,
                schema=self.schema,
                if_exists=self.if_exists,
                index=False,
                method="multi",
                chunksize=self.batch_size,
            )

            self.log.info(
                "Successfully loaded %d rows to %s.%s",
                row_count, self.schema, self.table_name
            )

            # Push row count to XCom
            context["ti"].xcom_push(key=f"{self.task_id}_row_count", value=row_count)

        except Exception as exc:
            self.log.exception("Failed to load DataFrame to PostgreSQL: %s", exc)
            raise


class LineageExportOperator(BaseOperator):
    """Operator to export data lineage information.

    This operator exports lineage data to a specified format and location.

    :param lineage_dir: Directory containing lineage data
    :param output_format: Export format ('json', 'graphml', 'mermaid')
    :param output_path: Path for the exported file
    """

    template_fields: Sequence[str] = ("lineage_dir", "output_path")

    def __init__(
        self,
        *,
        lineage_dir: str,
        output_format: str = "json",
        output_path: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.lineage_dir = lineage_dir
        self.output_format = output_format
        self.output_path = output_path

    def execute(self, context: Context) -> str:
        """Execute the operator."""
        try:
            import sys
            from pathlib import Path

            # Add project root to path
            project_root = Path(__file__).resolve().parent.parent.parent
            sys.path.insert(0, str(project_root))

            from src.lineage import get_lineage_tracker

            # Get lineage tracker
            tracker = get_lineage_tracker()

            if tracker is None:
                self.log.warning("No lineage tracker available")
                return ""

            # Export lineage
            if self.output_path is None:
                timestamp = context["ts_nodash"]
                self.output_path = f"{self.lineage_dir}/lineage_export_{timestamp}.json"

            Path(self.output_path).parent.mkdir(parents=True, exist_ok=True)
            export_path = tracker.export_lineage(self.output_path)

            self.log.info("Lineage exported to: %s", export_path)

            # Push to XCom
            context["ti"].xcom_push(key="lineage_export_path", value=export_path)

            return export_path

        except Exception as exc:
            self.log.exception("Failed to export lineage: %s", exc)
            raise


class SLACheckOperator(BaseOperator):
    """Operator to check SLA compliance.

    This operator checks if data processing meets SLA requirements
    and can alert if SLA is violated.

    :param sla_config: SLA configuration dictionary
    :param data_timestamp: Timestamp of the data being processed
    :param processing_start: When processing started
    :param alert_on_violation: Whether to alert if SLA is violated
    """

    def __init__(
        self,
        *,
        sla_config: dict[str, Any],
        data_timestamp: str | None = None,
        processing_start: str | None = None,
        alert_on_violation: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sla_config = sla_config
        self.data_timestamp = data_timestamp
        self.processing_start = processing_start
        self.alert_on_violation = alert_on_violation

    def execute(self, context: Context) -> dict[str, Any]:
        """Execute the operator."""
        try:
            import sys
            from datetime import datetime, timezone
            from pathlib import Path

            # Add project root to path
            project_root = Path(__file__).resolve().parent.parent.parent
            sys.path.insert(0, str(project_root))

            from src.sla_monitor import init_sla_monitor, SLAConfig

            # Initialize SLA monitor
            sla_monitor = init_sla_monitor(
                SLAConfig(
                    max_data_age_hours=self.sla_config.get("max_data_age_hours", 24),
                    max_processing_time_minutes=self.sla_config.get(
                        "max_processing_time_minutes", 60
                    ),
                ),
                Path("data/sla"),
            )

            # Check data freshness
            if self.data_timestamp:
                data_time = datetime.fromisoformat(self.data_timestamp)
                freshness_report = sla_monitor.check_data_freshness(
                    "pipeline_data", data_time
                )

                if freshness_report.status == "stale" and self.alert_on_violation:
                    self.log.error(
                        "SLA violation: Data is %.1f hours old",
                        freshness_report.data_age_hours
                    )

            # Check processing latency
            if self.processing_start:
                start_time = datetime.fromisoformat(self.processing_start)
                end_time = datetime.now(timezone.utc)
                sla_monitor.check_processing_latency(
                    "pipeline_execution", start_time, end_time
                )

            # Get summary
            summary = sla_monitor.get_sla_summary()

            self.log.info("SLA Summary: %s", summary)

            # Push to XCom
            context["ti"].xcom_push(key="sla_summary", value=summary)

            return summary

        except Exception as exc:
            self.log.exception("Failed to check SLA: %s", exc)
            raise