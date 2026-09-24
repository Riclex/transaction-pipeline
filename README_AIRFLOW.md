# Apache Airflow Integration Guide

This document describes how to run the Bank Transaction Pipeline using Apache Airflow for orchestration, scheduling, and monitoring.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Local Development](#local-development)
- [Configuration](#configuration)
- [Task Reference](#task-reference)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)
- [Production Deployment](#production-deployment)

## Overview

The Airflow integration provides:

- **Automated Scheduling**: Run the pipeline daily at 6 AM UTC (configurable)
- **Retry Logic**: Automatic retries with exponential backoff
- **Observability**: Task duration, success rates, and failure alerts
- **Data Quality**: Built-in quality checks and reconciliation validation
- **Failure Handling**: Graceful degradation with proper alerting

## Architecture

```
FileSensor(wait_for_raw_file)
    ↓
extract_task → XCom: raw_path, row_count
    ↓
transform_task → XCom: transformed_path, ledger_path
    ↓
[quality_check_task + reconcile_task] (parallel)
    ↓
branch_on_reconcile
    ├─ success → load_task
    └─ failure → alert_reconcile_failure_task → fail
    ↓
load_task
    ↓
daily_aggregation_task
    ↓
aml_detection_task (optional)
    ↓
cleanup_intermediate_task (always runs)
```

## Quick Start

### Prerequisites

- Docker Desktop or Docker Engine
- Docker Compose
- 4GB+ available RAM

### Start Airflow

```bash
# Copy environment template
cp .env.airflow.example .env.airflow

# Edit with your settings (optional)
# nano .env.airflow

# Start Airflow services
docker-compose up -d

# Wait for initialization (30-60 seconds)
docker-compose logs -f airflow-init

# Access the UI
open http://localhost:8080
```

**Default Credentials:**
- Username: `admin`
- Password: `admin`

## Local Development

### Start Services

```bash
# Start all services
docker-compose up -d

# Start specific service
docker-compose up -d airflow-webserver

# View logs
docker-compose logs -f airflow-scheduler
```

### Stop Services

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

### Run a DAG

1. Open the Airflow UI: http://localhost:8080
2. Enable the DAG: `bank_transaction_pipeline`
3. Click the play button (▶️) to trigger manually
4. Monitor task progress in the Grid or Graph view

### View Task Logs

```bash
# Via UI: Admin → Logs
# Or via command line
docker-compose exec airflow-webserver cat logs/bank_transaction_pipeline/extract/2024-01-01T06:00:00+00:00/1.log
```

## Configuration

### Environment Variables

Edit `.env.airflow` to customize:

```bash
# Admin credentials
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=secure-password

# Scheduling
AIRFLOW__SCHEDULER__CATCHUP_BY_DEFAULT=false
AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=300

# Pipeline settings
PIPELINE_ENVIRONMENT=development
TRUST_LAYER_ENABLED=true

# Alerting
AIRFLOW_ALERT_EMAIL_SMTP_HOST=smtp.gmail.com
AIRFLOW_ALERT_EMAIL_USERNAME=alerts@example.com
AIRFLOW_ALERT_EMAIL_PASSWORD=your-app-password
```

### Pipeline Config

The DAG reads from `config/pipeline_config.yaml`. Key sections:

```yaml
# Schedule configuration
pipeline:
  name: bank_transaction_pipeline
  environment: production

# Data paths
paths:
  raw_transactions: data/raw/transactions_raw.csv
  ledger_output: data/processed/ledger_transactions.parquet

# Reconciliation settings
reconciliation:
  tolerance_amount: 0.01
  fail_on_mismatch: true

# AML detection
aml_detection:
  enabled: true
```

### Connections

Airflow connections can be configured via:

1. **Environment Variables:**
   ```bash
   export AIRFLOW_CONN_FS_DEFAULT='fs?path=/opt/airflow/data'
   ```

2. **UI:** Admin → Connections

3. **CLI:**
   ```bash
   docker-compose exec airflow-webserver airflow connections add fs_default \
       --conn-type fs \
       --conn-extra '{"path": "/opt/airflow/data"}'
   ```

## Task Reference

### wait_for_raw_file

- **Type:** FileSensor
- **Description:** Waits for the raw transactions CSV file
- **Timeout:** 2 hours
- **Poke Interval:** 60 seconds

### extract

- **Type:** PythonOperator
- **Description:** Extracts data from CSV to Parquet
- **Retries:** 2
- **Outputs:** `raw_path`, `row_count`, `run_id`

### transform

- **Type:** PythonOperator
- **Description:** Normalizes status, amounts, flags late arrivals
- **Inputs:** `raw_path` (from extract)
- **Outputs:** `transformed_path`, `ledger_path`, `transformed_count`, `ledger_count`

### quality_check

- **Type:** PythonOperator
- **Description:** Generates quality metrics and rejection report
- **Parallel to:** reconcile
- **Outputs:** `quality_passed`, `rejection_rate`, `metrics_path`

### reconcile

- **Type:** PythonOperator
- **Description:** Validates data integrity (raw vs ledger)
- **Parallel to:** quality_check
- **Outputs:** `reconcile_passed`, `mismatch_count`, `report_path`
- **Failure Behavior:** Fails pipeline if `fail_on_mismatch: true`

### branch_on_reconcile

- **Type:** BranchPythonOperator
- **Description:** Routes to load or alert based on reconciliation
- **Branches:** `load` (success) or `alert_reconcile_failure` (failure)

### load

- **Type:** PythonOperator
- **Description:** Persists ledger to final Parquet output
- **Outputs:** `output_path`, `output_row_count`

### daily_aggregation

- **Type:** PythonOperator
- **Description:** Generates daily account balances
- **Outputs:** `aggregation_path`, `aggregation_count`

### aml_detection

- **Type:** PythonOperator
- **Description:** Runs AML detection rules
- **Trigger Rule:** all_done (runs even if daily_aggregation fails)
- **Outputs:** `aml_enabled`, `aml_alerts_path`, `aml_alert_count`

### cleanup_intermediates

- **Type:** PythonOperator
- **Description:** Deletes temporary files
- **Trigger Rule:** all_done (always runs)
- **Non-fatal:** Errors don't fail the DAG

## Monitoring

### Task Duration

Monitor task durations in the Airflow UI:
- **Grid View:** Visual representation of task status
- **Graph View:** Dependency graph with execution times
- **Duration View:** Historical task duration trends

### Metrics

Airflow exports metrics in StatsD format. Enable:

```bash
# .env.airflow
AIRFLOW__METRICS__STATSD_ON=True
AIRFLOW__METRICS__STATSD_HOST=statsd
AIRFLOW__METRICS__STATSD_PORT=8125
```

### SLA Monitoring

The pipeline has built-in SLA monitoring:

```python
# In pipeline_config.yaml
trust_layer:
  sla_max_data_age_hours: 24
  sla_max_processing_time_minutes: 60
```

Alerts are generated if SLA is violated.

## Troubleshooting

### DAG Not Appearing

```bash
# Check DAG parsing
docker-compose exec airflow-webserver airflow dags list

# View DAG import errors
docker-compose exec airflow-webserver airflow dags report

# Check syntax
docker-compose exec airflow-webserver python -m py_compile dags/transaction_pipeline_dag.py
```

### Task Failures

```bash
# View task logs
docker-compose logs -f airflow-worker

# Check XCom values
docker-compose exec airflow-webserver airflow tasks test \
    bank_transaction_pipeline extract 2024-01-01
```

### Connection Issues

```bash
# List connections
docker-compose exec airflow-webserver airflow connections list

# Test connection
docker-compose exec airflow-webserver airflow connections test fs_default
```

### File Permission Issues

```bash
# Fix permissions
docker-compose exec airflow-webserver chown -R airflow: /opt/airflow/data

# Check file access
docker-compose exec airflow-webserver ls -la data/raw/
```

## Production Deployment

### Option 1: Docker Swarm

```bash
# Initialize swarm
docker swarm init

# Deploy stack
docker stack deploy -c docker-compose.yaml bank-pipeline
```

### Option 2: Kubernetes (Helm)

```bash
# Add Helm repo
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# Create values file
helm show values apache-airflow/airflow > values.yaml

# Customize values.yaml
# - Set executor to CeleryExecutor
# - Configure resource limits
# - Mount volumes for data

# Install
helm install airflow apache-airflow/airflow -f values.yaml
```

### Option 3: Managed Airflow (MWAA, Cloud Composer)

1. Package your DAGs and dependencies
2. Upload to S3 (MWAA) or GCS (Cloud Composer)
3. Configure connections via UI or environment variables
4. Monitor via cloud console

### Security Checklist

- [ ] Change default admin password
- [ ] Use Fernet key for connection encryption
- [ ] Store secrets in environment variables or secret manager
- [ ] Enable RBAC in production
- [ ] Configure SSL/TLS for webserver
- [ ] Set up backup for PostgreSQL
- [ ] Configure log rotation

## Testing

### Unit Tests

```bash
# Run tests
docker-compose exec airflow-worker pytest tests/ -v

# Run specific test
docker-compose exec airflow-worker pytest tests/test_airflow_tasks.py -v
```

### DAG Testing

```bash
# Test single task
docker-compose exec airflow-webserver airflow tasks test \
    bank_transaction_pipeline extract 2024-01-01

# Test full DAG
docker-compose exec airflow-webserver airflow dags test \
    bank_transaction_pipeline 2024-01-01
```

### Integration Tests

```bash
# Create test data
python -c "
import pandas as pd
from datetime import date
df = pd.DataFrame({
    'txn_id': ['txn-001', 'txn-002'],
    'account_id': ['acc-001', 'acc-002'],
    'txn_date': [date(2024, 1, 1), date(2024, 1, 1)],
    'amount': [100.00, -50.00],
    'currency': ['USD', 'USD'],
    'txn_type': ['CARD', 'REFUND'],
    'status': ['COMPLETED', 'COMPLETED']
})
df.to_csv('data/raw/transactions_raw.csv', index=False)
"

# Trigger DAG
open http://localhost:8080
```

## Development Tips

### Adding New Tasks

1. Define task function in `src/airflow_tasks.py`
2. Add task to DAG in `dags/transaction_pipeline_dag.py`
3. Set up dependencies
4. Add XCom pushes/pulls as needed

### Custom Operators

Create in `plugins/operators/custom_operators.py`:

```python
from airflow.models import BaseOperator

class MyCustomOperator(BaseOperator):
    def execute(self, context):
        # Your logic here
        pass
```

### XCom Best Practices

- **Pass file paths**, not DataFrames (XCom size limits)
- Use unique filenames with timestamps
- Clean up intermediates in cleanup task
- Document XCom keys in docstrings

## Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Docker Compose Reference](https://docs.docker.com/compose/)

## Support

For issues specific to this pipeline:
- Check logs: `docker-compose logs -f`
- Review CLAUDE.md for pipeline-specific context
- Run tests: `pytest tests/ -v`