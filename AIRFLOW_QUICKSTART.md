# Quick Start Guide: Apache Airflow Integration

## Overview

The bank transaction pipeline now supports Apache Airflow for production-grade orchestration.

## Files Created

| File | Description |
|------|-------------|
| `docker-compose.yaml` | Airflow services configuration |
| `Dockerfile.airflow` | Custom Airflow Docker image |
| `.env.airflow.example` | Environment variables template |
| `airflow-requirements.txt` | Additional Python dependencies |
| `src/airflow_tasks.py` | Task entry points (8 functions) |
| `dags/transaction_pipeline_dag.py` | Main DAG definition |
| `plugins/operators/custom_operators.py` | Custom Airflow operators |
| `config/airflow/airflow_connections.yaml` | Connection definitions |
| `config/airflow/airflow_variables.json` | Variable definitions |
| `scripts/init_airflow.sh` | Initialization script |
| `README_AIRFLOW.md` | Full documentation |
| `tests/test_airflow_tasks.py` | Tests for task functions |
| `tests/test_dag_definition.py` | Tests for DAG structure |

## Quick Start

### 1. Start Airflow

```bash
# Copy environment file
cp .env.airflow.example .env.airflow

# Start services
docker-compose up -d

# Wait for initialization (30-60 seconds)
```

### 2. Access the UI

Open http://localhost:8080

- Username: `admin`
- Password: `admin`

### 3. Enable the DAG

1. Toggle the `bank_transaction_pipeline` DAG to ON
2. Click the DAG name to view details
3. Trigger manually or wait for scheduled run

### 4. Monitor Tasks

- **Grid View**: Task execution status
- **Graph View**: Dependency visualization
- **Logs**: Detailed task output

## DAG Structure

```
wait_for_raw_file (FileSensor)
    ↓
extract → XCom: raw_path, row_count
    ↓
transform → XCom: transformed_path, ledger_path
    ↓
┌─────────────────────────────────────┐
│  validate (TaskGroup)               │
│  ├── quality_check (parallel)     │
│  └── reconcile (parallel)          │
└─────────────────────────────────────┘
    ↓
branch_on_reconcile
    ├── success → load
    └── failure → alert_reconcile_failure
    ↓
load → XCom: output_path
    ↓
daily_aggregation
    ↓
aml_detection (optional)
    ↓
cleanup_intermediates (always runs)
```

## Configuration

### Schedule

Edit `config/pipeline_config.yaml`:

```yaml
airflow:
  enabled: true
  schedule_interval: "0 6 * * *"  # Daily at 6 AM UTC
  max_active_runs: 1
```

### Alerting

```bash
# Edit .env.airflow
AIRFLOW_ALERT_EMAIL_SMTP_HOST=smtp.gmail.com
AIRFLOW_ALERT_EMAIL_USERNAME=your-email@gmail.com
AIRFLOW_ALERT_EMAIL_PASSWORD=your-app-password
```

## Useful Commands

```bash
# View logs
docker-compose logs -f airflow-scheduler

# Run a task manually
docker-compose exec airflow-webserver \
    airflow tasks test bank_transaction_pipeline extract 2024-01-01

# Check DAG parsing
docker-compose exec airflow-webserver airflow dags list

# Trigger DAG manually
docker-compose exec airflow-webserver \
    airflow dags trigger bank_transaction_pipeline

# Stop all services
docker-compose down

# Full cleanup (including data)
docker-compose down -v
```

## Data Flow

1. **XCom**: Used for metadata (paths, counts, status flags)
2. **Parquet Files**: DataFrames passed via `data/intermediate/`
3. **Cleanup**: Automatic cleanup of intermediates (all_done trigger)

## Troubleshooting

| Issue | Solution |
|-------|----------|
| DAG not appearing | `docker-compose logs airflow-init` |
| Import errors | Check `PYTHONPATH` in docker-compose.yaml |
| File not found | Verify `fs_default` connection |
| Task timeout | Increase `execution_timeout` in config |
| Memory issues | Adjust `chunksize` in pipeline config |

## Testing

```bash
# Run Airflow task tests
pytest tests/test_airflow_tasks.py -v

# Run DAG structure tests
pytest tests/test_dag_definition.py -v

# Run all tests
pytest tests/ -v
```

## Production Deployment

See `README_AIRFLOW.md` for:
- Kubernetes deployment
- CeleryExecutor setup
- SSL/TLS configuration
- Backup strategies
- Monitoring and alerts

## Architecture Highlights

- **Full Task Decomposition**: Each pipeline step is a separate Airflow task
- **XCom for Metadata**: File paths and counts passed via XCom
- **Parquet Intermediates**: DataFrames stored in `data/intermediate/`
- **Parallel Validation**: Quality check and reconcile run simultaneously
- **Branching Logic**: Conditional flow based on reconciliation result
- **Always Cleanup**: Cleanup task runs regardless of success/failure
- **Configurable**: All settings in `config/pipeline_config.yaml`