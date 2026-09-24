# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.\n
## Project Overview

This is a production-grade ETL pipeline for processing financial transaction data. It follows a classic ETL pattern: Extract (CSV with validation), Transform (standardize/clean/enrich), Load (Parquet with reconciliation).

**Business Purpose:** Converts raw transaction feeds from upstream systems (card processors, core banking, settlement engines) into an auditable ledger, reconciliation-validated outputs, and daily account-level reporting tables.

## Common Commands

### Run the Pipeline
```bash
# Basic run with default config
python src/pipeline.py

# Run with custom config
python -c "from src.pipeline import run_pipeline; run_pipeline('config/custom_config.yaml')"

# Resume from checkpoint after failure
python -c "from src.pipeline import run_pipeline; run_pipeline(resume=True)"
```

### Run Tests
```bash
# Run all tests
pytest tests/ -v

# Run a single test file
pytest tests/test_pipeline_logic.py -v
pytest tests/test_extract.py -v

# Run a specific test
pytest tests/test_pipeline_logic.py::test_refund_amount_is_negative -v

# Run AML tests only
pytest tests/test_aml_*.py -v
```

### Run AML Detection
```bash
# Run AML detection on ledger output (after pipeline completes)
python src/aml_detector.py

# Run with custom config
python src/aml_detector.py --config config/pipeline_config.yaml

# Run with custom ledger input
python src/aml_detector.py --ledger data/processed/ledger_transactions.parquet
```

### Run with Apache Airflow
```bash
# Start Airflow services
docker-compose up -d

# Access UI at http://localhost:8080 (admin/admin)

# Trigger DAG manually
docker-compose exec airflow-webserver airflow dags trigger bank_transaction_pipeline

# Check DAG status
docker-compose exec airflow-webserver airflow dags list-runs -d bank_transaction_pipeline

# View task logs
docker-compose logs -f airflow-scheduler

# Stop Airflow
docker-compose down
```

### Install Dependencies
```bash
pip install -r requirements.txt
```

### Run with Apache Airflow
```bash
# Start Airflow services
docker-compose up -d

# Access UI at http://localhost:8080 (admin/admin)

# Trigger DAG manually
docker-compose exec airflow-webserver airflow dags trigger bank_transaction_pipeline

# Check DAG status
docker-compose exec airflow-webserver airflow dags list-runs -d bank_transaction_pipeline

# View logs
docker-compose logs -f airflow-scheduler

# Stop Airflow
docker-compose down
```

## Architecture

### High-Level Pipeline Flow
```
Raw CSV → Extract & Validate → Transform → Quality Check → Reconcile → Load → Daily Aggregation
```

### Apache Airflow Orchestration (Production Deployment)

The pipeline can run under Apache Airflow for production-grade orchestration:

```
FileSensor(wait_for_raw_file)
    ↓
extract_task → XCom: raw_path, row_count
    ↓
transform_task → XCom: transformed_path, ledger_path
    ↓
[quality_check_task + reconcile_task] (parallel in TaskGroup)
    ↓
branch_on_reconcile (BranchPythonOperator)
    ├─ success → load_task → daily_aggregation_task → aml_detection_task
    └─ failure → alert_reconcile_failure_task
    ↓
cleanup_intermediate_task (trigger_rule='all_done')
```

**Airflow DAG Features:**
- **FileSensor**: Waits for raw file with configurable timeout (2 hours default)
- **TaskGroup**: Parallel validation (quality + reconcile) for efficiency
- **BranchPythonOperator**: Conditional routing based on reconciliation result
- **XCom**: Inter-task communication for file paths and metadata
- **Parquet intermediates**: DataFrames passed via `data/intermediate/` directory
- **Always cleanup**: Cleanup task runs regardless of success/failure
- **Retry logic**: Configurable retries with exponential backoff
- **SLA monitoring**: Built-in SLA tracking and alerting

**Airflow Task Entry Points:** `src/airflow_tasks.py`
- `extract_task()` - Extract from CSV, save to intermediate Parquet
- `transform_task()` - Normalize status/amounts, flag late arrivals
- `quality_check_task()` - Generate quality metrics and rejection report
- `reconcile_task()` - Compare raw vs ledger totals
- `branch_on_reconcile()` - Route to load or alert based on reconciliation
- `load_task()` - Persist ledger to Parquet
- `daily_aggregation_task()` - Generate daily account balances
- `aml_detection_task()` - Run AML detection (optional)
- `cleanup_intermediate_task()` - Delete temporary files

**Airflow Configuration:** `config/pipeline_config.yaml`
```yaml
airflow:
  enabled: false  # Set to true when running under Airflow
  dag_id: bank_transaction_pipeline
  schedule_interval: "0 6 * * *"  # Daily at 6 AM UTC
  start_date: 2024-01-01
  catchup: false
  max_active_runs: 1
  default_args:
    retries: 2
    retry_delay_minutes: 5
    execution_timeout_minutes: 120
```

### AML Detection Module (Post-Processing)

The AML (Anti-Money Laundering) detection module runs as a separate post-processing step on the ledger output:

```
Ledger Parquet → Load → Feature Engineering → Rule Evaluation → Alert Generation → Output
```

**Detection Rules:**
- **Velocity checks**: Daily transaction count, daily amount, 7-day rolling amount thresholds
- **Structuring detection**: Transactions near CTR threshold ($10,000) indicating potential threshold avoidance
- **Round number analysis**: Suspiciously round amounts (e.g., $5,000, $10,000)

**Risk Scoring:**
- Composite score 0-100 based on weighted rule triggers
- Severity levels: none, low, medium, high
- Deduplication: One alert per (account, rule, date) combination

### Component Responsibilities

**`src/pipeline.py`** - Main orchestrator (`run_pipeline()`)
- Loads config via `load_config_validated()`
- Executes ETL steps in sequence with checkpointing
- **Resume capability**: Full step-by-step resume with `ResumeState` dataclass
  - Tracks completed steps: `extract`, `transform`, `reconcile`, `load`
  - Skips completed steps on resume, runs remaining steps
  - Preserves data across steps via checkpoint pickle files
- Performs daily balance aggregation after load
- Quality metrics and rejection reports (always run, idempotent)

**`src/extract.py`** - Data ingestion with strict validation
- `extract_transactions()`: Loads CSV, validates schema
- Raises `SchemaValidationError` on: missing columns, duplicate `txn_id`, null values in required fields (`account_id`, `status`, `txn_type`), invalid transaction types
- Supports chunked processing via `chunksize` parameter

**`src/transform.py`** - Business logic standardization
- `transform_transactions()`: Returns `(clean_df, ledger_df)` tuple
- Status normalization: COMPLETED/SETTLED/OK → SUCCESS; others → FAILED
- Amount normalization: Refunds forced negative (`-abs(amount)`), other types keep original sign
- Late arrival detection: `is_late` flag when `txn_date < ingestion_date`
- Date parsing with fail-fast on invalid dates

**`src/reconcile.py`** - Data integrity validation
- `reconcile_raw_vs_ledger()`: Compares raw vs ledger totals by date
- Configurable tolerance (default 0.01)
- **Fails pipeline** if mismatches exceed tolerance (raises `ValueError`)

**`src/load.py`** - Atomic persistence
- `load_ledger()`: Writes to Parquet with `txn_id` uniqueness validation
- Atomic write pattern (temp file + rename)
- Falls back to CSV if no Parquet engine available

**`src/quality.py`** - Observability
- `calculate_data_quality_metrics()`: Computes row counts, nulls, late arrivals, status distributions
- `export_data_quality_metrics()`: Writes to JSON for monitoring dashboards
- `generate_rejection_report()`: CSV of failed transactions

**`src/checkpoint.py`** - Fault tolerance
- `PipelineCheckpoint`: Saves state at each step using JSON metadata + pickle for DataFrames
- `resume_pipeline_from_checkpoint()`: Loads checkpoint data for resumption
- `ResumeState` dataclass: Tracks completed steps (`extract`, `transform`, `reconcile`, `load`) with step-by-step skipping logic
- Full checkpoint/resume implementation with `_load_resume_state()` and `is_step_completed()` checks
- Auto-cleanup on successful completion

**`src/airflow_tasks.py`** - Airflow task entry points
- `extract_task()`: Extract from CSV, save to intermediate Parquet, push XCom metadata
- `transform_task()`: Normalize status/amounts, flag late arrivals, write intermediates
- `quality_check_task()`: Generate quality metrics, export JSON/CSV reports
- `reconcile_task()`: Compare raw vs ledger totals, return success/failure status
- `branch_on_reconcile()`: BranchPythonOperator logic for conditional routing
- `load_task()`: Persist ledger to Parquet with atomic write pattern
- `daily_aggregation_task()`: Generate daily account balances
- `aml_detection_task()`: Run AML detection on ledger output
- `cleanup_intermediate_task()`: Delete temporary files (trigger_rule='all_done')
- All tasks use XCom for metadata (paths, counts) and Parquet files for DataFrames

**`dags/transaction_pipeline_dag.py`** - Airflow DAG definition
- DAG with 11 tasks: FileSensor, PythonOperators, BranchPythonOperator, TaskGroup
- Schedule: Daily at 6 AM UTC (configurable)
- Parallel validation tasks (quality_check + reconcile in TaskGroup)
- Branching logic based on reconciliation result
- Always-cleanup pattern for resource management

**`plugins/operators/custom_operators.py`** - Custom Airflow operators
- `DataFrameToPostgresOperator`: Load Parquet files to PostgreSQL tables
- `LineageExportOperator`: Export data lineage graphs
- `SLACheckOperator`: Monitor SLA compliance and alert on violations

**`src/config_schema.py`** - Configuration validation
- Pydantic models (`PipelineConfig`, `BusinessRulesConfig`, etc.)
- Validates `config/pipeline_config.yaml` at runtime
- Default values for all settings
- **Environment variable support**: Use `${VAR_NAME}` syntax for sensitive values (email_password, webhook_url, etc.)

**`src/config.py`** - Configuration loader
- `load_config()`: Loads and validates config, returns dict for backward compatibility
- `load_config_validated()`: Returns validated `PipelineConfig` object with type hints
- Handles path resolution relative to project root

**`src/aml/`** - AML detection module (optional post-processing)
- `loader.py`: `load_ledger_for_aml()` reads ledger Parquet with schema validation
- `features.py`: Computes velocity features (1d/7d/30d), time features, structuring detection, round number detection
- `rules.py`: `AmlRuleEngine` class evaluates rules and calculates risk scores (0-100)
- `alerts.py`: `AlertManager` generates deduplicated alerts with severity levels
- `aml_detector.py`: Standalone CLI entry point for running AML detection

**`src/airflow_tasks.py`** - Airflow task entry points
- `extract_task()`: Extract from CSV, save to intermediate Parquet, push XCom
- `transform_task()`: Normalize data, read/write intermediates, XCom metadata
- `quality_check_task()`: Generate quality metrics and rejection report
- `reconcile_task()`: Validate data integrity, return reconciliation status
- `branch_on_reconcile()`: Branching logic for success/failure paths
- `load_task()`: Persist ledger to Parquet output
- `daily_aggregation_task()`: Generate daily account balances
- `aml_detection_task()`: Run AML detection (conditional on config)
- `cleanup_intermediate_task()`: Delete temporary files (always runs)
- `alert_reconcile_failure_task()`: Send alerts on reconciliation failure

**`dags/transaction_pipeline_dag.py`** - Airflow DAG definition
- 11 tasks with dependencies: FileSensor → extract → transform → validation (parallel) → branch → load → aggregation → AML → cleanup
- Uses TaskGroup for parallel validation tasks
- BranchPythonOperator for conditional routing
- Trigger rules: `all_done` for cleanup, `all_done` for AML

**`docker-compose.yaml`** - Airflow infrastructure
- PostgreSQL metadata database
- Airflow Webserver (port 8080)
- Airflow Scheduler
- Airflow Triggerer
- Volume mounts for dags/, src/, config/, data/

### Key Design Principles

1. **Fail-fast validation**: Schema errors halt pipeline immediately; no silent data fixes
2. **Idempotency**: Safe to re-run with same inputs; `txn_id` uniqueness enforced on load
3. **Ledger-first**: Accounting-ready outputs prioritized; only SUCCESS transactions go to ledger
4. **Explicit sign handling**: Refunds negative, others keep sign (configurable via `TXN_TYPE_SIGN` mapping)
5. **Reconciliation-first**: Data integrity check **before** persistence prevents corrupted data downstream
6. **Configuration-driven**: All business rules externalized in YAML (success statuses, refund types, valid transaction types, tolerance)

### Security Considerations

**Secrets Management:**
- Never commit passwords or API keys to the repository
- Use environment variables for sensitive configuration:
  ```yaml
  trust_layer:
    alerting:
      email_password: "${EMAIL_PASSWORD}"
      webhook_url: "${WEBHOOK_URL}"
  ```
- Set environment variables before running:
  ```bash
  export EMAIL_PASSWORD="your-password"
  export WEBHOOK_URL="https://hooks.slack.com/..."
  python src/pipeline.py
  ```

**Cryptographic Hashing:**
- File lineage tracking uses SHA-256 (not MD5)
- Config hash validation uses SHA-256 for detecting config changes on resume

**Timeouts:**
- Webhook requests have 10-second timeout to prevent hanging
- Consider adding timeouts for database connections if applicable

**Deprecation Notes:**
- All datetime handling uses timezone-aware UTC (`datetime.now(timezone.utc)`)
- Pydantic v2 patterns used throughout (`.model_dump()` instead of `.dict()`)

### Configuration File (`config/pipeline_config.yaml`)

```yaml
pipeline:
  name: bank_transaction_pipeline
  environment: local

paths:
  raw_transactions: data/raw/transactions_raw.csv
  ledger_output: data/processed/ledger_transactions.parquet
  daily_balance_output: data/processed/daily_account_balance.parquet

data_schema:
  required_columns:
    - txn_id
    - account_id
    - txn_date
    - ingestion_date
    - amount
    - currency
    - txn_type
    - status

business_rules:
  success_statuses: [COMPLETED, SETTLED, OK]
  refund_types: [REFUND]
  valid_txn_types: [CARD, CASH, REFUND, DEBIT, CREDIT, TRANSFER, WITHDRAWAL, FEE, DEPOSIT, PAYMENT]
  late_arrival_days_threshold: 0

reconciliation:
  tolerance_amount: 0.00
  fail_on_mismatch: true

processing:
  chunksize: null  # Set to number for large files

aggregation:
  daily_balance:
    group_by: [account_id, txn_date]
    metrics:
      total_amount: sum
      txn_count: count

# AML Detection Configuration (optional post-processing)
aml_detection:
  enabled: true
  input_path: data/processed/ledger_transactions.parquet
  output_paths:
    scored_transactions: data/processed/aml_scored_transactions.parquet
    alerts: data/processed/aml_alerts.parquet
  velocity_rules:
    daily_txn_count_threshold: 10        # Flag >10 txns/day
    daily_amount_threshold: 50000.00     # Flag >$50k/day
    seven_day_amount_threshold: 150000.00
  structuring_rules:
    ctr_threshold: 10000.00              # CTR threshold
    lower_bound_factor: 0.90             # Flag >90% of threshold
    upper_bound_factor: 0.99             # Flag <99% of threshold
  round_number_rules:
    enabled: true
    large_round_amounts: [10000, 5000, 1000, 500]
  risk_weights:                          # Score contributions
    velocity_daily_count: 25
    velocity_daily_amount: 30
    velocity_7d_amount: 35
    structuring: 50
    round_number: 15
  alert_thresholds:
    low: 25
    medium: 50
    high: 75
```

### Output Files

| File | Description |
|------|-------------|
| `data/processed/ledger_transactions.parquet` | Standardized transaction ledger (SUCCESS only) |
| `data/processed/daily_account_balance.parquet` | Daily account aggregations |
| `data/processed/data_quality_metrics.json` | Quality metrics for monitoring |
| `data/processed/rejection_report.csv` | Rejected transaction details |
| `data/processed/aml_scored_transactions.parquet` | AML-scored transactions with risk features |
| `data/processed/aml_alerts.parquet` | AML alerts (deduplicated by account/rule/date) |
| `data/checkpoints/` | Pipeline state for resume |

### Testing Strategy

**Test Coverage: 93+ tests across 8 test files**

- **Unit tests**: `tests/test_extract.py` (5 tests - schema validation, duplicates, edge cases, empty DataFrames)
- **Integration tests**: `tests/test_pipeline_logic.py` (33 tests - full E2E pipeline, transformation logic, reconciliation, load validation, parametrized status tests with 14 cases)
- **AML tests**: `tests/test_aml_*.py` (33 tests total)
  - `test_aml_features.py`: 6 tests - velocity features, time features, structuring detection, round numbers
  - `test_aml_rules.py`: 9 tests - rule evaluation, risk scoring, severity mapping
  - `test_aml_alerts.py`: 7 tests - alert generation, deduplication, save/load
  - `test_aml_integration.py`: 4 tests - full AML pipeline E2E, metrics computation
- **Airflow tests**: `tests/test_airflow_tasks.py`, `tests/test_dag_definition.py`
  - Task function unit tests with mocked TaskInstance
  - XCom data passing validation
  - DAG structure tests (task count, dependencies, properties)
  - Branching logic validation
  - Custom operator tests
- **Testing patterns**:
  - Uses `tmp_path` fixture for isolated file operations
  - Parametrized tests for status normalization covering various input formats
  - Full type hints with `-> None` return types on all test functions
  - Variable annotations (`pd.DataFrame`, `pd.Series`, `Path`) throughout
  - Edge case coverage: empty DataFrames, null values, duplicates, numpy boolean comparisons
  - Airflow fallback imports for local development without Airflow installed

### Module Import Pattern

The project uses explicit path manipulation to support running scripts directly:
```python
sys.path.insert(0, str(Path(__file__).parent.parent))
```

This allows `python src/pipeline.py` to work without relying on PYTHONPATH.

### Type Hints Pattern

All source files use `from __future__ import annotations` for forward compatibility.
Test files include comprehensive type annotations:
```python
def test_extract_success() -> None:
    df: pd.DataFrame = extract_transactions(io.StringIO(CSV))
    assert len(df) == 2
```

Numpy boolean comparisons use equality (`== True`) rather than identity (`is True`) to handle `np.True_` values correctly.