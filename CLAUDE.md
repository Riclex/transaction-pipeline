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

### Install Dependencies
```bash
pip install -r requirements.txt
```

## Architecture

### High-Level Pipeline Flow
```
Raw CSV → Extract & Validate → Transform → Quality Check → Reconcile → Load → Daily Aggregation
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

**Test Coverage: 71 tests across 6 test files**

- **Unit tests**: `tests/test_extract.py` (5 tests - schema validation, duplicates, edge cases, empty DataFrames)
- **Integration tests**: `tests/test_pipeline_logic.py` (33 tests - full E2E pipeline, transformation logic, reconciliation, load validation, parametrized status tests with 14 cases)
- **AML tests**: `tests/test_aml_*.py` (33 tests total)
  - `test_aml_features.py`: 6 tests - velocity features, time features, structuring detection, round numbers
  - `test_aml_rules.py`: 9 tests - rule evaluation, risk scoring, severity mapping
  - `test_aml_alerts.py`: 7 tests - alert generation, deduplication, save/load
  - `test_aml_integration.py`: 4 tests - full AML pipeline E2E, metrics computation
- **Testing patterns**:
  - Uses `tmp_path` fixture for isolated file operations
  - Parametrized tests for status normalization covering various input formats
  - Full type hints with `-> None` return types on all test functions
  - Variable annotations (`pd.DataFrame`, `pd.Series`, `Path`) throughout
  - Edge case coverage: empty DataFrames, null values, duplicates, numpy boolean comparisons

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