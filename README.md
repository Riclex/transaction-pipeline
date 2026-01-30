# Bank Transaction Data Pipeline

A production-grade ETL pipeline for processing financial transaction data with enterprise features including configuration management, data quality monitoring, fault tolerance, and comprehensive validation.

## Business Context

Banks and fintechs ingest raw transaction data from multiple upstream systems (card processors, core banking, settlement engines). This data is often:

* Delivered late or out of order
* Inconsistent in status codes
* Incorrectly signed (refunds vs payments)
* Missing critical fields
* Containing invalid transaction types
* Not immediately usable for reporting or compliance

This project implements a **bank transaction pipeline** that converts raw transaction feeds into:

* An auditable transaction ledger
* Reconciliation-validated outputs
* Daily account-level reporting tables
* Comprehensive data quality metrics

The design mirrors real-world financial data engineering patterns used in regulated environments.

## Architecture Overview

**High-level flow:**

Raw CSV → **Extract & Validate** → **Transform** → **Quality Check** → **Reconcile** → **Load** → **Report**

**Key design principles:**

* **Configuration-driven**: All business rules and processing options configurable via YAML
* **Fail-fast validation**: Strict data quality checks prevent bad data from entering the pipeline
* **Ledger-first thinking**: Accounting-ready outputs prioritized over raw data
* **Idempotent processing**: Safe to re-run with same inputs
* **Late-arriving transaction handling**: Explicit tracking and processing of delayed data
* **Fault tolerance**: Checkpoint/resume capability for long-running jobs
* **Data quality monitoring**: Comprehensive metrics and rejection reporting

## Pipeline Components

### Extract (`src/extract.py`)

**Responsibilities:**
* Load raw transaction CSV files with chunked processing support
* Enforce required schema and data types
* **Fail fast** on missing columns, duplicate transaction IDs, null values in required fields, or invalid transaction types
* Validate against configurable business rules

**Validation checks:**
* Required columns present
* No duplicate `txn_id` values
* No null/empty values in `account_id`, `status`, `txn_type`
* All `txn_type` values in configured `valid_txn_types` list

### Transform (`src/transform.py`)

Standardizes raw transactions using explicit financial rules.

**Standardization rules:**
* Parse transaction and ingestion dates
* Normalize transaction statuses (SUCCESS/FAILED based on configured success statuses)
* Normalize transaction amounts with proper signing (refunds negative, others keep sign)
* Detect late-arriving transactions (`txn_date < ingestion_date`)

**Outputs:**
* `transformed_df`: All standardized transactions with audit trail
* `ledger_df`: Accounting-ready ledger (SUCCESS transactions only)

### Quality Monitoring (`src/quality.py`)

**Data quality features:**
* Calculate comprehensive quality metrics (null counts, late arrivals, status distributions)
* Export metrics to JSON for monitoring dashboards
* Generate CSV rejection reports for failed transactions
* Track data quality trends over time

### Reconciliation (`src/reconcile.py`)

Mandatory control before data persistence:

* Compares standardized transaction totals vs ledger totals by date
* Applies configurable tolerance thresholds
* **Fails pipeline** if reconciliation mismatches exceed tolerance
* Prevents corrupted data from entering downstream systems

### Load (`src/load.py`)

Idempotent persistence layer:

* Enforces uniqueness on `txn_id`
* Prevents duplicate ledger entries
* Writes columnar Parquet files optimized for analytics
* Supports both PyArrow and fastparquet engines

### Checkpoint/Resume (`src/checkpoint.py`)

**Fault tolerance features:**
* Saves pipeline state at each major step
* Enables resume from last successful checkpoint
* Prevents reprocessing on failure
* Automatic cleanup on successful completion

## Configuration Management

The pipeline is fully configurable via `config/pipeline_config.yaml`:

```yaml
pipeline:
  name: bank_transaction_pipeline
  environment: local

paths:
  raw_transactions: data/raw/transactions_raw.csv
  ledger_output: data/processed/ledger_transactions.parquet
  daily_balance_output: data/processed/daily_account_balance.parquet

business_rules:
  success_statuses: [COMPLETED, SETTLED, OK]
  refund_types: [REFUND]
  valid_txn_types: [CARD, CASH, REFUND, DEBIT, CREDIT, TRANSFER, WITHDRAWAL, FEE, DEPOSIT, PAYMENT]
  late_arrival_days_threshold: 0

reconciliation:
  tolerance_amount: 0.00
  fail_on_mismatch: true

processing:
  chunksize: null  # Set to number (e.g., 10000) for large files
```

Configuration is validated using Pydantic models ensuring type safety and required fields.

## Late-Arriving Transaction Handling

Transactions may arrive days after they occur. The pipeline:

* Tracks both `txn_date` and `ingestion_date`
* Flags late-arriving transactions explicitly
* Recomputes aggregates when late data arrives
* Ensures historical balances remain correct without full reloads

## Regulatory Alignment

### Basel (Risk & Capital Reporting)
* Accurate daily exposure per account
* Complete and auditable transaction history
* Correct handling of backdated transactions

### AML (Anti-Money Laundering)
* Transaction-level traceability
* Reliable transaction counts and volumes
* Reproducible investigation datasets

### PSD2 / Open Banking
* Accurate transaction history
* Correct account balances
* Consistent corrections when delayed settlements arrive

## Project Structure

```
bank-transaction-pipeline/
├── config/
│   └── pipeline_config.yaml          # Pipeline configuration
├── data/
│   ├── raw/
│   │   ├── transactions_raw.csv      # Input data
│   │   └── bad_transactions.csv      # Test data with validation errors
│   └── processed/                    # Output directory
├── src/
│   ├── __init__.py
│   ├── pipeline.py                   # Main orchestrator
│   ├── extract.py                    # Data extraction & validation
│   ├── transform.py                  # Business logic & standardization
│   ├── quality.py                    # Data quality monitoring
│   ├── reconcile.py                  # Reconciliation checks
│   ├── load.py                       # Data persistence
│   ├── checkpoint.py                 # Fault tolerance
│   ├── config.py                     # Configuration loading
│   ├── config_schema.py              # Pydantic validation models
│   └── sql/
│       └── daily_account_balance.sql # Aggregation queries
├── tests/                            # Unit tests
├── requirements.txt                  # Python dependencies
└── README.md                         # This file
```

## Setup & Installation

### Prerequisites
- Python 3.10+
- pip

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd bank-transaction-pipeline
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Basic Run
```bash
python src/pipeline.py
```

### With Custom Config
```bash
python -c "from src.pipeline import run_pipeline; run_pipeline('config/custom_config.yaml')"
```

### Resume from Checkpoint
```bash
python -c "from src.pipeline import run_pipeline; run_pipeline(resume=True)"
```

### Processing Large Files
Update `config/pipeline_config.yaml`:
```yaml
processing:
  chunksize: 10000  # Process in chunks of 10k rows
```

## Outputs

The pipeline generates:

* **`data/processed/ledger_transactions.parquet`**: Standardized transaction ledger
* **`data/processed/daily_account_balance.parquet`**: Daily account aggregations
* **`data/processed/data_quality_metrics.json`**: Quality metrics for monitoring
* **`data/processed/rejection_report.csv`**: Details of rejected transactions
* **`data/checkpoints/`**: Pipeline state for resume capability

## Data Quality Monitoring

The pipeline exports comprehensive quality metrics:

```json
{
  "total_rows": 10000,
  "successful_rows": 7986,
  "failed_rows": 2014,
  "null_counts": {"account_id": 0, "status": 0},
  "late_arrivals": 2983,
  "status_distribution": {"SUCCESS": 7986, "FAILED": 2014}
}
```

## Error Handling

The pipeline implements comprehensive error handling:

* **Schema validation errors**: Missing columns, null values, invalid types
* **Reconciliation failures**: Data integrity mismatches
* **File I/O errors**: Missing files, permission issues
* **Configuration errors**: Invalid YAML, missing required fields

All errors are logged with structured messages and appropriate exit codes.

## Testing

Run the test suite:
```bash
pip install pytest
pytest tests/ -v
```

## Production Deployment

This project is production-ready and can scale to enterprise environments:

* **Streaming ingestion**: Replace CSV with Kafka/SFTP feeds
* **Cloud storage**: S3/ADLS/GCS for Parquet files
* **Orchestration**: Airflow/Dagster/Prefect for scheduling
* **Data lakehouse**: Delta Lake/Iceberg for advanced analytics
* **Monitoring**: Integrate quality metrics with observability platforms

## Key Takeaways

This project demonstrates:

* **Financial data pipeline design** with regulatory compliance
* **Configuration-driven architecture** for maintainability
* **Comprehensive validation** preventing data quality issues
* **Fault-tolerant processing** with checkpoint/resume
* **Data quality monitoring** for production observability
* **Late-arriving data handling** for accurate historical reporting
* **Reconciliation-first approach** ensuring data integrity