#!/bin/bash
# Airflow Initialization Script
# Sets up Airflow connections, variables, and initial configuration

set -e

echo "============================================"
echo "Bank Transaction Pipeline - Airflow Setup"
echo "============================================"

# Wait for database to be ready
echo "Waiting for PostgreSQL to be ready..."
sleep 5

# Initialize Airflow database
echo "Initializing Airflow database..."
airflow db init || echo "Database already initialized"
airflow db upgrade

# Create admin user (if not exists)
echo "Creating admin user..."
airflow users create \
    --username "${AIRFLOW_ADMIN_USERNAME:-admin}" \
    --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
    --firstname "Admin" \
    --lastname "User" \
    --role "Admin" \
    --email "${AIRFLOW_ADMIN_EMAIL:-admin@example.com}" \
    || echo "Admin user already exists"

# Create file system connection
echo "Creating file system connection..."
airflow connections add fs_default \
    --conn-type fs \
    --conn-extra '{"path": "/opt/airflow/data"}' \
    || echo "Connection fs_default already exists"

# Create PostgreSQL connection (optional, for metadata storage)
echo "Creating PostgreSQL connection..."
airflow connections add postgres_default \
    --conn-type postgres \
    --conn-host postgres \
    --conn-port 5432 \
    --conn-schema airflow \
    --conn-login airflow \
    --conn-password airflow \
    || echo "Connection postgres_default already exists"

# Create SMTP connection for email alerts (if configured)
if [ -n "$AIRFLOW_ALERT_EMAIL_SMTP_HOST" ]; then
    echo "Creating SMTP connection..."
    airflow connections add smtp_default \
        --conn-type smtp \
        --conn-host "${AIRFLOW_ALERT_EMAIL_SMTP_HOST}" \
        --conn-port "${AIRFLOW_ALERT_EMAIL_SMTP_PORT:-587}" \
        --conn-login "${AIRFLOW_ALERT_EMAIL_USERNAME}" \
        --conn-password "${AIRFLOW_ALERT_EMAIL_PASSWORD}" \
        || echo "Connection smtp_default already exists"
fi

# Create HTTP connection for webhooks (if configured)
if [ -n "$AIRFLOW_ALERT_WEBHOOK_URL" ]; then
    echo "Creating webhook connection..."
    airflow connections add webhook_default \
        --conn-type http \
        --conn-host "${AIRFLOW_ALERT_WEBHOOK_URL}" \
        || echo "Connection webhook_default already exists"
fi

# Set Airflow variables
echo "Setting Airflow variables..."
airflow variables set pipeline_environment "${PIPELINE_ENVIRONMENT:-development}" || true
airflow variables set pipeline_log_level "${PIPELINE_LOG_LEVEL:-INFO}" || true
airflow variables set trust_layer_enabled "${TRUST_LAYER_ENABLED:-true}" || true

# Create pools for resource management
echo "Creating resource pools..."
airflow pools set pipeline_workers 4 "Pipeline worker slots" || true
airflow pools set aml_workers 2 "AML detection worker slots" || true

# Verify DAGs are detected
echo "Verifying DAGs..."
airflow dags list

echo "============================================"
echo "Airflow setup complete!"
echo "============================================"
echo ""
echo "Access the UI at: http://localhost:8080"
echo "Username: ${AIRFLOW_ADMIN_USERNAME:-admin}"
echo "Password: ${AIRFLOW_ADMIN_PASSWORD:-admin}"
echo ""

# Run the specified command
exec "$@"