#!/usr/bin/env bash
# Runs once, on the first start of an empty data volume. Creates one database
# per concern on the single Postgres instance: Airflow metadata, the
# warehouse, and Metabase's application database.
set -euo pipefail

for db in "$AIRFLOW_DB" "$WAREHOUSE_DB" "$METABASE_DB"; do
    echo "creating database $db"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
        -c "CREATE DATABASE \"$db\""
done
