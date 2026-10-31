#!/usr/bin/env bash
# Creates the raw schema with the ingestion package's DDL and loads the CSV
# fixture, so `dbt build` can run without Socrata or Airflow. Connection comes
# from the standard PG* variables.
set -euo pipefail
cd "$(dirname "$0")/../.."

uv run --project ingestion python -c "
import psycopg
from tasometro_ingestion.warehouse import ensure_schema
with psycopg.connect('') as conn:
    ensure_schema(conn)
"

for table in sfc_lotes sfc_tasas sfc_tasas_cero sfc_entidades; do
  psql -v ON_ERROR_STOP=1 -c "\\copy raw.$table from 'dbt/ci/fixture/$table.csv' with csv header"
done
