# Tasometro

How much does each Colombian bank charge for each type of credit, compared with
the Banco de la República monetary policy rate (TPM)? A second question: do
rates differ by debtor sex or company size?

Tasometro is a data-engineering project: an analytical lakehouse over the
lending rates the Superintendencia Financiera de Colombia (SFC) publishes
weekly on [datos.gov.co](https://www.datos.gov.co).

## Architecture

```
Socrata (SFC) ──▶ bronze (Parquet) ──▶ raw (Postgres) ──▶ silver ──▶ gold ──▶ Metabase
   SoQL            per run_id            delete+insert       dbt       star schema
                                                                       + weekly marts
                         orchestrated by one weekly Airflow DAG
```

Stack: Python · Polars · Parquet · PostgreSQL · dbt-core · Apache Airflow ·
Metabase · Docker Compose · GitHub Actions.

## Key decisions

- **Aggregate at the source.** Each cut has about 900k rows; SoQL returns the sums needed to rebuild amount-weighted rates, about 1,250 rows per week.
- **Two queries per cut and entity.** `rates_query` and `zero_rate_query` are small requests that retry independently and map to Airflow tasks.
- **Idempotent bronze.** Batches are written under `source=/fecha_corte=/run_id=` with a manifest; a retry overwrites only its own run.
- **Spread against the weekly TPM.** The policy rate is averaged over the seven days of the cut, and weeks with a rate change are flagged.
- **Revision audit.** Banks revise recent weeks, so the last eight cuts are re-extracted and an audit model warns when published numbers changed.
- **Sex gap within bank and product.** Rates are compared only inside the same bank, product and term, never across them.

## Quick start

Requires Docker, [uv](https://docs.astral.sh/uv/) and a copy of the settings:

```bash
cp .env.example .env     # fill in the passwords and the Airflow keys it describes
docker compose up -d --build
```

| Service | Default URL |
|---|---|
| Airflow | http://localhost:8080 |
| Metabase | http://localhost:3000 |
| Postgres | localhost:5432 (databases `airflow`, `warehouse`, `metabase`) |

The Airflow DAG `tasometro_semanal` is created paused. A manual run extracts
one cut and the seven before it, loads them and runs `dbt build`. Unpausing it
starts a catch-up from 2022-07, the full history (about 223 weekly positions; the number of published cuts depends on the dataset).
The stack is for local use: every Airflow user is an admin.

Build the dashboard once the warehouse has data:

```bash
set -a; . ./.env; set +a
uv run --project ingestion python metabase/provision.py
```

The script is idempotent. It completes the Metabase setup, registers the
warehouse and creates four questions with filters for credit type, term, date
and entity.

## Running pieces by hand

```bash
cd ingestion
uv run tasometro-ingestion ingest --source qzsc-9esp --fecha-corte 2026-10-16 \
    --run-id manual --bronze-root ../data/bronze
uv run tasometro-ingestion load --bronze-root ../data/bronze
uv run tasometro-ingestion catalog
cd ../dbt && uv run dbt build --profiles-dir .
```

`dbt` reads its connection from `WAREHOUSE_HOST`, `WAREHOUSE_PORT`,
`WAREHOUSE_USER`, `WAREHOUSE_PASSWORD` and `WAREHOUSE_DB`.

## Data sources and limits

| Dataset | Use |
|---|---|
| `w9zh-vetq` | SFC rates, history since 2022-07 |
| `qzsc-9esp` | SFC rates, recent months, used for weekly loads |
| `sr9n-792w` | Supervised-entity catalog |
| TPM seed | `dbt/seeds/tpm_cambios.csv`, one row per BanRep rate change |

- The TPM is a **manually maintained seed**. A freshness test fails when a cut is newer than the date set in `tpm_revisado_hasta`; review the seed against BanRep and bump that variable.
- A rate of 0 is treated as missing. Spread uses only fixed and DTF/IBR-indexed rates; UVR and IPC rates are excluded and other indexes are flagged.
- Sex is reported only for natural persons, and rates are source averages, so differences are descriptive, not causal.

## Repository layout

| Path | Contents |
|---|---|
| `ingestion/` | Python package: SoQL extraction, bronze writer, warehouse loader |
| `dbt/` | Seeds, staging, silver, gold, audit models and tests; `ci/` holds the two-cut fixture |
| `airflow/dags/` | The weekly DAG |
| `docker/` | Airflow image and Postgres initialisation |
| `metabase/` | Provisioning script for the connection and dashboard |
| `.github/workflows/` | CI: ruff, pytest, `dbt build` on the fixture |

## License

[MIT](LICENSE)
