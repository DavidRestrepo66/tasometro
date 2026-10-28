# Tasometro

How much does each Colombian bank charge for each type of credit, compared with
the Banco de la República monetary policy rate?

Tasometro is a data-engineering project that builds an analytical lakehouse
over the lending rates published weekly by the Superintendencia Financiera de
Colombia (SFC).

> Work in progress.

## Stack

Python · Polars · Parquet · PostgreSQL · dbt-core · Apache Airflow · Metabase ·
Docker Compose · GitHub Actions

## Repository layout

| Path | Contents |
|---|---|
| `ingestion/` | Python package: SoQL extraction to the Parquet bronze layer |

## License

[MIT](LICENSE)
