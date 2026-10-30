"""Load bronze batches and the entity catalog into the warehouse ``raw`` schema.

``raw`` mirrors bronze: every value stays a string, typing belongs to dbt.
Each bronze batch is loaded in its own transaction: the rows of that exact
(source, cut, run, entity, kind) are deleted and re-inserted, and the manifest
is upserted into ``raw.sfc_lotes``. A batch whose manifest was already loaded
unchanged is skipped, so reloading the whole bronze tree is cheap and safe.

Only batches with a manifest are complete (the manifest is written last), so
a data file without one is ignored. A batch whose row count or content hash
disagrees with its manifest stops the load before anything of it is written.
"""

import json
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import psycopg
from psycopg import sql

from tasometro_ingestion.bronze import BatchKind, rows_hash
from tasometro_ingestion.ingest import RowFetcher
from tasometro_ingestion.soql import RATE_COLUMNS, ZERO_RATE_COLUMNS

CATALOG_DATASET = "sr9n-792w"
# Only the key, the legal name and the NIT. The catalog also publishes people's
# names, e-mails and addresses, which the project has no use for.
CATALOG_COLUMNS: tuple[str, ...] = (
    "tipo_entidad",
    "cod_entidad",
    "razon_social",
    "numeroidentificacion",
)

_BATCH_KEY = ("_source", "_fecha_corte", "_run_id", "_tipo_entidad", "_codigo_entidad")
_TABLES: dict[BatchKind, tuple[str, tuple[str, ...]]] = {
    BatchKind.RATES: ("sfc_tasas", RATE_COLUMNS),
    BatchKind.ZERO_RATES: ("sfc_tasas_cero", ZERO_RATE_COLUMNS),
}


class LoadError(RuntimeError):
    """A bronze batch is incomplete or does not match its manifest."""


@dataclass(slots=True)
class LoadReport:
    loaded: int = 0
    skipped: int = 0
    rows: int = 0


def ensure_schema(conn: psycopg.Connection[Any]) -> None:
    """Create the raw schema and tables when missing."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.sfc_lotes (
            source text NOT NULL,
            fecha_corte date NOT NULL,
            run_id text NOT NULL,
            tipo_entidad text NOT NULL,
            codigo_entidad text NOT NULL,
            kind text NOT NULL,
            row_count integer NOT NULL,
            rows_sha256 text NOT NULL,
            soql jsonb NOT NULL,
            extracted_at timestamptz NOT NULL,
            ingestion_version text NOT NULL,
            loaded_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (source, fecha_corte, run_id, tipo_entidad, codigo_entidad, kind)
        )
        """
    )
    for table, columns in _TABLES.values():
        conn.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS raw.{} ("
                "_source text NOT NULL, _fecha_corte date NOT NULL, _run_id text NOT NULL,"
                " _tipo_entidad text NOT NULL, _codigo_entidad text NOT NULL,"
                " _extracted_at timestamptz NOT NULL, {})"
            ).format(
                sql.Identifier(table),
                sql.SQL(", ").join(sql.SQL("{} text").format(sql.Identifier(c)) for c in columns),
            )
        )
        conn.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON raw.{} ({})").format(
                sql.Identifier(f"{table}_batch_idx"),
                sql.Identifier(table),
                sql.SQL(", ").join(map(sql.Identifier, _BATCH_KEY)),
            )
        )
    conn.execute(
        sql.SQL(
            "CREATE TABLE IF NOT EXISTS raw.sfc_entidades ({}, _extracted_at timestamptz NOT NULL)"
        ).format(
            sql.SQL(", ").join(
                sql.SQL("{} text").format(sql.Identifier(c)) for c in CATALOG_COLUMNS
            )
        )
    )


def load_bronze(
    dsn: str,
    bronze_root: Path,
    source: str | None = None,
    fecha_corte: date | None = None,
) -> LoadReport:
    """Load every complete batch under ``bronze_root`` that changed since last load."""
    report = LoadReport()
    with psycopg.connect(dsn) as conn:
        ensure_schema(conn)
        conn.commit()
        for manifest_path in _manifests(bronze_root, source, fecha_corte):
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if _already_loaded(conn, manifest):
                report.skipped += 1
                continue
            frame = _read_checked(manifest_path, manifest)
            with conn.transaction():
                _replace_batch(conn, manifest, frame)
            conn.commit()  # explicit: a later failure must not roll this batch back
            report.loaded += 1
            report.rows += frame.height
    return report


def load_catalog(dsn: str, client: RowFetcher) -> int:
    """Replace ``raw.sfc_entidades`` with the current supervised-entity catalog."""
    rows = client.fetch_all(
        CATALOG_DATASET,
        {"$select": ",".join(CATALOG_COLUMNS), "$order": "tipo_entidad,cod_entidad"},
    )
    with psycopg.connect(dsn) as conn:
        ensure_schema(conn)
        with conn.transaction():
            conn.execute("DELETE FROM raw.sfc_entidades")
            columns = sql.SQL(", ").join(map(sql.Identifier, (*CATALOG_COLUMNS, "_extracted_at")))
            copy_sql = sql.SQL("COPY raw.sfc_entidades ({}) FROM STDIN").format(columns)
            with conn.cursor().copy(copy_sql) as copy:
                for row in rows:
                    copy.write_row([*(row.get(c) for c in CATALOG_COLUMNS), "now"])
    return len(rows)


def _manifests(root: Path, source: str | None, fecha_corte: date | None) -> Iterator[Path]:
    source_part = f"source={source}" if source else "source=*"
    cut_part = f"fecha_corte={fecha_corte.isoformat()}" if fecha_corte else "fecha_corte=*"
    yield from sorted(root.glob(f"{source_part}/{cut_part}/run_id=*/*.manifest.json"))


def _already_loaded(conn: psycopg.Connection[Any], manifest: dict[str, Any]) -> bool:
    row = conn.execute(
        "SELECT rows_sha256, extracted_at = %(extracted_at)s::timestamptz FROM raw.sfc_lotes"
        " WHERE source = %(source)s AND fecha_corte = %(fecha_corte)s AND run_id = %(run_id)s"
        " AND tipo_entidad = %(tipo_entidad)s AND codigo_entidad = %(codigo_entidad)s"
        " AND kind = %(kind)s",
        manifest,
    ).fetchone()
    conn.commit()
    return row is not None and row[0] == manifest["rows_sha256"] and bool(row[1])


def _read_checked(manifest_path: Path, manifest: dict[str, Any]) -> pl.DataFrame:
    data_path = manifest_path.with_name(manifest["data_file"])
    if not data_path.is_file():
        raise LoadError(f"missing data file for {manifest_path}")
    try:
        frame = pl.read_parquet(data_path)
    except (OSError, pl.exceptions.PolarsError) as exc:
        raise LoadError(f"unreadable data file {data_path}: {exc}") from exc
    _, columns = _TABLES[BatchKind(manifest["kind"])]
    if tuple(frame.columns) != columns:
        raise LoadError(f"unexpected columns in {data_path}: {frame.columns}")
    if frame.height != manifest["row_count"]:
        raise LoadError(
            f"row count mismatch in {data_path}: {frame.height} != {manifest['row_count']}"
        )
    if rows_hash(frame) != manifest["rows_sha256"]:
        raise LoadError(f"hash mismatch in {data_path}")
    return frame


def _replace_batch(
    conn: psycopg.Connection[Any], manifest: dict[str, Any], frame: pl.DataFrame
) -> None:
    table, columns = _TABLES[BatchKind(manifest["kind"])]
    key = (
        manifest["source"],
        manifest["fecha_corte"],
        manifest["run_id"],
        manifest["tipo_entidad"],
        manifest["codigo_entidad"],
    )
    conn.execute(
        sql.SQL("DELETE FROM raw.{} WHERE {}").format(
            sql.Identifier(table),
            sql.SQL(" AND ").join(sql.SQL("{} = %s").format(sql.Identifier(c)) for c in _BATCH_KEY),
        ),
        key,
    )
    all_columns = (*_BATCH_KEY, "_extracted_at", *columns)
    copy_sql = sql.SQL("COPY raw.{} ({}) FROM STDIN").format(
        sql.Identifier(table), sql.SQL(", ").join(map(sql.Identifier, all_columns))
    )
    with conn.cursor().copy(copy_sql) as copy:
        for row in frame.iter_rows():
            copy.write_row([*key, manifest["extracted_at"], *row])

    conn.execute(
        """
        INSERT INTO raw.sfc_lotes (source, fecha_corte, run_id, tipo_entidad, codigo_entidad,
            kind, row_count, rows_sha256, soql, extracted_at, ingestion_version)
        VALUES (%(source)s, %(fecha_corte)s, %(run_id)s, %(tipo_entidad)s, %(codigo_entidad)s,
            %(kind)s, %(row_count)s, %(rows_sha256)s, %(soql)s, %(extracted_at)s,
            %(ingestion_version)s)
        ON CONFLICT (source, fecha_corte, run_id, tipo_entidad, codigo_entidad, kind)
        DO UPDATE SET row_count = EXCLUDED.row_count, rows_sha256 = EXCLUDED.rows_sha256,
            soql = EXCLUDED.soql, extracted_at = EXCLUDED.extracted_at,
            ingestion_version = EXCLUDED.ingestion_version, loaded_at = now()
        """,
        {**manifest, "soql": json.dumps(manifest["soql"], ensure_ascii=False)},
    )
