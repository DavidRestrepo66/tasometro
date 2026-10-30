"""Bronze layer writer: immutable Parquet batches plus a manifest per batch.

Layout::

    <root>/source=<dataset>/fecha_corte=<YYYY-MM-DD>/run_id=<run_id>/
        <kind>_<tipo>-<codigo>.parquet
        <kind>_<tipo>-<codigo>.manifest.json

A retry of the same run rewrites only its own files; files from other runs are
never touched, so bronze is idempotent per run and immutable across runs.
Both files are written to a temporary name and atomically renamed, and the
manifest is withdrawn before a retry replaces the data and written last, so
a manifest always marks a complete batch that matches its data.

Values are stored as the raw strings returned by Socrata. Typing belongs to
silver, where it is tested.
"""

import hashlib
import json
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

import polars as pl

from tasometro_ingestion import __version__
from tasometro_ingestion.soql import RATE_COLUMNS, ZERO_RATE_COLUMNS, EntityKey

_SAFE_RUN_ID = re.compile(r"[A-Za-z0-9._:+-]+")


class BatchKind(StrEnum):
    RATES = "rates"
    ZERO_RATES = "zero_rates"


_SCHEMAS: dict[BatchKind, tuple[str, ...]] = {
    BatchKind.RATES: RATE_COLUMNS,
    BatchKind.ZERO_RATES: ZERO_RATE_COLUMNS,
}


@dataclass(frozen=True, slots=True)
class BronzeBatch:
    """Result of one query for one (source, cut, entity) within one run."""

    source: str
    fecha_corte: date
    run_id: str
    entity: EntityKey
    kind: BatchKind
    query: Mapping[str, str]
    rows: Sequence[Mapping[str, Any]]


@dataclass(frozen=True, slots=True)
class WrittenBatch:
    data_path: Path
    manifest_path: Path
    row_count: int
    rows_sha256: str


def validate_run_id(run_id: str) -> str:
    """Return ``run_id`` if it is safe as a path component, else raise ValueError."""
    if run_id in {".", ".."} or not _SAFE_RUN_ID.fullmatch(run_id):
        raise ValueError(f"unsafe run_id for a path component: {run_id!r}")
    return run_id


def write_batch(root: Path, batch: BronzeBatch) -> WrittenBatch:
    """Write ``batch`` under ``root`` and return where it landed."""
    validate_run_id(batch.run_id)

    validate_rows(batch.kind, batch.rows)
    columns = _SCHEMAS[batch.kind]
    frame = pl.DataFrame(
        [[row.get(c) for c in columns] for row in batch.rows],
        schema=dict.fromkeys(columns, pl.String),
        orient="row",
    )
    rows_sha256 = rows_hash(frame)

    directory = (
        root
        / f"source={batch.source}"
        / f"fecha_corte={batch.fecha_corte.isoformat()}"
        / f"run_id={batch.run_id}"
    )
    directory.mkdir(parents=True, exist_ok=True)
    stem = f"{batch.kind}_{batch.entity.tipo_entidad}-{batch.entity.codigo_entidad}"
    data_path = directory / f"{stem}.parquet"
    manifest_path = directory / f"{stem}.manifest.json"

    # Withdraw the previous manifest first: while the data file is replaced, a
    # manifest must never describe data it no longer matches.
    manifest_path.unlink(missing_ok=True)
    _atomic_write(data_path, lambda tmp: frame.write_parquet(tmp))
    manifest = {
        "source": batch.source,
        "fecha_corte": batch.fecha_corte.isoformat(),
        "run_id": batch.run_id,
        "tipo_entidad": batch.entity.tipo_entidad,
        "codigo_entidad": batch.entity.codigo_entidad,
        "kind": str(batch.kind),
        "data_file": data_path.name,
        "row_count": frame.height,
        "rows_sha256": rows_sha256,
        "soql": dict(batch.query),
        "extracted_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "ingestion_version": __version__,
    }
    payload = json.dumps(manifest, ensure_ascii=False, indent=2) + "\n"
    _atomic_write(manifest_path, lambda tmp: tmp.write_text(payload, encoding="utf-8"))

    return WrittenBatch(data_path, manifest_path, frame.height, rows_sha256)


def validate_rows(kind: BatchKind, rows: Sequence[Mapping[str, Any]]) -> None:
    """Raise ValueError when ``rows`` carry columns outside the schema of ``kind``."""
    _check_columns(rows, _SCHEMAS[kind])


def _check_columns(rows: Sequence[Mapping[str, Any]], columns: tuple[str, ...]) -> None:
    unexpected = sorted({key for row in rows for key in row} - set(columns))
    if unexpected:
        raise ValueError(f"schema drift, unexpected columns: {unexpected}")


def rows_hash(frame: pl.DataFrame) -> str:
    """Order-independent content hash, comparable across extractions of a cut."""
    lines = sorted(
        json.dumps(row, ensure_ascii=False, separators=(",", ":")) for row in frame.iter_rows()
    )
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()


def _atomic_write(target: Path, write: Any) -> None:
    tmp = target.with_name(f".{target.name}.tmp")
    try:
        write(tmp)
        os.replace(tmp, target)
    finally:
        tmp.unlink(missing_ok=True)
