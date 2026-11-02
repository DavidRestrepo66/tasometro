"""Bronze layer writer: immutable Parquet batches plus a manifest per batch.

Layout::

    <root>/source=<dataset>/fecha_corte=<YYYY-MM-DD>/run_id=<run_id>/
        <kind>_<tipo>-<codigo>.parquet
        <kind>_<tipo>-<codigo>.manifest.json

A retry of the same run rewrites only its own files; files from other runs are
never touched, so bronze is idempotent per run and immutable across runs.
Both files are staged under unique temporary names and then renamed, data
first and manifest last. If the second rename fails, the previous files are
restored, so a manifest always describes the data next to it.

Values are stored as the raw strings returned by Socrata. Typing belongs to
silver, where it is tested.
"""

import hashlib
import json
import os
import re
import uuid
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


class Snapshot:
    """Bytes of files as they were, so a failed publication can put them back."""

    def __init__(self, contents: dict[Path, bytes | None]) -> None:
        self._contents = contents

    @classmethod
    def take(cls, paths: Sequence[Path]) -> "Snapshot":
        return cls({p: p.read_bytes() if p.exists() else None for p in paths})

    def restore(self) -> None:
        """Rewrite the saved bytes, or remove files that did not exist before."""
        for path, content in self._contents.items():
            if content is None:
                path.unlink(missing_ok=True)
                continue
            tmp = _tmp_name(path)
            try:
                tmp.write_bytes(content)
                os.replace(tmp, path)
            finally:
                tmp.unlink(missing_ok=True)


def batch_paths(root: Path, batch: BronzeBatch) -> tuple[Path, Path]:
    """Return the (data, manifest) paths ``write_batch`` uses for ``batch``."""
    directory = (
        root
        / f"source={batch.source}"
        / f"fecha_corte={batch.fecha_corte.isoformat()}"
        / f"run_id={batch.run_id}"
    )
    stem = f"{batch.kind}_{batch.entity.tipo_entidad}-{batch.entity.codigo_entidad}"
    return directory / f"{stem}.parquet", directory / f"{stem}.manifest.json"


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

    data_path, manifest_path = batch_paths(root, batch)
    data_path.parent.mkdir(parents=True, exist_ok=True)

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
        "extracted_at": datetime.now(UTC).isoformat(timespec="microseconds"),
        "ingestion_version": __version__,
    }
    payload = json.dumps(manifest, ensure_ascii=False, indent=2) + "\n"

    # Stage both files first: a failure while writing leaves the previous batch
    # untouched. Only the two renames remain, data first and manifest last; if
    # the second one fails the previous files are put back, so the manifest on
    # disk always describes the data next to it.
    data_tmp = _tmp_name(data_path)
    manifest_tmp = _tmp_name(manifest_path)
    snapshot = Snapshot.take([data_path, manifest_path])
    try:
        frame.write_parquet(data_tmp)
        manifest_tmp.write_text(payload, encoding="utf-8")
        os.replace(data_tmp, data_path)
        os.replace(manifest_tmp, manifest_path)
    except BaseException:
        snapshot.restore()
        raise
    finally:
        data_tmp.unlink(missing_ok=True)
        manifest_tmp.unlink(missing_ok=True)

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


def _tmp_name(target: Path) -> Path:
    return target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
