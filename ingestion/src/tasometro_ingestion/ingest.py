"""Ingest one cut of one source into bronze, one (cut, entity) unit at a time.

Each entity is an independent unit: its two queries run first and its two
files are written only when both succeeded, so a failed entity leaves no
partial batch behind. Failures are collected and reported instead of
aborting the cut, so one slow entity does not discard the rest of the run.
"""

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Protocol

from tasometro_ingestion.bronze import BatchKind, BronzeBatch, validate_rows, write_batch
from tasometro_ingestion.soql import EntityKey, entities_query, rates_query, zero_rate_query

log = logging.getLogger(__name__)

SOURCES: tuple[str, ...] = ("w9zh-vetq", "qzsc-9esp")


class RowFetcher(Protocol):
    def fetch_all(self, dataset: str, query: Mapping[str, str]) -> list[dict[str, Any]]: ...


@dataclass(slots=True)
class IngestReport:
    entities: int = 0
    rate_rows: int = 0
    zero_rate_rows: int = 0
    failed: list[EntityKey] = field(default_factory=list)


def discover_entities(client: RowFetcher, source: str, fecha_corte: date) -> list[EntityKey]:
    """Entities that reported rates in the cut, in numeric key order."""
    rows = client.fetch_all(source, entities_query(fecha_corte))
    keys = {EntityKey(row["tipo_entidad"], row["codigo_entidad"]) for row in rows}
    return sorted(keys, key=lambda k: (int(k.tipo_entidad), int(k.codigo_entidad)))


def ingest_cut(
    client: RowFetcher,
    bronze_root: Path,
    source: str,
    fecha_corte: date,
    run_id: str,
    entities: Sequence[EntityKey] | None = None,
) -> IngestReport:
    """Extract every entity of a cut into bronze and summarize the outcome."""
    if entities is None:
        entities = discover_entities(client, source, fecha_corte)

    report = IngestReport()
    for entity in entities:
        queries = {
            BatchKind.RATES: rates_query(fecha_corte, entity),
            BatchKind.ZERO_RATES: zero_rate_query(fecha_corte, entity),
        }
        try:
            results = {kind: client.fetch_all(source, q) for kind, q in queries.items()}
        except Exception:
            log.exception("extraction failed for entity %s-%s", *_key(entity))
            report.failed.append(entity)
            continue

        try:
            # Validate every batch before writing any, so schema drift in one
            # batch cannot leave the other one published.
            for kind, rows in results.items():
                validate_rows(kind, rows)
            for kind, rows in results.items():
                write_batch(
                    bronze_root,
                    BronzeBatch(source, fecha_corte, run_id, entity, kind, queries[kind], rows),
                )
        except (OSError, ValueError):
            # A batch without its manifest is incomplete and ignored by the loader.
            log.exception("write failed for entity %s-%s", *_key(entity))
            report.failed.append(entity)
            continue
        report.entities += 1
        report.rate_rows += len(results[BatchKind.RATES])
        report.zero_rate_rows += len(results[BatchKind.ZERO_RATES])
        log.info("entity %s-%s: %d rate rows", *_key(entity), len(results[BatchKind.RATES]))
    return report


def _key(entity: EntityKey) -> tuple[str, str]:
    return entity.tipo_entidad, entity.codigo_entidad
