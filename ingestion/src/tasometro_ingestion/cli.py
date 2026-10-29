"""Command-line entry point.

Example::

    tasometro-ingestion ingest --source qzsc-9esp --fecha-corte 2026-10-16 \\
        --run-id manual__2026-10-16 --bronze-root data/bronze

The Socrata app token is read from ``SOCRATA_APP_TOKEN`` when set. The command
prints a JSON summary and exits 1 when any entity failed.
"""

import argparse
import json
import logging
import os
import sys
from datetime import date
from pathlib import Path

from tasometro_ingestion import __version__
from tasometro_ingestion.client import SocrataClient
from tasometro_ingestion.ingest import SOURCES, ingest_cut
from tasometro_ingestion.soql import EntityKey


def make_client(app_token: str | None) -> SocrataClient:
    return SocrataClient(app_token=app_token)


def _cut_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date {value!r}, expected YYYY-MM-DD") from exc


def _entity(value: str) -> EntityKey:
    tipo, sep, codigo = value.partition("-")
    try:
        if not sep:
            raise ValueError
        return EntityKey(tipo, codigo)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"invalid entity {value!r}, expected <tipo>-<codigo> digits, e.g. 1-7"
        ) from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tasometro-ingestion",
        description="Extract SFC lending rates into the bronze layer.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    commands = parser.add_subparsers(dest="command", required=True)

    ingest = commands.add_parser("ingest", help="extract one cut into bronze")
    ingest.add_argument("--source", required=True, choices=SOURCES, help="Socrata dataset id")
    ingest.add_argument("--fecha-corte", required=True, type=_cut_date, help="cut date")
    ingest.add_argument("--run-id", required=True, help="run identifier, used as a partition")
    ingest.add_argument("--bronze-root", required=True, type=Path, help="bronze root directory")
    ingest.add_argument(
        "--entity",
        action="append",
        type=_entity,
        help="restrict to entity <tipo>-<codigo>; repeatable (default: discover all)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    with make_client(os.environ.get("SOCRATA_APP_TOKEN") or None) as client:
        report = ingest_cut(
            client, args.bronze_root, args.source, args.fecha_corte, args.run_id, args.entity
        )

    summary = {
        "source": args.source,
        "fecha_corte": args.fecha_corte.isoformat(),
        "run_id": args.run_id,
        "entities": report.entities,
        "rate_rows": report.rate_rows,
        "zero_rate_rows": report.zero_rate_rows,
        "failed": [f"{e.tipo_entidad}-{e.codigo_entidad}" for e in report.failed],
    }
    json.dump(summary, sys.stdout)
    sys.stdout.write("\n")
    return 1 if report.failed else 0
