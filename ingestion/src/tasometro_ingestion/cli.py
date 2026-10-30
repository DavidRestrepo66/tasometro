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
from tasometro_ingestion.bronze import validate_run_id
from tasometro_ingestion.client import SocrataClient
from tasometro_ingestion.ingest import SOURCES, ingest_cut
from tasometro_ingestion.soql import EntityKey
from tasometro_ingestion.warehouse import LoadError, load_bronze, load_catalog


def make_client(app_token: str | None) -> SocrataClient:
    return SocrataClient(app_token=app_token)


def _run_id(value: str) -> str:
    try:
        return validate_run_id(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


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
    ingest.add_argument(
        "--run-id", required=True, type=_run_id, help="run identifier, used as a partition"
    )
    ingest.add_argument("--bronze-root", required=True, type=Path, help="bronze root directory")
    ingest.add_argument(
        "--entity",
        action="append",
        type=_entity,
        help="restrict to entity <tipo>-<codigo>; repeatable (default: discover all)",
    )

    load = commands.add_parser(
        "load", help="load complete bronze batches into the warehouse raw schema"
    )
    load.add_argument("--bronze-root", required=True, type=Path, help="bronze root directory")
    load.add_argument("--source", choices=SOURCES, help="only this dataset (default: all)")
    load.add_argument("--fecha-corte", type=_cut_date, help="only this cut (default: all)")

    commands.add_parser("catalog", help="replace the supervised-entity catalog (sr9n-792w)")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.command == "ingest":
        return _ingest(args)
    dsn = os.environ.get("WAREHOUSE_DSN")
    if not dsn:
        parser.error("WAREHOUSE_DSN is not set")
    if args.command == "load":
        try:
            report = load_bronze(dsn, args.bronze_root, args.source, args.fecha_corte)
        except LoadError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        _print({"loaded": report.loaded, "skipped": report.skipped, "rows": report.rows})
        return 0
    with make_client(os.environ.get("SOCRATA_APP_TOKEN") or None) as client:
        count = load_catalog(dsn, client)
    _print({"entities": count})
    return 0


def _print(summary: dict[str, object]) -> None:
    json.dump(summary, sys.stdout)
    sys.stdout.write("\n")


def _ingest(args: argparse.Namespace) -> int:
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
    _print(summary)
    return 1 if report.failed else 0
