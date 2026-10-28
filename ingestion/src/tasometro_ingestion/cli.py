"""Command-line entry point."""

import argparse

from tasometro_ingestion import __version__


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tasometro-ingestion",
        description="Extract SFC lending rates into the bronze layer.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return parser


def main(argv: list[str] | None = None) -> int:
    build_parser().parse_args(argv)
    return 0
