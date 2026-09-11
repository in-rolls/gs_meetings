"""Command line entry points for enumeration, fetching and offline parsing."""

import argparse
import json
import logging
from pathlib import Path

import requests

from gs_meetings.fetch import Client
from gs_meetings.frame import enumerate_units, fetch_units
from gs_meetings.parse import convert
from gs_meetings.source import EDITIONS

LOG = logging.getLogger(__name__)


def positive(value: str) -> int:
    """Reject nonpositive scope and retry arguments."""
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("Must be positive")
    return number


def main(argv: list[str] | None = None) -> int:
    """Run a bounded collection stage and report failures to the caller."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ["list", "fetch", "parse"]:
        command = commands.add_parser(name)
        command.add_argument("--root", type=Path, default=Path("data/current"))
        if name != "parse":
            command.add_argument("--edition", choices=EDITIONS, default="current")
            command.add_argument("--retries", type=positive, default=4)
        if name == "list":
            command.add_argument("--state", type=positive, required=True)
            command.add_argument("--district", type=positive)
        if name == "fetch":
            command.add_argument("--limit", type=positive)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        if args.command == "parse":
            report = convert(args.root)
            LOG.info(
                "rows=%s missing_units=%s empty_units=%s",
                report["rows"],
                len(report["missing_units"]),
                len(report["empty_units"]),
            )
            return int(bool(report["missing_units"] or report["empty_units"]))
        client = Client(args.root, args.edition, args.retries)
        try:
            if args.command == "list":
                units = enumerate_units(client, args.state, args.district)
                LOG.info("Enumerated %s GP report requests", len(units))
                return 0
            report = fetch_units(client, args.root / "frame.parquet", args.limit)
            LOG.info("%s", json.dumps(report))
            return int(bool(report["failures"]))
        finally:
            client.close()
    except (requests.RequestException, ValueError, OSError) as exc:
        LOG.error("%s", exc)
        return 1
