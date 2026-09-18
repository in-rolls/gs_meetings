"""Command line entry points for enumeration, fetching and offline parsing."""

import argparse
import json
import logging
from importlib.metadata import version as package_version
from pathlib import Path

import requests

from gs_meetings.collect import collect
from gs_meetings.datacard import write_data_card
from gs_meetings.export import export_national
from gs_meetings.feedback_collect import collect_feedback
from gs_meetings.feedback_export import export_feedback
from gs_meetings.fetch import Client
from gs_meetings.frame import enumerate_units, fetch_units
from gs_meetings.images import collect_images, export_images, plan_images
from gs_meetings.meeting_export import export_meetings
from gs_meetings.meetings import collect_meetings
from gs_meetings.parse import convert
from gs_meetings.pipeline import collect_all
from gs_meetings.source import EDITIONS
from gs_meetings.upload import deposit

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
    command = commands.add_parser("collect-all")
    command.add_argument("--root", type=Path, default=Path("data"))
    command.add_argument("--workers", type=positive, default=16)
    command.add_argument("--retries", type=positive, default=2)
    command = commands.add_parser("collect")
    command.add_argument("--root", type=Path, default=Path("data/national"))
    command.add_argument("--workers", type=positive, default=16)
    command.add_argument("--retries", type=positive, default=8)
    command.add_argument("--max-requests", type=positive)
    command = commands.add_parser("export")
    command.add_argument("--root", type=Path, default=Path("data/national"))
    command.add_argument("--allow-source-errors", action="store_true")
    command = commands.add_parser("export-meetings")
    command.add_argument("--root", type=Path, default=Path("data/meetings"))
    command.add_argument("--allow-source-errors", action="store_true")
    command = commands.add_parser("export-feedback")
    command.add_argument("--root", type=Path, default=Path("data/feedback"))
    command.add_argument("--allow-source-errors", action="store_true")
    command = commands.add_parser("collect-meetings")
    command.add_argument("--root", type=Path, default=Path("data/meetings"))
    command.add_argument("--workers", type=positive, default=4)
    command.add_argument("--retries", type=positive, default=2)
    command.add_argument("--max-requests", type=positive)
    command = commands.add_parser("collect-feedback")
    command.add_argument("--root", type=Path, default=Path("data/feedback"))
    command.add_argument("--meetings-root", type=Path, default=Path("data/meetings"))
    command.add_argument("--workers", type=positive, default=4)
    command.add_argument("--retries", type=positive, default=2)
    command.add_argument("--max-requests", type=positive)
    command.add_argument(
        "--outage-limit",
        type=float,
        default=24 * 3600,
        help="Seconds to wait for an unreachable edition; 0 records failures at once",
    )
    command = commands.add_parser(
        "collect-images",
        help="Download report photos; --dry-run queues and projects without fetching",
    )
    command.add_argument("--root", type=Path, default=Path("data/images"))
    command.add_argument("--feedback-root", type=Path, default=Path("data/feedback"))
    command.add_argument("--workers", type=positive, default=4)
    command.add_argument("--retries", type=positive, default=2)
    command.add_argument("--max-requests", type=positive)
    command.add_argument("--seed-workers", type=positive, default=4)
    command.add_argument("--dry-run", action="store_true")
    command = commands.add_parser("export-images")
    command.add_argument("--root", type=Path, default=Path("data/images"))
    command.add_argument("--allow-source-errors", action="store_true")
    command = commands.add_parser(
        "data-card",
        help="Write deposit/README_DATA.md from the exported manifests",
    )
    command.add_argument("--root", type=Path, default=Path("data"))
    command.add_argument("--schema", type=Path, default=Path("SCHEMA.md"))
    command = commands.add_parser(
        "upload",
        help="Deposit exported tables on Zenodo as a draft; --publish makes it public",
    )
    command.add_argument("--root", type=Path, default=Path("data"))
    command.add_argument("--sandbox", action="store_true")
    command.add_argument("--publish", action="store_true")
    command.add_argument("--deposition", type=positive)
    command.add_argument("--new-version", action="store_true")
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
        if args.command == "collect-all":
            report = collect_all(args.root, args.workers, args.retries)
            return int(
                any(
                    not report[name]["all_discovered_requests_succeeded"]
                    for name in ["summaries", "meetings", "feedback"]
                )
            )
        if args.command == "data-card":
            report = write_data_card(
                args.root, schema=args.schema, version=package_version("gs-meetings")
            )
            LOG.info("%s", json.dumps(report))
            return 0
        if args.command == "upload":
            report = deposit(
                args.root,
                sandbox=args.sandbox,
                publish=args.publish,
                deposition_id=args.deposition,
                new_version=args.new_version,
            )
            LOG.info("%s", json.dumps(report))
            return 0
        if args.command == "collect-images":
            if args.dry_run:
                plan = plan_images(args.root, args.feedback_root, args.seed_workers)
                LOG.info("%s", json.dumps(plan))
                return 0
            report = collect_images(
                args.root,
                args.feedback_root,
                args.workers,
                args.retries,
                args.max_requests,
                args.seed_workers,
            )
            return int(any(group["status"] != "done" for group in report["groups"]))
        if args.command == "export-images":
            LOG.info(
                "%s",
                json.dumps(
                    export_images(
                        args.root, allow_source_errors=args.allow_source_errors
                    )
                ),
            )
            return 0
        if args.command == "export-feedback":
            LOG.info(
                "%s",
                json.dumps(
                    export_feedback(
                        args.root, allow_source_errors=args.allow_source_errors
                    )
                ),
            )
            return 0
        if args.command == "collect-feedback":
            report = collect_feedback(
                args.root,
                args.meetings_root,
                args.workers,
                args.retries,
                args.max_requests,
                outage_limit=args.outage_limit,
            )
            return int(any(group["status"] != "done" for group in report["groups"]))
        if args.command == "export-meetings":
            LOG.info(
                "%s",
                json.dumps(
                    export_meetings(
                        args.root, allow_source_errors=args.allow_source_errors
                    )
                ),
            )
            return 0
        if args.command == "collect-meetings":
            report = collect_meetings(
                args.root, args.workers, args.retries, args.max_requests
            )
            return int(any(group["status"] != "done" for group in report["groups"]))
        if args.command == "export":
            LOG.info(
                "%s",
                json.dumps(
                    export_national(
                        args.root, allow_source_errors=args.allow_source_errors
                    )
                ),
            )
            return 0
        if args.command == "collect":
            report = collect(args.root, args.workers, args.retries, args.max_requests)
            return int(any(group["status"] != "done" for group in report["groups"]))
        if args.command == "parse":
            report = convert(args.root)
            if report["attendance_issues"]:
                LOG.warning(
                    "Rows with attendance inconsistencies: %s",
                    len(report["attendance_issues"]),
                )
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
