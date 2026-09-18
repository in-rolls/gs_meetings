"""Queue public facilitator reports and retain their links to dated source rows."""

import fcntl
import hashlib
import json
import sqlite3
from contextlib import closing
from functools import partial
from pathlib import Path
from urllib.parse import urlencode

from gs_meetings.collect import add_request, open_queue, run_queue
from gs_meetings.feedback import FORM_ABSENT, parse_feedback
from gs_meetings.fetch import Client, atomic_json, read_capture
from gs_meetings.meetings import parse_meetings, validate_meeting_rows
from gs_meetings.source import EDITIONS


def feedback_url(record: dict) -> str | None:
    """Use each edition's published controller or village-profile link format."""
    edition = record["edition"]
    raw_date = record["meeting_date_raw"]
    if edition != "PPC2018" and not raw_date:
        return None
    if edition == "current":
        params = {
            "lbCode": record["local_body_code"],
            "stateCode": record["state_code"],
            "date": raw_date,
        }
    else:
        hierarchy = json.loads(record["hierarchy"])
        names = {row["level"]: row["name"] for row in hierarchy}
        params = {
            "gpCode": record["local_body_code"],
            "dpName": names.get("D", "null"),
            "bpName": names.get("I", "null"),
            "gpName": record["local_body_name"],
        }
        if edition != "PPC2018":
            params["date"] = raw_date
        if edition in {"PPC", "PPC2020"}:
            params["meetingType"] = (
                record["meeting_type"] or record["requested_meeting_type"] or "null"
            )
    return f"https://gpdp.nic.in/{EDITIONS[edition]}facilitatorFeedbackDetails.html?{urlencode(params)}"


def seed_feedback(root: Path, *, meetings_root: Path):
    """Add newly completed dated listings without rereading already seeded reports."""
    if not (meetings_root / "collection.sqlite").is_file():
        raise ValueError("No dated meeting queue found")
    db = open_queue(root, seed_summaries=False, terminal_errors=(FORM_ABSENT,))
    db.execute("CREATE TABLE IF NOT EXISTS seeded_reports (url TEXT PRIMARY KEY)")
    db.execute(
        "CREATE TABLE IF NOT EXISTS feedback_links "
        "(meeting_url TEXT, row_ordinal INTEGER, "
        "feedback_url TEXT, expected_date TEXT, expected_type TEXT, issue TEXT, "
        "PRIMARY KEY(meeting_url,row_ordinal))"
    )
    metadata = root / "source.json"
    source = {"meetings_root": str(meetings_root.resolve())}
    if metadata.exists() and json.loads(metadata.read_text()) != source:
        db.close()
        raise ValueError("Feedback queue belongs to a different dated snapshot")
    atomic_json(metadata, source)
    try:
        seeded = {row[0] for row in db.execute("SELECT url FROM seeded_reports")}
        with closing(sqlite3.connect(meetings_root / "collection.sqlite")) as source_db:
            source_db.row_factory = sqlite3.Row
            tasks = source_db.execute(
                "SELECT * FROM requests WHERE status='done' ORDER BY url"
            ).fetchall()
        for task in tasks:
            if task["url"] in seeded:
                continue
            key = hashlib.sha256(task["url"].encode()).hexdigest()
            capture = read_capture(
                meetings_root / "raw" / task["edition"] / f"{key}.jsonl.gz",
                validate_meeting_rows,
            )
            if capture is None or capture["url"] != task["url"]:
                raise ValueError(
                    "Missing or damaged meeting capture while seeding feedback"
                )
            rows = json.loads(capture["body"])
            if len(rows) != task["rows"]:
                raise ValueError("Meeting capture row count differs from queue")
            if rows and any(
                "gram_sabha_date" in row or "gramSabhaDate" in row for row in rows
            ):
                records = parse_meetings(
                    rows,
                    json.loads(task["context"]),
                    task["url"],
                    capture["fetched_at"],
                )
                for record in records:
                    url = feedback_url(record)
                    if url is not None:
                        context = {
                            key: record[key]
                            for key in ["edition", "state_code", "local_body_code"]
                        }
                        add_request(db, task["edition"], "gp", context, url=url)
                    db.execute(
                        "INSERT OR REPLACE INTO feedback_links VALUES (?,?,?,?,?,?)",
                        (
                            task["url"],
                            record["row_ordinal"],
                            url,
                            record["meeting_date_raw"],
                            record["meeting_type"] or record["requested_meeting_type"],
                            "missing_date" if url is None else None,
                        ),
                    )
            db.execute("INSERT INTO seeded_reports VALUES (?)", (task["url"],))
            db.commit()
        return db
    except Exception:
        db.close()
        raise


def no_children(_db, _task, _rows) -> None:
    """Facilitator reports are leaves of the collection graph."""


def collect_feedback(
    root: Path,
    meetings_root: Path,
    workers=4,
    retries=2,
    max_requests=None,
    outage_limit: float = 24 * 3600,
) -> dict:
    """Collect newly discovered feedback; rerun after dated collection finishes."""
    root.mkdir(parents=True, exist_ok=True)
    with (root / "collection.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return run_queue(
            root,
            workers,
            retries,
            max_requests,
            initialize=partial(seed_feedback, meetings_root=meetings_root),
            expand=no_children,
            client_factory=partial(Client, parser=parse_feedback),
            outage_limit=outage_limit,
        )
