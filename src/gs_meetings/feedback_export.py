"""Export facilitator reports and checked links to the dated meeting listings."""

import hashlib
import json
import sqlite3
from collections import Counter
from contextlib import ExitStack, closing
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from gs_meetings.feedback import ATTENDANCE, BOOLEAN_QUESTIONS, parse_feedback
from gs_meetings.fetch import atomic_json, read_capture

SCHEMAS = {
    "feedback": pa.schema(
        [
            (name, pa.string())
            for name in [
                "source_url",
                "edition",
                "state_code",
                "local_body_code",
                "meeting_date_raw",
                "date_error",
                "feedback_type",
                "document_links",
                "images",
            ]
        ]
        + [
            ("meeting_date", pa.date32()),
            ("fetched_at", pa.timestamp("us", tz="UTC")),
            ("report_available", pa.bool_()),
            ("attendance_inconsistent", pa.bool_()),
        ]
        + [(name, pa.int64()) for name in ATTENDANCE.values()]
        + [(name, pa.bool_()) for name in BOOLEAN_QUESTIONS.values()]
    ),
    "feedback_answers": pa.schema(
        [
            ("source_url", pa.string()),
            ("answer_ordinal", pa.int64()),
            ("question", pa.string()),
            ("text", pa.string()),
            ("boolean", pa.bool_()),
        ]
    ),
    "feedback_tables": pa.schema(
        [
            ("source_url", pa.string()),
            ("row_ordinal", pa.int64()),
            ("headers", pa.list_(pa.string())),
            (
                "cells",
                pa.list_(pa.struct([("text", pa.string()), ("boolean", pa.bool_())])),
            ),
        ]
    ),
    "feedback_links": pa.schema(
        [
            (name, pa.string())
            for name in [
                "meeting_url",
                "feedback_url",
                "expected_date",
                "expected_type",
                "issue",
                "request_status",
                "reported_date",
                "reported_type",
            ]
        ]
        + [
            ("row_ordinal", pa.int64()),
            ("date_match", pa.bool_()),
            ("type_match", pa.bool_()),
            ("same_report_for_multiple_rows", pa.bool_()),
        ]
    ),
}


def matching_type(expected: str | None, observed: str | None) -> bool | None:
    """Compare only documented type labels, leaving missing or unknown labels null."""
    lookup = {"S": "sabha", "M": "meeting", "Sabha": "sabha", "Meeting": "meeting"}
    expected = lookup.get(expected)
    observed = lookup.get(observed)
    return (
        expected == observed if expected is not None and observed is not None else None
    )


def matching_date(expected: str | None, observed: str | None) -> bool | None:
    """Compare dates without treating display padding as a disagreement."""
    if not expected or not observed:
        return None
    try:
        dates = []
        for value in [expected, observed]:
            day, month, year = (int(part) for part in value.split("-"))
            dates.append(date(year, month, day))
        return dates[0] == dates[1]
    except ValueError:
        return None


def export_feedback(root: Path, *, allow_source_errors: bool = False) -> dict:
    """Export a drained feedback queue after all completed date listings are seeded."""
    if not (root / "source.json").is_file():
        raise ValueError("No feedback source manifest found")
    source_root = Path(json.loads((root / "source.json").read_text())["meetings_root"])
    with closing(sqlite3.connect(source_root / "collection.sqlite")) as source_db:
        source_groups = dict(
            source_db.execute("SELECT status,count(*) FROM requests GROUP BY status")
        )
    allowed = {"done", "error"} if allow_source_errors else {"done"}
    if not source_groups or set(source_groups) - allowed:
        raise ValueError("Dated meeting collection is incomplete")
    with closing(sqlite3.connect(root / "collection.sqlite")) as db:
        db.row_factory = sqlite3.Row
        groups = dict(
            db.execute("SELECT status,count(*) FROM requests GROUP BY status")
        )
        if not groups or set(groups) - allowed:
            raise ValueError("Feedback collection is incomplete")
        seeded = db.execute("SELECT count(*) FROM seeded_reports").fetchone()[0]
        if seeded != source_groups.get("done", 0):
            raise ValueError(
                "Rerun feedback collection to seed the remaining dated reports"
            )
        return write_feedback(db, root, source_groups)


def write_feedback(db: sqlite3.Connection, root: Path, source_groups: dict) -> dict:
    """Stream reports, question answers, table cells and validated join edges."""
    output = root / "tables"
    output.mkdir(exist_ok=True)
    for name in ["manifest.json", "CHECKSUMS"]:
        (output / name).unlink(missing_ok=True)
    db.execute(
        "CREATE TEMP TABLE parsed_feedback "
        "(url TEXT PRIMARY KEY, reported_date TEXT, reported_type TEXT)"
    )
    db.execute(
        "CREATE TEMP TABLE link_counts AS SELECT feedback_url, count(*) AS n "
        "FROM feedback_links WHERE feedback_url IS NOT NULL GROUP BY feedback_url"
    )
    db.execute("CREATE UNIQUE INDEX link_counts_url ON link_counts(feedback_url)")
    buffers = {name: [] for name in SCHEMAS}
    counts, issues = Counter(), Counter()
    missing_counts = Counter()
    errors = []
    with ExitStack() as stack:
        writers = {
            name: stack.enter_context(
                pq.ParquetWriter(
                    output / f"{name}.parquet.part", schema, compression="zstd"
                )
            )
            for name, schema in SCHEMAS.items()
        }

        def add(name: str, row: dict) -> None:
            buffers[name].append(row)
            counts[name] += 1
            if len(buffers[name]) >= 10000:
                writers[name].write_table(
                    pa.Table.from_pylist(buffers[name], schema=SCHEMAS[name])
                )
                buffers[name].clear()

        for task in db.execute("SELECT * FROM requests ORDER BY url"):
            if task["status"] == "error":
                errors.append({"url": task["url"], "error": task["error"]})
                continue
            key = hashlib.sha256(task["url"].encode()).hexdigest()
            capture = read_capture(
                root / "raw" / task["edition"] / f"{key}.jsonl.gz",
                parser=parse_feedback,
            )
            if (
                capture is None
                or capture["url"] != task["url"]
                or capture["edition"] != task["edition"]
            ):
                raise ValueError(f"Missing or damaged feedback capture: {task['url']}")
            records = capture["parsed_rows"]
            if len(records) != 1 or task["rows"] != 1:
                raise ValueError("Expected one feedback report per URL")
            record = records[0]
            context = json.loads(task["context"])
            row = {
                **context,
                **record,
                "source_url": task["url"],
                "fetched_at": datetime.fromisoformat(capture["fetched_at"]),
                "meeting_date": date.fromisoformat(record["meeting_date"])
                if record["meeting_date"]
                else None,
            }
            if row["fetched_at"].tzinfo is None:
                raise ValueError("Capture timestamp must include a time zone")
            add("feedback", row)
            db.execute(
                "INSERT INTO parsed_feedback VALUES (?,?,?)",
                (task["url"], record["meeting_date_raw"], record["feedback_type"]),
            )
            for ordinal, answer in enumerate(json.loads(record["answers"]), start=1):
                add(
                    "feedback_answers",
                    {"source_url": task["url"], "answer_ordinal": ordinal, **answer},
                )
            for ordinal, table_row in enumerate(json.loads(record["tables"]), start=1):
                add(
                    "feedback_tables",
                    {"source_url": task["url"], "row_ordinal": ordinal, **table_row},
                )
            issues["attendance_inconsistencies"] += int(
                record["attendance_inconsistent"]
            )
            issues["unpopulated_forms"] += int(not record["report_available"])
            issues["invalid_dates"] += int(record["date_error"] is not None)
            for field in ATTENDANCE.values():
                missing_counts[field] += int(record[field] is None)
        links = db.execute(
            "SELECT l.*,r.status AS request_status,p.reported_date,p.reported_type,c.n "
            "FROM feedback_links l LEFT JOIN requests r ON l.feedback_url=r.url "
            "LEFT JOIN parsed_feedback p ON l.feedback_url=p.url "
            "LEFT JOIN link_counts c ON l.feedback_url=c.feedback_url "
            "ORDER BY l.meeting_url,l.row_ordinal"
        )
        for link in links:
            row = dict(link)
            row["date_match"] = matching_date(
                row["expected_date"], row["reported_date"]
            )
            row["type_match"] = matching_type(
                row["expected_type"], row["reported_type"]
            )
            row["same_report_for_multiple_rows"] = row["n"] is not None and row["n"] > 1
            issues["date_mismatches"] += int(row["date_match"] is False)
            issues["type_mismatches"] += int(row["type_match"] is False)
            issues["ambiguous_link_rows"] += int(row["same_report_for_multiple_rows"])
            add("feedback_links", row)
        for name, rows in buffers.items():
            if rows:
                writers[name].write_table(
                    pa.Table.from_pylist(rows, schema=SCHEMAS[name])
                )
    for name in SCHEMAS:
        (output / f"{name}.parquet.part").replace(output / f"{name}.parquet")
    report = {
        "rows": dict(counts),
        "issues": dict(issues),
        "missing_attendance_counts": dict(missing_counts),
        "source_errors": errors,
        "dated_source_requests": source_groups,
        "all_discovered_requests_succeeded": not errors
        and not source_groups.get("error"),
        "feedback_key": "source_url",
        "link_key": ["meeting_url", "row_ordinal"],
        "join_rule": (
            "many-to-one by feedback_url; inspect date/type mismatches "
            "and repeated report links before attaching attendance"
        ),
    }
    atomic_json(output / "manifest.json", report)
    atomic_json(
        output / "SCHEMA.json",
        {
            name: {field.name: str(field.type) for field in schema}
            for name, schema in SCHEMAS.items()
        },
    )
    (output / "CHECKSUMS").write_text(
        "".join(
            f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n"
            for path in sorted(output.iterdir())
            if path.suffix in {".json", ".parquet"}
        )
    )
    return report
