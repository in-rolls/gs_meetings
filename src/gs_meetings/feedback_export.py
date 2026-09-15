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

from gs_meetings.feedback import (
    ATTENDANCE,
    BOOLEAN_QUESTIONS,
    FORM_ABSENT,
    parse_feedback,
)
from gs_meetings.fetch import atomic_json, read_capture

GP_YEAR = ["edition", "financial_year", "state_code", "report_scope", "local_body_code"]
COVERAGE_COUNTS = [
    "listed_meetings",
    "reports",
    "forms_absent",
    "fetch_errors",
    "not_requested",
    "unfilled_forms",
    "distinct_reports",
]

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
                *GP_YEAR,
                "feedback_url",
                "outcome",
                "expected_date",
                "expected_type",
                "issue",
                "request_status",
                "request_error",
                "reported_date",
                "reported_type",
            ]
        ]
        + [
            ("row_ordinal", pa.int64()),
            ("meeting_date", pa.date32()),
            ("date_match", pa.bool_()),
            ("type_match", pa.bool_()),
            ("same_report_for_multiple_rows", pa.bool_()),
        ]
    ),
    "feedback_coverage": pa.schema(
        [(name, pa.string()) for name in GP_YEAR]
        + [(name, pa.int64()) for name in COVERAGE_COUNTS]
    ),
}


def outcome(status: str | None, error: str | None) -> str:
    """Separate the portal's stable no-form response from transport failures."""
    if status is None:
        return "not_requested"
    if status == "done":
        return "report"
    if error == FORM_ABSENT:
        return "form_absent"
    return "fetch_error"


def load_meeting_keys(db: sqlite3.Connection, meetings_root: Path) -> None:
    """Stage the GP-year identity of every exported dated listing row for joins."""
    path = meetings_root / "tables" / "meetings.parquet"
    if not path.is_file():
        raise ValueError("Run the meetings export before the feedback export")
    columns = ["source_url", "row_ordinal", *GP_YEAR, "meeting_date"]
    db.execute(
        "CREATE TEMP TABLE meeting_keys (source_url TEXT, row_ordinal INTEGER, "
        "edition TEXT, financial_year TEXT, state_code TEXT, report_scope TEXT, "
        "local_body_code TEXT, meeting_date TEXT, "
        "PRIMARY KEY(source_url,row_ordinal))"
    )
    for batch in pq.ParquetFile(path).iter_batches(columns=columns):
        try:
            db.executemany(
                "INSERT INTO meeting_keys VALUES (?,?,?,?,?,?,?,?)",
                (
                    tuple(
                        value.isoformat() if isinstance(value, date) else value
                        for value in (row[name] for name in columns)
                    )
                    for row in batch.to_pylist()
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise ValueError(
                "The meetings export repeats a (source_url, row_ordinal) key; "
                "rerun the meetings export"
            ) from exc
    unmatched = db.execute(
        "SELECT count(*) FROM feedback_links l LEFT JOIN meeting_keys m "
        "ON l.meeting_url=m.source_url AND l.row_ordinal=m.row_ordinal "
        "WHERE m.source_url IS NULL"
    ).fetchone()[0]
    if unmatched:
        raise ValueError(
            f"The meetings export does not cover {unmatched} linked listing rows; "
            "rerun the meetings export"
        )
    unlinked = db.execute(
        "SELECT count(*) FROM meeting_keys m LEFT JOIN feedback_links l "
        "ON l.meeting_url=m.source_url AND l.row_ordinal=m.row_ordinal "
        "WHERE l.meeting_url IS NULL"
    ).fetchone()[0]
    if unlinked:
        raise ValueError(
            f"The meetings export has {unlinked} listing rows that feedback "
            "seeding never saw; rerun feedback collection"
        )


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
        if not groups or set(groups) - {"done", "error"}:
            raise ValueError("Feedback collection is incomplete")
        fetch_errors = db.execute(
            "SELECT count(*) FROM requests WHERE status='error' AND error IS NOT ?",
            (FORM_ABSENT,),
        ).fetchone()[0]
        if fetch_errors and not allow_source_errors:
            raise ValueError(
                f"{fetch_errors} feedback requests failed; "
                "rerun collection or export with source errors allowed"
            )
        seeded = db.execute("SELECT count(*) FROM seeded_reports").fetchone()[0]
        if seeded != source_groups.get("done", 0):
            raise ValueError(
                "Rerun feedback collection to seed the remaining dated reports"
            )
        load_meeting_keys(db, source_root)
        return write_feedback(db, root, source_groups)


def write_feedback(db: sqlite3.Connection, root: Path, source_groups: dict) -> dict:
    """Stream reports, question answers, table cells and validated join edges."""
    output = root / "tables"
    output.mkdir(exist_ok=True)
    for name in ["manifest.json", "CHECKSUMS"]:
        (output / name).unlink(missing_ok=True)
    db.execute(
        "CREATE TEMP TABLE parsed_feedback (url TEXT PRIMARY KEY, "
        "reported_date TEXT, reported_type TEXT, report_available INTEGER)"
    )
    db.execute(
        "CREATE TEMP TABLE link_counts AS SELECT feedback_url, count(*) AS n "
        "FROM feedback_links WHERE feedback_url IS NOT NULL GROUP BY feedback_url"
    )
    db.execute("CREATE UNIQUE INDEX link_counts_url ON link_counts(feedback_url)")
    buffers = {name: [] for name in SCHEMAS}
    counts, issues = Counter(), Counter()
    missing_counts = Counter()
    outcomes, absent_by_state = Counter(), Counter()
    coverage: dict[tuple, Counter] = {}
    reported_urls: dict[tuple, set] = {}
    unfilled_urls: dict[tuple, set] = {}
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
                if task["error"] != FORM_ABSENT:
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
                "INSERT INTO parsed_feedback VALUES (?,?,?,?)",
                (
                    task["url"],
                    record["meeting_date_raw"],
                    record["feedback_type"],
                    int(record["report_available"]),
                ),
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
            "SELECT l.*,r.status AS request_status,r.error AS request_error,"
            "p.reported_date,p.reported_type,p.report_available,c.n,"
            "m.edition,m.financial_year,m.state_code,m.report_scope,"
            "m.local_body_code,m.meeting_date FROM feedback_links l "
            "JOIN meeting_keys m "
            "ON l.meeting_url=m.source_url AND l.row_ordinal=m.row_ordinal "
            "LEFT JOIN requests r ON l.feedback_url=r.url "
            "LEFT JOIN parsed_feedback p ON l.feedback_url=p.url "
            "LEFT JOIN link_counts c ON l.feedback_url=c.feedback_url "
            "ORDER BY l.meeting_url,l.row_ordinal"
        )
        for link in links:
            row = dict(link)
            row["meeting_date"] = (
                date.fromisoformat(row["meeting_date"]) if row["meeting_date"] else None
            )
            row["outcome"] = outcome(row["request_status"], row["request_error"])
            outcomes[(row["edition"], row["outcome"])] += 1
            if row["outcome"] == "form_absent":
                absent_by_state[row["state_code"]] += 1
            gp_year = tuple(row[key] for key in GP_YEAR)
            tally = coverage.setdefault(gp_year, Counter())
            tally["listed_meetings"] += 1
            tally[
                {
                    "report": "reports",
                    "form_absent": "forms_absent",
                    "fetch_error": "fetch_errors",
                    "not_requested": "not_requested",
                }[row["outcome"]]
            ] += 1
            if row["outcome"] == "report":
                reported_urls.setdefault(gp_year, set()).add(row["feedback_url"])
                if not row["report_available"]:
                    unfilled_urls.setdefault(gp_year, set()).add(row["feedback_url"])
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
        for gp_year in sorted(coverage, key=lambda key: tuple(map(str, key))):
            tally = coverage[gp_year]
            tally["distinct_reports"] = len(reported_urls.get(gp_year, ()))
            tally["unfilled_forms"] = len(unfilled_urls.get(gp_year, ()))
            add(
                "feedback_coverage",
                {
                    **dict(zip(GP_YEAR, gp_year, strict=True)),
                    **{name: tally[name] for name in COVERAGE_COUNTS},
                },
            )
        for name, rows in buffers.items():
            if rows:
                writers[name].write_table(
                    pa.Table.from_pylist(rows, schema=SCHEMAS[name])
                )
    for name in SCHEMAS:
        (output / f"{name}.parquet.part").replace(output / f"{name}.parquet")
    by_edition: dict[str, dict[str, int]] = {}
    for (edition, name), n in sorted(outcomes.items()):
        by_edition.setdefault(edition, {})[name] = n
    report = {
        "rows": dict(counts),
        "issues": dict(issues),
        "missing_attendance_counts": dict(missing_counts),
        "outcomes": dict(sum((Counter(v) for v in by_edition.values()), Counter())),
        "outcomes_by_edition": by_edition,
        "forms_absent_by_state": dict(sorted(absent_by_state.items())),
        "source_errors": errors,
        "dated_source_requests": source_groups,
        "all_discovered_requests_succeeded": not errors
        and not source_groups.get("error"),
        "feedback_key": "source_url",
        "link_key": ["meeting_url", "row_ordinal"],
        "coverage_key": GP_YEAR,
        "outcome_rule": (
            "form_absent is the portal's stable no-form response for a listed "
            "meeting; fetch_error is a transport failure; not_requested is a "
            "listing row without a usable date"
        ),
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
