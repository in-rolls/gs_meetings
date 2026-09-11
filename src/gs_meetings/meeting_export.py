"""Export dated report rows without turning list positions into event identities."""

import hashlib
import json
import sqlite3
from collections import Counter
from contextlib import closing
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from gs_meetings.fetch import atomic_json, read_capture
from gs_meetings.frame import write_parquet
from gs_meetings.meetings import parse_meetings, validate_meeting_rows

MEETING_SCHEMA = pa.schema(
    [
        (name, pa.string())
        for name in [
            "observation_id",
            "candidate_event_key",
            "edition",
            "financial_year",
            "state_code",
            "state_name",
            "hierarchy",
            "report_scope",
            "local_body_code",
            "local_body_name",
            "source_row_id",
            "meeting_date_raw",
            "date_error",
            "meeting_type",
            "requested_meeting_type",
            "source_url",
            "raw_row",
        ]
    ]
    + [
        ("row_ordinal", pa.int64()),
        ("meeting_date", pa.date32()),
        ("outside_financial_year", pa.bool_()),
        ("fetched_at", pa.timestamp("us", tz="UTC")),
    ]
)


def export_meetings(root: Path, *, allow_source_errors: bool = False) -> dict:
    """Require a drained queue and retain failed routes as explicit coverage gaps."""
    if not (root / "collection.sqlite").exists():
        raise ValueError("No meeting collection queue found")
    with closing(sqlite3.connect(root / "collection.sqlite")) as db:
        db.row_factory = sqlite3.Row
        tasks = [
            dict(row)
            for row in db.execute("SELECT * FROM requests ORDER BY edition,url")
        ]
        gaps = [
            dict(row)
            for row in db.execute("SELECT * FROM coverage_gaps ORDER BY url,raw_row")
        ]
    allowed = {"done", "error"} if allow_source_errors else {"done"}
    if not tasks or any(task["status"] not in allowed for task in tasks):
        raise ValueError("Meeting collection is incomplete")
    output = root / "tables"
    output.mkdir(exist_ok=True)
    for name in ["manifest.json", "CHECKSUMS"]:
        (output / name).unlink(missing_ok=True)
    counts, candidates = Counter(), Counter()
    dates = {}
    issues = Counter()
    buffer, hierarchy = [], []
    part = output / "meetings.parquet.part"
    with pq.ParquetWriter(part, MEETING_SCHEMA, compression="zstd") as writer:
        for task in tasks:
            if task["status"] != "done":
                continue
            key = hashlib.sha256(task["url"].encode()).hexdigest()
            capture = read_capture(
                root / "raw" / task["edition"] / f"{key}.jsonl.gz",
                validate_meeting_rows,
            )
            if (
                capture is None
                or capture["url"] != task["url"]
                or capture["edition"] != task["edition"]
            ):
                raise ValueError(f"Missing or damaged meeting capture: {task['url']}")
            rows = json.loads(capture["body"])
            if len(rows) != task["rows"]:
                raise ValueError("Meeting capture row count differs from queue")
            context = json.loads(task["context"])
            if context["edition"] != task["edition"]:
                raise ValueError("Meeting edition differs from request context")
            if not rows or not any(
                "gram_sabha_date" in row or "gramSabhaDate" in row for row in rows
            ):
                hierarchy.extend(
                    {
                        "url": task["url"],
                        "context": task["context"],
                        "raw_row": json.dumps(row, ensure_ascii=False),
                    }
                    for row in rows
                )
                continue
            records = parse_meetings(rows, context, task["url"], capture["fetched_at"])
            if len(records) != len(rows):
                raise ValueError("Dated row conservation failed")
            for record in records:
                if record["fetched_at"].tzinfo is None:
                    raise ValueError("Capture timestamp must include a time zone")
                candidate = [
                    record[field]
                    for field in [
                        "edition",
                        "state_code",
                        "report_scope",
                        "local_body_code",
                        "meeting_date_raw",
                        "meeting_type",
                    ]
                ]
                record["candidate_event_key"] = hashlib.sha256(
                    json.dumps(candidate).encode()
                ).hexdigest()
                candidates[record["candidate_event_key"]] += 1
                period = f"{record['edition']}:{record['financial_year'] or 'archive'}"
                counts[period] += 1
                if record["meeting_date"] is not None:
                    value = record["meeting_date"]
                    low, high = dates.get(period, (value, value))
                    dates[period] = min(low, value), max(high, value)
                issues["missing_dates"] += int(record["meeting_date_raw"] is None)
                issues["invalid_dates"] += int(record["date_error"] is not None)
                issues["outside_financial_year"] += int(
                    record["outside_financial_year"] is True
                )
                buffer.append(record)
            if len(buffer) >= 10000:
                writer.write_table(pa.Table.from_pylist(buffer, schema=MEETING_SCHEMA))
                buffer.clear()
        if buffer:
            writer.write_table(pa.Table.from_pylist(buffer, schema=MEETING_SCHEMA))
    if not sum(counts.values()):
        raise ValueError("No dated meeting records were collected")
    part.replace(output / "meetings.parquet")
    write_parquet(
        output / "request_coverage.parquet",
        tasks,
        pa.schema(
            [
                (name, pa.string())
                for name in [
                    "edition",
                    "level",
                    "url",
                    "context",
                    "parent",
                    "status",
                    "error",
                ]
            ]
            + [("rows", pa.int64())]
        ),
    )
    write_parquet(
        output / "hierarchy_summaries.parquet",
        hierarchy,
        pa.schema([(name, pa.string()) for name in ["url", "context", "raw_row"]]),
    )
    duplicates = [
        {"candidate_event_key": key, "rows": count}
        for key, count in sorted(candidates.items())
        if count > 1
    ]
    write_parquet(
        output / "duplicate_candidate_events.parquet",
        duplicates,
        pa.schema([("candidate_event_key", pa.string()), ("rows", pa.int64())]),
    )
    report = {
        "rows": sum(counts.values()),
        "rows_by_period": dict(counts),
        "date_ranges": {
            key: [low.isoformat(), high.isoformat()]
            for key, (low, high) in dates.items()
        },
        "date_issues": dict(issues),
        "routing_gaps": gaps,
        "duplicate_candidate_event_keys": len(duplicates),
        "all_discovered_requests_succeeded": all(
            task["status"] == "done" for task in tasks
        ),
        "source_errors": [
            {"url": task["url"], "error": task["error"]}
            for task in tasks
            if task["status"] == "error"
        ],
        "empty_responses": [task["url"] for task in tasks if task["rows"] == 0],
        "row_key": ["source_url", "row_ordinal"],
        "measurement": (
            "Source meeting listings; no verified event ID or attendance deduplication"
        ),
        "coverage": (
            "Discovered report routes; failed and empty responses "
            "are not evidence that no meetings occurred"
        ),
    }
    atomic_json(output / "manifest.json", report)
    atomic_json(
        output / "SCHEMA.json",
        {field.name: str(field.type) for field in MEETING_SCHEMA},
    )
    (output / "CHECKSUMS").write_text(
        "".join(
            f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n"
            for path in sorted(output.iterdir())
            if path.suffix in {".json", ".parquet"}
        )
    )
    return report
