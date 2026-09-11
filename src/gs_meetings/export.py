"""Stream national captures into typed tables with explicit period metadata."""

import hashlib
import json
import sqlite3
from collections import defaultdict
from contextlib import closing
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from gs_meetings.fetch import atomic_json, read_capture
from gs_meetings.frame import write_parquet
from gs_meetings.parse import SCHEMA, attendance_issues, parse_rows
from gs_meetings.source import EDITIONS, METRICS

REPORT_SCHEMA = pa.schema(
    [
        (
            field.name,
            pa.timestamp("us", tz="UTC") if field.name == "fetched_at" else field.type,
        )
        for field in SCHEMA
    ]
    + [("observation_id", pa.string())]
)
EDITION_SCHEMA = pa.schema(
    [
        (
            name,
            pa.date32()
            if name in {"homepage_campaign_start", "homepage_campaign_end"}
            else pa.string(),
        )
        for name in [
            "edition",
            "homepage_campaign_start",
            "homepage_campaign_end",
            "homepage_plan_year",
            "source_url",
            "scope_note",
        ]
    ]
)
PROFILE_SCHEMA = pa.schema(
    [
        ("edition", pa.string()),
        ("state_code", pa.string()),
        ("field", pa.string()),
        *[
            (key, pa.int64())
            for key in ["rows", "missing", "zero", "minimum", "maximum"]
        ],
    ]
)
COVERAGE_SCHEMA = pa.schema(
    [
        ("edition", pa.string()),
        ("level", pa.string()),
        ("url", pa.string()),
        ("context", pa.string()),
        ("parent", pa.string()),
        ("rows", pa.int64()),
        ("status", pa.string()),
    ]
)


def export_national(root: Path, *, allow_source_errors: bool = False) -> dict:
    """Export only a completed queue, retaining source gaps and quality flags."""
    if not (root / "collection.sqlite").is_file():
        raise ValueError("No collection queue found")
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
    accepted = {"done", "error"} if allow_source_errors else {"done"}
    if not tasks or any(task["status"] not in accepted for task in tasks):
        raise ValueError(
            "Collection is incomplete; finish pending/failed requests first"
        )
    output = root / "tables"
    output.mkdir(exist_ok=True)
    (output / "manifest.json").unlink(missing_ok=True)
    (output / "CHECKSUMS").unlink(missing_ok=True)
    metadata = json.loads(Path(__file__).with_name("editions.json").read_text())
    editions = [row["edition"] for row in metadata]
    if len(editions) != len(set(editions)) or set(editions) != set(EDITIONS):
        raise ValueError("Edition metadata must have exactly one row per edition")
    if any(task["edition"] not in editions for task in tasks):
        raise ValueError("Request edition is missing from period metadata")
    for record in metadata:
        for key in ["homepage_campaign_start", "homepage_campaign_end"]:
            if record[key] is not None:
                record[key] = date.fromisoformat(record[key])
    write_parquet(output / "report_editions.parquet", metadata, EDITION_SCHEMA)
    write_parquet(output / "request_coverage.parquet", tasks, COVERAGE_SCHEMA)
    profiles = defaultdict(
        lambda: {"rows": 0, "missing": 0, "zero": 0, "minimum": None, "maximum": None}
    )
    counts = defaultdict(int)
    gp_keys = defaultdict(list)
    fetched_times = []
    issues, reconciliation, summaries = [], [], []
    buffer = []
    part = output / "gp_reports.parquet.part"
    with pq.ParquetWriter(part, REPORT_SCHEMA, compression="zstd") as writer:
        for task in tasks:
            if task["status"] != "done":
                continue
            digest = hashlib.sha256(task["url"].encode()).hexdigest()
            capture = read_capture(
                root / "raw" / task["edition"] / f"{digest}.jsonl.gz"
            )
            if (
                capture is None
                or capture["url"] != task["url"]
                or capture["edition"] != task["edition"]
            ):
                raise ValueError(f"Missing or damaged completed capture: {task['url']}")
            context = json.loads(task["context"])
            if context.get("edition") != task["edition"]:
                raise ValueError("Queue context edition disagrees with request")
            unit = {**context, "url": task["url"]}
            rows = parse_rows(capture["body"], unit, capture["fetched_at"])
            if len(rows) != task["rows"]:
                raise ValueError(
                    f"Capture row count disagrees with queue: {task['url']}"
                )
            captured_at = datetime.fromisoformat(capture["fetched_at"])
            if captured_at.tzinfo is None:
                raise ValueError("Capture timestamp must include a time zone")
            fetched_times.append(captured_at)
            if task["level"] != "gp":
                summaries.extend(
                    {
                        "edition": task["edition"],
                        "level": task["level"],
                        "url": task["url"],
                        "context": task["context"],
                        "raw_row": row["raw_row"],
                        "fetched_at": capture["fetched_at"],
                    }
                    for row in rows
                )
                continue
            parent = json.loads(task["parent"])
            for source, target in METRICS.items():
                if source == "totalGp":
                    continue
                values = [row[target] for row in rows]
                total = (
                    sum(values) if all(value is not None for value in values) else None
                )
                expected = parent[source]
                reconciliation.append(
                    {
                        "edition": task["edition"],
                        "url": task["url"],
                        "metric": target,
                        "expected": expected,
                        "observed": total,
                        "difference": total - expected
                        if total is not None and expected is not None
                        else None,
                    }
                )
            issues.extend(
                {"edition": task["edition"], **issue}
                for issue in attendance_issues(rows)
            )
            for row in rows:
                row["fetched_at"] = captured_at
                row["observation_id"] = hashlib.sha256(
                    (task["url"] + "#" + row["gp_code"]).encode()
                ).hexdigest()
                gp_keys[(task["edition"], row["state_code"], row["gp_code"])].append(
                    task["url"]
                )
                counts[task["edition"]] += 1
                for field in METRICS.values():
                    profile = profiles[(task["edition"], row["state_code"], field)]
                    value = row[field]
                    profile["rows"] += 1
                    if value is None:
                        profile["missing"] += 1
                    else:
                        profile["zero"] += int(value == 0)
                        profile["minimum"] = (
                            value
                            if profile["minimum"] is None
                            else min(value, profile["minimum"])
                        )
                        profile["maximum"] = (
                            value
                            if profile["maximum"] is None
                            else max(value, profile["maximum"])
                        )
                buffer.append(row)
            if len(buffer) >= 10000:
                writer.write_table(pa.Table.from_pylist(buffer, schema=REPORT_SCHEMA))
                buffer.clear()
        if buffer:
            writer.write_table(pa.Table.from_pylist(buffer, schema=REPORT_SCHEMA))
    if not sum(counts.values()):
        raise ValueError("No GP records were collected")
    if sum(counts.values()) != sum(
        task["rows"]
        for task in tasks
        if task["level"] == "gp" and task["status"] == "done"
    ):
        raise ValueError("GP row conservation check failed")
    part.replace(output / "gp_reports.parquet")
    profile_rows = [
        {"edition": key[0], "state_code": key[1], "field": key[2], **value}
        for key, value in sorted(profiles.items())
    ]
    write_parquet(output / "column_profiles.parquet", profile_rows, PROFILE_SCHEMA)
    summary_schema = pa.schema(
        [
            (name, pa.string())
            for name in ["edition", "level", "url", "context", "raw_row", "fetched_at"]
        ]
    )
    write_parquet(output / "hierarchy_summaries.parquet", summaries, summary_schema)
    reconciliation_schema = pa.schema(
        [(name, pa.string()) for name in ["edition", "url", "metric"]]
        + [(name, pa.int64()) for name in ["expected", "observed", "difference"]]
    )
    write_parquet(
        output / "reconciliation.parquet", reconciliation, reconciliation_schema
    )
    atomic_json(output / "attendance_issues.json", issues)
    ambiguous = [
        {
            "edition": key[0],
            "state_code": key[1],
            "gp_code": key[2],
            "source_urls": urls,
        }
        for key, urls in sorted(gp_keys.items())
        if len(urls) > 1
    ]
    atomic_json(output / "ambiguous_gp_keys.json", ambiguous)
    report = {
        "rows": sum(counts.values()),
        "rows_by_edition": dict(counts),
        "requests": len(tasks),
        "routing_gaps": gaps,
        "all_discovered_requests_succeeded": all(
            task["status"] == "done" for task in tasks
        ),
        "source_errors": [
            {"url": task["url"], "error": task["error"]}
            for task in tasks
            if task["status"] == "error"
        ],
        "empty_responses": [task["url"] for task in tasks if task["rows"] == 0],
        "attendance_issues": len(issues),
        "ambiguous_gp_keys": len(ambiguous),
        "capture_start": min(fetched_times).isoformat(),
        "capture_end": max(fetched_times).isoformat(),
        "measurement": (
            "Source-reported aggregates; "
            "not verified unique participants or meeting counts"
        ),
        "coverage": (
            "Completed discovered summary routes; empty responses and "
            "undiscovered or unreported GPs remain coverage gaps"
        ),
        "nonzero_reconciliation_differences": sum(
            row["difference"] not in {0, None} for row in reconciliation
        ),
        "row_key": ["edition", "source_url", "gp_code"],
        "time_join": (
            "many-to-one on edition to report_editions.parquet; "
            "campaign windows are not verified observation bounds"
        ),
    }
    atomic_json(output / "manifest.json", report)
    atomic_json(
        output / "SCHEMA.json", {field.name: str(field.type) for field in REPORT_SCHEMA}
    )
    (output / "CHECKSUMS").write_text(
        "".join(
            f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n"
            for path in sorted(output.iterdir())
            if path.suffix in {".json", ".parquet"} and path.is_file()
        )
    )
    return report
