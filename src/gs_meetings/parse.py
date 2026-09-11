"""Convert saved GP responses to typed records without network access."""

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from gs_meetings.fetch import atomic_json, read_capture
from gs_meetings.frame import write_parquet
from gs_meetings.source import METRICS, validate_rows

CONTEXT = [
    "edition",
    "state_code",
    "state_name",
    "district_code",
    "district_name",
    "block_code",
    "block_name",
    "parent_level",
    "parent_code",
]
SCHEMA = pa.schema(
    [
        (key, pa.string())
        for key in [
            *CONTEXT,
            "gp_code",
            "gp_name",
            "source_level",
            "source_url",
            "fetched_at",
            "raw_row",
        ]
    ]
    + [(key, pa.int64()) for key in METRICS.values()]
    + [("source_tlb", pa.bool_())]
)


def parse_rows(body: str, unit: dict, fetched_at: str) -> list[dict]:
    """Preserve source counts, missing values and unedited row JSON."""
    return [
        {
            **{key: unit.get(key) for key in CONTEXT},
            "gp_code": str(row["code"]),
            "gp_name": row["name"],
            "source_level": row.get("level"),
            "source_tlb": row.get("tlb"),
            "source_url": unit["url"],
            "fetched_at": fetched_at,
            "raw_row": json.dumps(row, ensure_ascii=False),
            **{target: row[source] for source, target in METRICS.items()},
        }
        for row in validate_rows(json.loads(body))
    ]


def attendance_issues(rows: list[dict]) -> list[dict]:
    """Flag subgroup counts above total attendance without changing source values."""
    issues = []
    for row in rows:
        total = row["people_present"]
        if total is None:
            continue
        exceeding = {
            field: row[field]
            for field in ["women_present", "sc_present", "st_present", "shg_present"]
            if row[field] is not None and row[field] > total
        }
        if exceeding:
            issues.append(
                {
                    "source_url": row["source_url"],
                    "gp_code": row["gp_code"],
                    "gp_name": row["gp_name"],
                    "people_present": total,
                    "subgroups_exceeding_total": exceeding,
                }
            )
    return issues


def convert(root: Path) -> dict:
    """Write records, schema, checksums and explicit coverage/reconciliation."""
    units = pq.read_table(root / "frame.parquet").to_pylist()
    rows, missing, reconciliation = [], [], []
    keys = set()
    for unit in units:
        digest = hashlib.sha256(unit["url"].encode()).hexdigest()
        path = root / "raw" / unit["edition"] / f"{digest}.jsonl.gz"
        capture = read_capture(path)
        if capture is None:
            missing.append(unit["url"])
            continue
        if capture["url"] != unit["url"] or capture["edition"] != unit["edition"]:
            raise ValueError("Capture provenance does not match the frame")
        parsed = parse_rows(capture["body"], unit, capture["fetched_at"])
        for row in parsed:
            key = tuple(
                row.get(field)
                for field in [
                    "edition",
                    "state_code",
                    "district_code",
                    "block_code",
                    "parent_level",
                    "parent_code",
                    "gp_code",
                ]
            )
            if key in keys:
                raise ValueError(f"Duplicate GP within the same hierarchy: {key}")
            keys.add(key)
        rows.extend(parsed)
        parent = json.loads(unit["expected"])
        differences = {}
        for source, target in METRICS.items():
            if source == "totalGp":
                continue
            values = [row[target] for row in parsed]
            differences[target] = (
                sum(values) - parent[source]
                if parent[source] is not None
                and all(value is not None for value in values)
                else None
            )
        reconciliation.append(
            {
                "url": unit["url"],
                "gp_rows": len(parsed),
                "child_sum_minus_parent": differences,
            }
        )
    if not rows:
        raise ValueError("No GP records found; no deliverable written")
    write_parquet(root / "records.parquet", rows, SCHEMA)
    atomic_json(root / "SCHEMA.json", {field.name: str(field.type) for field in SCHEMA})
    report = {
        "rows": len(rows),
        "units_in_frame": len(units),
        "units_with_valid_responses": len(units) - len(missing),
        "missing_units": missing,
        "empty_units": [
            entry["url"] for entry in reconciliation if entry["gp_rows"] == 0
        ],
        "reconciliation": reconciliation,
        "attendance_issues": attendance_issues(rows),
        "null_counts": {
            field: sum(row[field] is None for row in rows) for field in METRICS.values()
        },
        "measurement": (
            "Source-reported campaign summaries; "
            "not unique attendees or verified meeting counts"
        ),
    }
    atomic_json(root / "manifest.json", report)
    checksums = [
        f"{hashlib.sha256((root / name).read_bytes()).hexdigest()}  {name}\n"
        for name in ["frame.parquet", "records.parquet", "SCHEMA.json", "manifest.json"]
    ]
    (root / "CHECKSUMS").write_text("".join(checksums))
    return report
