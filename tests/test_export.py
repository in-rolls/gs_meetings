"""Verify row conservation, source anomalies and time interpretation offline."""

import gzip
import hashlib
import json
import sqlite3
from contextlib import closing
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from gs_meetings.cli import main
from gs_meetings.collect import add_request, open_queue
from gs_meetings.export import export_national
from gs_meetings.source import METRICS


def save_capture(root, task, rows):
    path = (
        root
        / "raw"
        / task["edition"]
        / (hashlib.sha256(task["url"].encode()).hexdigest() + ".jsonl.gz")
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        json.dump(
            {
                "url": task["url"],
                "edition": task["edition"],
                "fetched_at": "2026-09-11T10:00:00+00:00",
                "ok": True,
                "done": True,
                "body": json.dumps(rows),
            },
            stream,
        )
    return path


@pytest.fixture
def collection(tmp_path):
    rows = json.loads((Path(__file__).parent / "fixtures/gp.json").read_text())[:2]
    rows[0]["peoplePresent"] = 0
    rows[0]["womenPresent"] = 2
    rows[1]["scPresent"] = None
    parent = dict.fromkeys(METRICS, 0)
    db = open_queue(tmp_path)
    for code in [1030, 1031]:
        add_request(
            db,
            "current",
            "gp",
            {
                "edition": "current",
                "state_code": "6",
                "state_name": "HARYANA",
                "block_code": str(code),
                "parent_code": str(code),
                "parent_level": "block",
            },
            parent,
            stateId=6,
            code=code,
        )
    for task in db.execute("SELECT * FROM requests").fetchall():
        body = rows if task["level"] == "gp" else []
        save_capture(tmp_path, task, body)
        db.execute(
            "UPDATE requests SET status='done',rows=? WHERE url=?",
            (len(body), task["url"]),
        )
    db.commit()
    db.close()
    return tmp_path


def test_export_preserves_counts_nulls_ambiguity_and_time(collection):
    report = export_national(collection)
    tables = collection / "tables"
    table = pq.read_table(tables / "gp_reports.parquet")
    assert table.num_rows == report["rows"] == 4
    assert len(set(table.column("observation_id").to_pylist())) == 4
    assert table.schema.field("fetched_at").type == pa.timestamp("us", tz="UTC")
    assert table.column("sc_present").null_count == 2
    assert table.column("people_present").to_pylist().count(0) == 2
    assert report["ambiguous_gp_keys"] == 2
    assert report["attendance_issues"] >= 2
    assert report["nonzero_reconciliation_differences"] > 0
    periods = {
        row["edition"]: row
        for row in pq.read_table(tables / "report_editions.parquet").to_pylist()
    }
    assert periods["PPC2019"]["homepage_campaign_start"] == date(2020, 5, 1)
    assert periods["current"]["homepage_campaign_start"] is None
    assert periods["PPC2020"]["homepage_plan_year"] == "2021-2022"
    profiles = pq.read_table(tables / "column_profiles.parquet").to_pylist()
    assert next(row for row in profiles if row["field"] == "sc_present")["missing"] == 2
    assert (
        next(row for row in profiles if row["field"] == "people_present")["zero"] == 2
    )
    for line in (tables / "CHECKSUMS").read_text().splitlines():
        digest, name = line.split("  ")
        assert hashlib.sha256((tables / name).read_bytes()).hexdigest() == digest


def test_incomplete_collection_cannot_be_exported(collection):
    with closing(sqlite3.connect(collection / "collection.sqlite")) as db, db:
        db.execute("UPDATE requests SET status='error' WHERE level='gp'")
    with pytest.raises(ValueError, match="incomplete"):
        export_national(collection)
    assert main(["export", "--root", str(collection)]) == 1
    assert not (collection / "tables/manifest.json").exists()


def test_changed_capture_count_fails(collection):
    with closing(sqlite3.connect(collection / "collection.sqlite")) as db, db:
        db.execute("UPDATE requests SET rows=99 WHERE level='gp'")
    with pytest.raises(ValueError, match="row count"):
        export_national(collection)
    assert not (collection / "tables/manifest.json").exists()


def test_missing_capture_fails(collection):
    next((collection / "raw/current").glob("*.gz")).unlink()
    with pytest.raises(ValueError, match="Missing or damaged"):
        export_national(collection)


def test_export_without_queue_does_not_create_one(tmp_path):
    assert main(["export", "--root", str(tmp_path)]) == 1
    assert not (tmp_path / "collection.sqlite").exists()


def test_export_can_label_source_errors_but_never_pending_requests(collection):
    with closing(sqlite3.connect(collection / "collection.sqlite")) as db, db:
        db.execute(
            "UPDATE requests SET status='error',error='HTTP 500' WHERE level='state'"
        )
    report = export_national(collection, allow_source_errors=True)
    assert report["rows"] == 4
    assert report["all_discovered_requests_succeeded"] is False
    assert len(report["source_errors"]) == 5
    with closing(sqlite3.connect(collection / "collection.sqlite")) as db, db:
        db.execute("UPDATE requests SET status='pending' WHERE level='state'")
    with pytest.raises(ValueError, match="incomplete"):
        export_national(collection, allow_source_errors=True)
