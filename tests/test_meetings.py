"""Check dated records and modern/archive hierarchy routes against source samples."""

import gzip
import hashlib
import json
from datetime import date
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pyarrow.parquet as pq
import pytest

from gs_meetings.collect import open_queue
from gs_meetings.meeting_export import export_meetings
from gs_meetings.meetings import (
    meeting_children,
    meeting_queue,
    parse_meetings,
    queue_report,
    validate_meeting_rows,
)

FIXTURES = Path(__file__).parent / "fixtures"


def fixture(name):
    return json.loads((FIXTURES / f"{name}.json").read_text())


def test_dated_source_rows_preserve_meeting_date_and_original_row_id():
    context = {
        "edition": "current",
        "financial_year": "2024-2025",
        "state_code": "6",
        "state_name": "HARYANA",
        "report_scope": "G",
    }
    records = parse_meetings(
        fixture("annual_gp"),
        context,
        "https://example.org/report",
        "2026-09-11T10:00:00+00:00",
    )
    assert records[0]["meeting_date"] == date(2024, 11, 22)
    assert records[0]["local_body_name"] == "Adho Majra"
    assert records[0]["source_row_id"] == "1"
    assert records[0]["outside_financial_year"] is False
    archived = parse_meetings(
        fixture("archive_ppc_gp"),
        {**context, "edition": "PPC", "financial_year": None},
        "https://example.org/archive",
        "2026-09-11T10:00:00+00:00",
    )
    assert archived[0]["meeting_date"] == date(2021, 12, 1)
    assert archived[0]["financial_year"] is None
    assert archived[0]["outside_financial_year"] is None


def test_invalid_and_out_of_year_dates_are_preserved_and_flagged():
    rows = fixture("annual_gp")
    rows[0]["gram_sabha_date"] = "31-02-2024"
    rows[1]["gram_sabha_date"] = "31-03-2024"
    rows.append(dict(rows[1]))
    context = {
        "edition": "current",
        "financial_year": "2024-2025",
        "state_code": "6",
        "state_name": "HARYANA",
    }
    records = parse_meetings(
        rows, context, "https://example.org/report", "2026-09-11T10:00:00+00:00"
    )
    assert records[0]["meeting_date"] is None
    assert records[0]["meeting_date_raw"] == "31-02-2024"
    assert records[0]["date_error"] == "unparseable_date"
    assert records[1]["outside_financial_year"] is True
    assert len({row["observation_id"] for row in records}) == 3


def test_current_hierarchy_descends_to_block_meetings(tmp_path):
    db = open_queue(tmp_path, seed_summaries=False)
    context = {
        "edition": "current",
        "financial_year": "2024-2025",
        "state_code": "6",
        "state_name": "HARYANA",
        "report_scope": "G",
    }
    queue_report(
        db,
        context,
        "hierarchy",
        "gramSabhaHeldDetails.html",
        stateId=6,
        code=0,
        level="G",
    )
    task = dict(db.execute("SELECT * FROM requests").fetchone())
    meeting_children(db, task, fixture("annual_state_6")[:1])
    district = dict(db.execute("SELECT * FROM requests WHERE level='block'").fetchone())
    meeting_children(db, district, fixture("annual_district_58")[:1])
    leaf = dict(db.execute("SELECT * FROM requests WHERE level='gp'").fetchone())
    assert parse_qs(urlparse(leaf["url"]).query)["code"] == ["1030"]
    assert json.loads(leaf["context"])["parent_level"] == "I"
    meeting_children(db, leaf, fixture("annual_gp"))
    assert db.execute("SELECT count(*) FROM requests").fetchone()[0] == 3
    db.close()


def test_meghalaya_uses_its_source_code_field_and_village_route(tmp_path):
    db = open_queue(tmp_path, seed_summaries=False)
    context = {
        "edition": "current",
        "financial_year": "2024-2025",
        "state_code": "17",
        "state_name": "MEGHALAYA",
        "report_scope": "G",
        "stage": "hierarchy",
    }
    meeting_children(
        db, {"context": json.dumps(context)}, fixture("annual_state_17")[:1]
    )
    task = dict(db.execute("SELECT * FROM requests").fetchone())
    assert parse_qs(urlparse(task["url"]).query)["code"] == ["740"]
    meeting_children(
        db,
        task,
        [{"code": 123, "name": "Block", "level": "I", "childlevel": "V", "count": 0}],
    )
    leaf = dict(db.execute("SELECT * FROM requests WHERE level='gp'").fetchone())
    assert parse_qs(urlparse(leaf["url"]).query)["level"] == ["V"]
    db.close()


def test_all_periods_are_seeded_without_inventing_archive_years(tmp_path):
    db = meeting_queue(tmp_path)
    tasks = [dict(row) for row in db.execute("SELECT * FROM requests")]
    assert len(tasks) == 11
    assert sum(task["edition"] == "current" for task in tasks) == 4
    assert all(
        json.loads(task["context"])["financial_year"] is None
        for task in tasks
        if task["edition"] != "current"
    )
    oldest = next(task for task in tasks if task["edition"] == "PPC2018")
    assert "?" not in oldest["url"]
    meeting_children(db, oldest, fixture("archive_2018_states")[:1])
    route = dict(db.execute("SELECT * FROM requests WHERE level='district'").fetchone())
    assert parse_qs(urlparse(route["url"]).query)["levelCount"] == ["3"]
    db.close()


@pytest.mark.parametrize(
    "bad", [{"error": "server error"}, [{"code": 6, "name": "Haryana"}], [True]]
)
def test_invalid_reports_fail(bad):
    with pytest.raises(ValueError, match=r"Expected|Unknown|objects"):
        validate_meeting_rows(bad)


def test_unknown_archived_row_keeps_working_siblings_and_records_gap(tmp_path):
    db = open_queue(tmp_path, seed_summaries=False)
    task = {
        "url": "https://example.org/hierarchy",
        "context": json.dumps(
            {
                "edition": "PPC",
                "stage": "hierarchy",
                "state_code": "19",
                "requested_meeting_type": "S",
                "listing_level": "D",
                "report_scope": "G",
            }
        ),
    }
    meeting_children(
        db,
        task,
        [
            {"code": 1, "name": "Working district", "level": "I", "count": 2},
            {"code": 2, "name": "Missing hierarchy", "level": None, "count": 0},
        ],
    )
    assert db.execute("SELECT count(*) FROM requests").fetchone()[0] == 1
    assert db.execute("SELECT count(*) FROM coverage_gaps").fetchone()[0] == 1
    db.close()


def test_meeting_export_roundtrip_preserves_duplicates_and_reports_failure(tmp_path):
    db = open_queue(tmp_path, seed_summaries=False)
    context = {
        "edition": "current",
        "financial_year": "2024-2025",
        "state_code": "6",
        "state_name": "HARYANA",
        "report_scope": "G",
    }
    queue_report(
        db,
        context,
        "meetings",
        "gramSabhaHeldDetails.html",
        stateId=6,
        code=1030,
        level="G",
    )
    task = dict(db.execute("SELECT * FROM requests").fetchone())
    rows = fixture("annual_gp")
    rows.append(dict(rows[0]))
    key = hashlib.sha256(task["url"].encode()).hexdigest()
    path = tmp_path / "raw/current" / f"{key}.jsonl.gz"
    path.parent.mkdir(parents=True)
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        json.dump(
            {
                "url": task["url"],
                "edition": "current",
                "ok": True,
                "done": True,
                "body": json.dumps(rows),
                "fetched_at": "2026-09-11T10:00:00+00:00",
            },
            stream,
        )
    db.execute("UPDATE requests SET status='done', rows=3")
    db.commit()
    db.close()
    report = export_meetings(tmp_path)
    assert report["rows"] == 3
    assert report["duplicate_candidate_event_keys"] == 1
    result = pq.read_table(tmp_path / "tables/meetings.parquet").to_pylist()
    assert result[0]["meeting_date"] == date(2024, 11, 22)
    assert len({row["observation_id"] for row in result}) == 3
    assert report["date_ranges"] == {"current:2024-2025": ["2024-11-19", "2024-11-22"]}
