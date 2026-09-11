"""Check event-level attendance against real current and historical forms."""

import gzip
import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from gs_meetings.collect import open_queue
from gs_meetings.feedback import parse_feedback
from gs_meetings.feedback_collect import feedback_url, seed_feedback
from gs_meetings.feedback_export import export_feedback, matching_date
from gs_meetings.meetings import queue_report

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.mark.parametrize(
    ("name", "attendance", "meeting_date"),
    [
        ("feedback_detail_current", [28, 7, 0, 3, 3], "2024-11-22"),
        ("feedback_ppc", [20, 5, 0, 5, 5], "2021-12-01"),
        ("feedback_2018", [35, 12, 0, 6, 12], "2018-10-05"),
    ],
)
def test_feedback_counts_and_date(name, attendance, meeting_date):
    record = parse_feedback((FIXTURES / f"{name}.html").read_text())[0]
    assert [
        record[key]
        for key in [
            "people_present",
            "sc_present",
            "st_present",
            "shg_present",
            "women_present",
        ]
    ] == attendance
    assert record["meeting_date"] == meeting_date
    assert record["report_available"] is True
    assert record["feedback_type"] == (None if name == "feedback_2018" else "Sabha")
    assert record["attendance_inconsistent"] is False
    answers = json.loads(record["answers"])
    mission = next(
        row
        for row in answers
        if row["question"] == "Presentation and validation of Mission Antyodaya data"
    )
    assert mission["boolean"] is (name != "feedback_ppc")
    assert json.loads(record["tables"])


def test_http_200_error_page_is_not_zero_attendance():
    with pytest.raises(ValueError, match="form is absent"):
        parse_feedback((FIXTURES / "feedback_absent.html").read_text())


def test_blank_form_keeps_missing_values():
    record = parse_feedback(
        '<form id="FACILITATOR_MODEL"><div><label>Sabha Held On :</label></div></form>'
    )[0]
    assert record["report_available"] is False
    assert record["people_present"] is None
    assert record["meeting_date"] is None


def test_feedback_seeding_deduplicates_requests_but_keeps_each_source_link(tmp_path):
    source = tmp_path / "meetings"
    target = tmp_path / "feedback"
    db = open_queue(source, seed_summaries=False)
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
    rows = json.loads((FIXTURES / "annual_gp.json").read_text())
    rows.append(dict(rows[0]))
    key = hashlib.sha256(task["url"].encode()).hexdigest()
    path = source / "raw/current" / f"{key}.jsonl.gz"
    path.parent.mkdir(parents=True)
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        json.dump(
            {
                "url": task["url"],
                "edition": "current",
                "body": json.dumps(rows),
                "ok": True,
                "done": True,
                "fetched_at": "2026-09-11T10:00:00+00:00",
            },
            stream,
        )
    db.execute("UPDATE requests SET status='done',rows=3")
    db.commit()
    db.close()
    for _ in range(2):
        db = seed_feedback(target, meetings_root=source)
        assert db.execute("SELECT count(*) FROM requests").fetchone()[0] == 2
        assert db.execute("SELECT count(*) FROM feedback_links").fetchone()[0] == 3
        db.close()
    db = open_queue(target, seed_summaries=False)
    for task in db.execute("SELECT * FROM requests").fetchall():
        key = hashlib.sha256(task["url"].encode()).hexdigest()
        path = target / "raw/current" / f"{key}.jsonl.gz"
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wt", encoding="utf-8") as stream:
            json.dump(
                {
                    "url": task["url"],
                    "edition": "current",
                    "ok": True,
                    "done": True,
                    "fetched_at": "2026-09-11T10:00:00+00:00",
                    "body": (FIXTURES / "feedback_detail_current.html").read_text(),
                },
                stream,
            )
    db.execute("UPDATE requests SET status='done',rows=1")
    db.commit()
    db.close()
    report = export_feedback(target)
    assert report["rows"]["feedback"] == 2
    assert report["rows"]["feedback_links"] == 3
    assert report["issues"]["date_mismatches"] == 1
    assert report["issues"]["ambiguous_link_rows"] == 2
    assert pq.read_table(target / "tables/feedback.parquet").column(
        "people_present"
    ).to_pylist() == [28, 28]
    assert report["rows"]["feedback_answers"] > 10
    assert report["rows"]["feedback_tables"] > 10


def test_oldest_feedback_link_omits_date_and_keeps_missing_parent_names():
    record = {
        "edition": "PPC2018",
        "local_body_code": "27783",
        "local_body_name": "ADHO MAJRA",
        "meeting_date_raw": "05-10-2018",
        "hierarchy": "[]",
    }
    url = feedback_url(record)
    assert "date=" not in url
    assert "gpCode=27783" in url
    assert "dpName=null" in url


def test_date_link_uses_parsed_dates_and_leaves_invalid_values_unmatched():
    assert matching_date("1-12-2021", "01-12-2021") is True
    assert matching_date("01-12-2021", "02-12-2021") is False
    assert matching_date("31-02-2021", "28-02-2021") is None
