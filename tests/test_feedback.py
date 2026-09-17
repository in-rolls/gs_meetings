"""Check event-level attendance against real current and historical forms."""

import gzip
import hashlib
import json
import sqlite3
from contextlib import closing
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from gs_meetings.collect import open_queue
from gs_meetings.feedback import FORM_ABSENT, parse_feedback
from gs_meetings.feedback_collect import feedback_url, seed_feedback
from gs_meetings.feedback_export import export_feedback, matching_date
from gs_meetings.meeting_export import export_meetings
from gs_meetings.meetings import queue_report

FIXTURES = Path(__file__).parent / "fixtures"
BLANK_FORM = (
    '<form id="FACILITATOR_MODEL"><div><label>Sabha Held On :</label></div></form>'
)
NETWORK_ERROR = "HTTPSConnectionPool(host='gpdp.nic.in', port=443): Read timed out."


def write_capture(path: Path, url: str, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        json.dump(
            {
                "url": url,
                "edition": "current",
                "body": body,
                "ok": True,
                "done": True,
                "fetched_at": "2026-09-11T10:00:00+00:00",
            },
            stream,
        )


def seeded_feedback_queue(tmp_path: Path, rows: list[dict]) -> tuple[Path, Path]:
    """Export one Haryana GP listing for 2024-2025 and seed its feedback queue."""
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
    key = hashlib.sha256(task["url"].encode()).hexdigest()
    write_capture(
        source / "raw/current" / f"{key}.jsonl.gz", task["url"], json.dumps(rows)
    )
    db.execute("UPDATE requests SET status='done',rows=?", (len(rows),))
    db.commit()
    db.close()
    export_meetings(source)
    seed_feedback(target, meetings_root=source).close()
    return source, target


def resolve_feedback(target: Path, outcomes: dict[str, tuple[str, str]]) -> None:
    """Mark each queued report done with HTML or failed with an error message."""
    db = open_queue(target, seed_summaries=False)
    for task in db.execute("SELECT * FROM requests").fetchall():
        code = json.loads(task["context"])["local_body_code"]
        kind, value = outcomes[code]
        if kind == "done":
            key = hashlib.sha256(task["url"].encode()).hexdigest()
            write_capture(
                target / "raw/current" / f"{key}.jsonl.gz", task["url"], value
            )
            db.execute(
                "UPDATE requests SET status='done',rows=1 WHERE url=?", (task["url"],)
            )
        else:
            db.execute(
                "UPDATE requests SET status='error',error=? WHERE url=?",
                (value, task["url"]),
            )
    db.commit()
    db.close()


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


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        (
            "feedback_detail_current",
            {
                "funds_utilized_discussion": True,
                "resource_discussion": True,
                "gaps_discussion": True,
                "resolution_passed_recorded": True,
                "quorum_attended": True,
                "mahila_sabha_held": False,
                "bal_sabha_held": False,
                "covid_behavior_discussed": True,
            },
        ),
        (
            "feedback_ppc",
            {
                "funds_utilized_discussion": True,
                "resource_discussion": True,
                "gaps_discussion": True,
                "resolution_passed_recorded": True,
                "quorum_attended": None,
                "mahila_sabha_held": None,
                "bal_sabha_held": None,
                "covid_behavior_discussed": True,
            },
        ),
        (
            "feedback_2018",
            {
                "funds_utilized_discussion": False,
                "resource_discussion": True,
                "gaps_discussion": False,
                "resolution_passed_recorded": True,
                "quorum_attended": None,
                "mahila_sabha_held": None,
                "bal_sabha_held": None,
                "covid_behavior_discussed": None,
            },
        ),
    ],
)
def test_discussion_indicators_with_and_without_labels(name, expected):
    """Archive editions render these questions as unlabelled text beside an icon."""
    record = parse_feedback((FIXTURES / f"{name}.html").read_text())[0]
    assert {key: record[key] for key in expected} == expected
    questions = [row["question"] for row in json.loads(record["answers"])]
    assert "Review of current year fund activities and fund utilized" in questions


@pytest.mark.parametrize(
    ("name", "images"),
    [
        (
            "feedback_detail_current",
            [
                {"src": "file/image/8990473", "caption": "Gram Sabha Image"},
                {
                    "src": "file/image/8990472",
                    "caption": "Public Information Board Image",
                },
            ],
        ),
        (
            "feedback_ppc",
            [{"src": "file/image/4814603", "caption": "Gram Sabha Image"}],
        ),
        (
            "feedback_2018",
            [
                {"src": "file/image/810091", "caption": "Gram Sabha Image"},
                {
                    "src": "file/image/810092",
                    "caption": "Public Information Board Image",
                },
            ],
        ),
    ],
)
def test_report_images_are_retained(name, images):
    record = parse_feedback((FIXTURES / f"{name}.html").read_text())[0]
    assert json.loads(record["images"]) == images


def test_textarea_lists_keep_line_breaks():
    record = parse_feedback((FIXTURES / "feedback_ppc.html").read_text())[0]
    answers = {row["question"]: row["text"] for row in json.loads(record["answers"])}
    assert answers["Mapping of Sankalp to Focus Areas"] == "Drinking water\nEducation"
    assert answers["Sankalp of Gram Panchayat"] == "Sankalp of Gram Panchayat"
    record = parse_feedback(
        '<form id="FACILITATOR_MODEL"><div class="row"><div><label>Sankalp</label>'
        "</div><div><textarea>Sankalp taken on :\r\nNo Poverty\r\n</textarea></div>"
        "</div></form>"
    )[0]
    assert json.loads(record["answers"])[0]["text"] == "Sankalp taken on :\nNo Poverty"


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
    rows = json.loads((FIXTURES / "annual_gp.json").read_text())
    rows.append(dict(rows[0]))
    source, target = seeded_feedback_queue(tmp_path, rows)
    for _ in range(2):
        db = seed_feedback(target, meetings_root=source)
        assert db.execute("SELECT count(*) FROM requests").fetchone()[0] == 2
        assert db.execute("SELECT count(*) FROM feedback_links").fetchone()[0] == 3
        db.close()
    html = (FIXTURES / "feedback_detail_current.html").read_text()
    resolve_feedback(target, {"27783": ("done", html), "27784": ("done", html)})
    report = export_feedback(target)
    assert report["rows"]["feedback"] == 2
    assert report["rows"]["feedback_links"] == 3
    assert report["issues"]["date_mismatches"] == 1
    assert report["issues"]["ambiguous_link_rows"] == 2
    assert pq.read_table(target / "tables/feedback.parquet").column(
        "people_present"
    ).to_pylist() == [28, 28]
    images = pq.read_table(target / "tables/feedback.parquet").column("images")
    assert [json.loads(value)[0]["src"] for value in images.to_pylist()] == [
        "file/image/8990473",
        "file/image/8990473",
    ]
    assert report["rows"]["feedback_answers"] > 10
    assert report["rows"]["feedback_tables"] > 10


def test_absent_forms_are_gp_year_outcomes_not_source_errors(tmp_path):
    rows = [
        {"id": 1, "code": 27783, "name": "A", "gram_sabha_date": "22-11-2024"},
        {"id": 2, "code": 27784, "name": "B", "gram_sabha_date": "19-11-2024"},
        {"id": 3, "code": 27785, "name": "C", "gram_sabha_date": "20-11-2024"},
        {"id": 4, "code": 27786, "name": "D", "gram_sabha_date": None},
        {"id": 5, "code": 27787, "name": "E", "gram_sabha_date": "21-11-2024"},
    ]
    _, target = seeded_feedback_queue(tmp_path, rows)
    resolve_feedback(
        target,
        {
            "27783": ("done", (FIXTURES / "feedback_detail_current.html").read_text()),
            "27784": ("error", FORM_ABSENT),
            "27785": ("error", NETWORK_ERROR),
            "27787": ("done", BLANK_FORM),
        },
    )
    with pytest.raises(ValueError, match="source errors"):
        export_feedback(target)
    report = export_feedback(target, allow_source_errors=True)
    assert report["all_discovered_requests_succeeded"] is False
    assert report["source_errors"] == [
        {"url": feedback_url_for(target, "27785"), "error": NETWORK_ERROR}
    ]
    assert report["outcomes"] == {
        "report": 2,
        "form_absent": 1,
        "fetch_error": 1,
        "not_requested": 1,
    }
    assert report["outcomes_by_edition"] == {"current": report["outcomes"]}
    assert report["forms_absent_by_state"] == {"6": 1}
    links = pq.read_table(target / "tables/feedback_links.parquet").to_pylist()
    by_code = {row["local_body_code"]: row for row in links}
    assert [row["outcome"] for row in links] == [
        "report",
        "form_absent",
        "fetch_error",
        "not_requested",
        "report",
    ]
    assert by_code["27784"]["request_error"] == FORM_ABSENT
    assert by_code["27783"]["request_error"] is None
    assert by_code["27786"]["feedback_url"] is None
    for row in links:
        assert (row["edition"], row["financial_year"], row["state_code"]) == (
            "current",
            "2024-2025",
            "6",
        )
        assert row["report_scope"] == "G"
    assert by_code["27784"]["meeting_date"].isoformat() == "2024-11-19"
    assert by_code["27786"]["meeting_date"] is None
    coverage = pq.read_table(target / "tables/feedback_coverage.parquet").to_pylist()
    assert [row["local_body_code"] for row in coverage] == [
        "27783",
        "27784",
        "27785",
        "27786",
        "27787",
    ]
    counts = {
        row["local_body_code"]: {
            key: row[key]
            for key in [
                "listed_meetings",
                "reports",
                "forms_absent",
                "fetch_errors",
                "not_requested",
                "unfilled_forms",
                "distinct_reports",
            ]
        }
        for row in coverage
    }
    zero = dict.fromkeys(counts["27783"], 0)
    assert counts["27783"] == {
        **zero,
        "listed_meetings": 1,
        "reports": 1,
        "distinct_reports": 1,
    }
    assert counts["27784"] == {**zero, "listed_meetings": 1, "forms_absent": 1}
    assert counts["27785"] == {**zero, "listed_meetings": 1, "fetch_errors": 1}
    assert counts["27786"] == {**zero, "listed_meetings": 1, "not_requested": 1}
    assert counts["27787"] == {
        **zero,
        "listed_meetings": 1,
        "reports": 1,
        "unfilled_forms": 1,
        "distinct_reports": 1,
    }
    assert all(
        (row["edition"], row["financial_year"], row["state_code"], row["report_scope"])
        == ("current", "2024-2025", "6", "G")
        for row in coverage
    )


def test_resuming_retries_fetch_errors_but_not_absent_forms(tmp_path):
    rows = [
        {"id": 1, "code": 27784, "name": "B", "gram_sabha_date": "19-11-2024"},
        {"id": 2, "code": 27785, "name": "C", "gram_sabha_date": "20-11-2024"},
    ]
    source, target = seeded_feedback_queue(tmp_path, rows)
    resolve_feedback(
        target, {"27784": ("error", FORM_ABSENT), "27785": ("error", NETWORK_ERROR)}
    )
    with closing(seed_feedback(target, meetings_root=source)) as db:
        found = dict(db.execute("SELECT error,status FROM requests"))
    assert found == {FORM_ABSENT: "error", NETWORK_ERROR: "pending"}


def feedback_url_for(target: Path, local_body_code: str) -> str:
    db = open_queue(target, seed_summaries=False)
    try:
        return next(
            task["url"]
            for task in db.execute("SELECT url,context FROM requests")
            if json.loads(task["context"])["local_body_code"] == local_body_code
        )
    finally:
        db.close()


def test_only_absent_forms_do_not_require_the_source_error_override(tmp_path):
    rows = [
        {"id": 1, "code": 27783, "name": "A", "gram_sabha_date": "22-11-2024"},
        {"id": 2, "code": 27784, "name": "B", "gram_sabha_date": "19-11-2024"},
    ]
    _, target = seeded_feedback_queue(tmp_path, rows)
    resolve_feedback(
        target,
        {
            "27783": ("done", (FIXTURES / "feedback_detail_current.html").read_text()),
            "27784": ("error", FORM_ABSENT),
        },
    )
    report = export_feedback(target)
    assert report["all_discovered_requests_succeeded"] is True
    assert report["source_errors"] == []
    assert report["outcomes"] == {"report": 1, "form_absent": 1}
    with closing(sqlite3.connect(target / "collection.sqlite")) as db, db:
        db.execute("UPDATE requests SET status='running' WHERE status='error'")
    with pytest.raises(ValueError, match="incomplete"):
        export_feedback(target)


def test_feedback_export_requires_the_matching_meetings_export(tmp_path):
    rows = [{"id": 1, "code": 27783, "name": "A", "gram_sabha_date": "22-11-2024"}]
    source, target = seeded_feedback_queue(tmp_path, rows)
    resolve_feedback(
        target,
        {"27783": ("done", (FIXTURES / "feedback_detail_current.html").read_text())},
    )
    meetings = source / "tables/meetings.parquet"
    exported = pq.read_table(meetings)
    pq.write_table(exported.slice(0, 0), meetings)
    with pytest.raises(ValueError, match="does not cover 1 linked"):
        export_feedback(target)
    pq.write_table(pa.concat_tables([exported, exported]), meetings)
    with pytest.raises(ValueError, match="repeats"):
        export_feedback(target)
    extra = exported.to_pylist()[0] | {"row_ordinal": 2}
    pq.write_table(
        pa.Table.from_pylist([*exported.to_pylist(), extra], schema=exported.schema),
        meetings,
    )
    with pytest.raises(ValueError, match="1 listing rows that feedback"):
        export_feedback(target)
    meetings.unlink()
    with pytest.raises(ValueError, match="Run the meetings export"):
        export_feedback(target)


def test_unfilled_forms_count_forms_not_the_rows_that_share_them(tmp_path):
    rows = [
        {"id": 1, "code": 27783, "name": "A", "gram_sabha_date": "22-11-2024"},
        {"id": 2, "code": 27783, "name": "A", "gram_sabha_date": "22-11-2024"},
        {"id": 3, "code": 27783, "name": "A", "gram_sabha_date": "22-11-2024"},
    ]
    _, target = seeded_feedback_queue(tmp_path, rows)
    resolve_feedback(target, {"27783": ("done", BLANK_FORM)})
    report = export_feedback(target)
    assert report["issues"]["unpopulated_forms"] == 1
    coverage = pq.read_table(target / "tables/feedback_coverage.parquet").to_pylist()
    assert coverage == [
        {
            "edition": "current",
            "financial_year": "2024-2025",
            "state_code": "6",
            "report_scope": "G",
            "local_body_code": "27783",
            "listed_meetings": 3,
            "reports": 3,
            "forms_absent": 0,
            "fetch_errors": 0,
            "not_requested": 0,
            "unfilled_forms": 1,
            "distinct_reports": 1,
        }
    ]


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
