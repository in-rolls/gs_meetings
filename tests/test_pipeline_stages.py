"""Verify recovery provenance and prevent export after an interrupted stage."""

import json

import pytest

from gs_meetings.collect import open_queue
from gs_meetings.meetings import queue_report
from gs_meetings.pipeline import assert_drained, recover_summary_routes


def test_summary_recovery_keeps_source_route_and_does_not_invent_parent_totals(
    tmp_path,
):
    meetings = tmp_path / "meetings"
    summaries = tmp_path / "summaries"
    db = open_queue(meetings, seed_summaries=False)
    context = {
        "edition": "current",
        "financial_year": "2024-2025",
        "state_code": "6",
        "state_name": "HARYANA",
        "parent_code": "1030",
        "parent_level": "I",
        "report_scope": "G",
        "hierarchy": json.dumps(
            [
                {"code": "58", "name": "Ambala", "level": "D"},
                {"code": "1030", "name": "Ambala-I", "level": "I"},
            ]
        ),
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
    db.execute("UPDATE requests SET status='done',rows=1")
    db.commit()
    db.close()
    assert recover_summary_routes(summaries, meetings) == 1
    assert recover_summary_routes(summaries, meetings) == 0
    db = open_queue(summaries)
    task = dict(db.execute("SELECT * FROM requests WHERE level='gp'").fetchone())
    route = json.loads(task["context"])
    assert route["district_code"] == "58"
    assert route["block_code"] == "1030"
    assert "gramSabhaHeldDetails" in route["routing_source_url"]
    assert route["routing_source_financial_year"] == "2024-2025"
    assert all(value is None for value in json.loads(task["parent"]).values())
    db.close()


def test_pending_work_prevents_advancing_a_pipeline(tmp_path):
    db = open_queue(tmp_path)
    db.close()
    with pytest.raises(ValueError, match="requests remain"):
        assert_drained(tmp_path)
