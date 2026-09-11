"""Finish collection stages, recover source routes and export their deliverables."""

import json
import sqlite3
from contextlib import closing
from pathlib import Path

from gs_meetings.collect import add_request, collect, open_queue
from gs_meetings.export import export_national
from gs_meetings.feedback_collect import collect_feedback
from gs_meetings.feedback_export import export_feedback
from gs_meetings.fetch import atomic_json
from gs_meetings.meeting_export import export_meetings
from gs_meetings.meetings import collect_meetings
from gs_meetings.source import METRICS, endpoint


def recover_summary_routes(root: Path, meetings_root: Path) -> int:
    """Use verified meeting-report parent codes to recover missing summary routes."""
    with closing(sqlite3.connect(meetings_root / "collection.sqlite")) as source:
        source.row_factory = sqlite3.Row
        tasks = source.execute(
            "SELECT * FROM requests WHERE status='done' AND level='gp' "
            "ORDER BY url DESC"
        ).fetchall()
    db = open_queue(root)
    try:
        existing = {row[0] for row in db.execute("SELECT url FROM requests")}
        added = 0
        for task in tasks:
            route = json.loads(task["context"])
            if route.get("report_scope") != "G" or route.get("stage") != "meetings":
                continue
            state, parent = route["state_code"], route.get("parent_code")
            if not parent:
                continue
            url = endpoint(task["edition"], "gp", stateId=int(state), code=int(parent))
            if url in existing:
                continue
            category = {"S": "state", "D": "district", "I": "block"}.get(
                route.get("parent_level")
            )
            if category is None:
                continue
            context = {
                "edition": task["edition"],
                "state_code": state,
                "state_name": route["state_name"],
                "parent_code": parent,
                "parent_level": category,
                "routing_source_url": task["url"],
                "routing_source_financial_year": route.get("financial_year"),
            }
            for ancestor in json.loads(route.get("hierarchy", "[]")):
                level = {"D": "district", "I": "block"}.get(ancestor["level"])
                if level:
                    context[f"{level}_code"] = ancestor["code"]
                    context[f"{level}_name"] = ancestor["name"]
            add_request(
                db, task["edition"], "gp", context, dict.fromkeys(METRICS), url=url
            )
            existing.add(url)
            added += 1
        db.commit()
        return added
    finally:
        db.close()


def assert_drained(root: Path) -> None:
    """Do not continue a pipeline that stopped with pending or running work."""
    with closing(sqlite3.connect(root / "collection.sqlite")) as db:
        remaining = db.execute(
            "SELECT count(*) FROM requests WHERE status NOT IN ('done','error')"
        ).fetchone()[0]
    if remaining:
        raise ValueError(
            f"{root}: {remaining} requests remain; resume collection first"
        )


def collect_all(root: Path, workers: int = 16, retries: int = 2) -> dict:
    """Run complete resumable stages; make a second pass over source failures."""
    root.mkdir(parents=True, exist_ok=True)
    status = {"stage": "summaries", "complete": False}

    def stage(name: str) -> None:
        status["stage"] = name
        atomic_json(root / "pipeline-progress.json", status)

    stage("summaries")
    collect(root / "national", workers, retries)
    assert_drained(root / "national")
    stage("dated_meetings")
    for _ in range(2):
        collect_meetings(root / "meetings", workers, retries)
        assert_drained(root / "meetings")
    stage("summary_route_recovery")
    status["recovered_summary_requests"] = recover_summary_routes(
        root / "national", root / "meetings"
    )
    collect(root / "national", workers, retries)
    assert_drained(root / "national")
    stage("summary_and_meeting_exports")
    status["summaries"] = export_national(root / "national", allow_source_errors=True)
    status["meetings"] = export_meetings(root / "meetings", allow_source_errors=True)
    stage("facilitator_reports")
    for _ in range(2):
        collect_feedback(root / "feedback", root / "meetings", workers, retries)
        assert_drained(root / "feedback")
    stage("feedback_export")
    status["feedback"] = export_feedback(root / "feedback", allow_source_errors=True)
    status["complete"] = True
    stage("finished")
    return status
