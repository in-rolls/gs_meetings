"""Collect dated meeting listings from current and archived official reports."""

import fcntl
import hashlib
import json
from datetime import date, datetime
from functools import partial
from pathlib import Path
from urllib.parse import urlencode

from gs_meetings.collect import add_request, open_queue, run_queue
from gs_meetings.fetch import Client
from gs_meetings.source import EDITIONS

FINANCIAL_YEARS = ["2022-2023", "2023-2024", "2024-2025", "2025-2026"]


def validate_meeting_rows(value: object) -> list[dict]:
    """Accept known listing contracts, retaining repeated meetings and raw dates."""
    if not isinstance(value, list):
        raise ValueError("Expected a JSON array of meeting report rows")
    for row in value:
        if not isinstance(row, dict):
            raise ValueError("Meeting report rows must be objects")
        code = row.get("stateCode", row.get("code", row.get("local_body_code")))
        name = row.get("stateName", row.get("name"))
        if type(code) is not int or code <= 0 or not isinstance(name, str):
            raise ValueError("Meeting report is missing its geographic code or name")
        date_field = next(
            (key for key in ["gram_sabha_date", "gramSabhaDate"] if key in row), None
        )
        if date_field:
            if row[date_field] is not None and not isinstance(row[date_field], str):
                raise ValueError("Meeting date must be text or null")
        elif not any(key in row for key in ["count", "gpCount"]):
            raise ValueError("Unknown meeting hierarchy contract")
        for key in ["count", "gpCount", "districtCount", "blockCount", "total"]:
            if (
                key in row
                and row[key] is not None
                and (type(row[key]) is not int or row[key] < 0)
            ):
                raise ValueError(f"Invalid meeting count: {key}")
    return value


def queue_report(
    db, context: dict, stage: str, page: str, parent=None, **params
) -> None:
    """Record the exact public request and the geography needed to interpret it."""
    context = {**context, "stage": stage}
    edition = context["edition"]
    if edition == "current":
        params["finYear"] = context["financial_year"]
    elif edition != "PPC2018":
        params["meetingType"] = context["requested_meeting_type"]
    url = f"https://gpdp.nic.in/{EDITIONS[edition]}{page}"
    if params:
        url += "?" + urlencode(params)
    level = {
        "states": "state",
        "hierarchy": "district",
        "blocks": "block",
        "meetings": "gp",
    }[stage]
    add_request(db, edition, level, context, parent, url=url)


def meeting_queue(root: Path):
    """Seed four current financial years and the four historical editions."""
    db = open_queue(root, seed_summaries=False)
    for year in FINANCIAL_YEARS:
        queue_report(
            db,
            {
                "edition": "current",
                "financial_year": year,
                "requested_meeting_type": None,
            },
            "states",
            "stateGSHeldReport.html",
        )
    for edition in EDITIONS:
        if edition == "current":
            continue
        types = [None] if edition == "PPC2018" else ["S", "M"]
        for meeting_type in types:
            queue_report(
                db,
                {
                    "edition": edition,
                    "financial_year": None,
                    "requested_meeting_type": meeting_type,
                },
                "states",
                "stateGSHeldReport.html",
            )
    db.commit()
    return db


def hierarchy_path(context: dict, row: dict, category: str | None) -> dict:
    """Append source hierarchy without inventing an administrative crosswalk."""
    path = json.loads(context.get("hierarchy", "[]"))
    code = row.get("code", row.get("local_body_code"))
    path.append({"code": str(code), "name": row["name"], "level": category})
    return {
        **context,
        "hierarchy": json.dumps(path, ensure_ascii=False),
        "parent_code": str(code),
        "parent_level": category,
    }


def meeting_children(db, task: dict, rows: list[dict]) -> None:
    """Follow modern child-level routing and the archived report controller."""
    context = json.loads(task["context"])
    current = context["edition"] == "current"
    stage = context["stage"]
    if not rows or any(
        "gram_sabha_date" in row or "gramSabhaDate" in row for row in rows
    ):
        if rows and not all(
            "gram_sabha_date" in row or "gramSabhaDate" in row for row in rows
        ):
            raise ValueError("Mixed hierarchy and meeting rows")
        return
    if stage == "meetings":
        raise ValueError("Expected dated meetings, received hierarchy rows")
    seen = {}
    for row in rows:
        code = row.get("stateCode", row.get("code", row.get("local_body_code")))
        encoded = json.dumps(row, sort_keys=True)
        if seen.get(code) == encoded:
            continue
        if code in seen:
            raise ValueError(f"Duplicate hierarchy code {code}")
        seen[code] = encoded
        if stage == "states":
            if code in {4, 7}:
                continue
            path = {
                **context,
                "state_code": str(code),
                "state_name": row.get("stateName", row.get("name")),
                "hierarchy": "[]",
                "parent_code": str(code),
                "parent_level": "S",
            }
            if current:
                for scope in ["G", "Z", "B"]:
                    queue_report(
                        db,
                        {**path, "report_scope": scope},
                        "hierarchy",
                        "gramSabhaHeldDetails.html",
                        row,
                        stateId=code,
                        code=0,
                        level=scope,
                    )
            elif row["level"] == "U":
                queue_report(
                    db,
                    {**path, "report_scope": "G"},
                    "meetings",
                    "gsHeldDetailsReport.html",
                    row,
                    stateId=code,
                    code=code,
                )
            else:
                queue_report(
                    db,
                    {**path, "report_scope": "G", "listing_level": row["level"]},
                    "hierarchy",
                    "districtGSHeldReport.html",
                    row,
                    stateId=code,
                    levelType=row["level"],
                    levelCount=row["levelCount"],
                )
            continue
        state = int(context["state_code"])
        if current:
            level, child = row.get("level"), row.get("childlevel")
            if context["report_scope"] == "B" and "level" not in row:
                level, child = "D", None
            if level not in {"D", "I", "V"} or child not in {"I", "V", None}:
                db.execute(
                    "INSERT OR IGNORE INTO coverage_gaps VALUES (?,?,?)",
                    (
                        task["url"],
                        json.dumps(row, ensure_ascii=False),
                        "Unknown dated-report hierarchy",
                    ),
                )
                continue
            path = hierarchy_path(context, row, level)
            scope = context["report_scope"]
            request_level = (
                "V" if state == 17 and level == "I" and scope == "G" else scope
            )
            queue_report(
                db,
                path,
                "meetings" if child == "V" or child is None else "blocks",
                "gramSabhaHeldDetails.html",
                row,
                stateId=state,
                code=code,
                level=request_level,
            )
        else:
            level = row.get("level")
            if level is None and state == 35:
                level = "V"
            if level not in {"D", "I", "V"}:
                db.execute(
                    "INSERT OR IGNORE INTO coverage_gaps VALUES (?,?,?)",
                    (
                        task["url"],
                        json.dumps(row, ensure_ascii=False),
                        "Unknown archived dated-report hierarchy",
                    ),
                )
                continue
            path = hierarchy_path(context, row, context.get("listing_level"))
            path["listing_level"] = level
            if level == "V":
                queue_report(
                    db,
                    path,
                    "meetings",
                    "gsHeldDetailsReport.html",
                    row,
                    stateId=state,
                    code=code,
                )
            else:
                queue_report(
                    db,
                    path,
                    "blocks",
                    "blockGSHeldReport.html",
                    row,
                    stateId=state,
                    zpCode=code,
                )


def collect_meetings(root: Path, workers=4, retries=2, max_requests=None) -> dict:
    """Collect all discovered dated listings with the shared durable queue."""
    root.mkdir(parents=True, exist_ok=True)
    with (root / "collection.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return run_queue(
            root,
            workers,
            retries,
            max_requests,
            initialize=meeting_queue,
            expand=meeting_children,
            client_factory=partial(Client, validator=validate_meeting_rows),
        )


def parse_meetings(
    rows: list[dict], context: dict, url: str, fetched_at: str
) -> list[dict]:
    """Keep source rows distinct; flag unparseable or out-of-period dates."""
    result = []
    for ordinal, row in enumerate(validate_meeting_rows(rows), start=1):
        if "gram_sabha_date" not in row and "gramSabhaDate" not in row:
            raise ValueError("Expected dated records")
        raw_date = row.get("gram_sabha_date", row.get("gramSabhaDate"))
        meeting_date = None
        date_error = None
        if raw_date is not None:
            try:
                day, month, year_number = (int(part) for part in raw_date.split("-"))
                meeting_date = date(year_number, month, day)
            except ValueError:
                date_error = "unparseable_date"
        year = context.get("financial_year")
        outside = None
        if year and meeting_date:
            start = int(year[:4])
            outside = not (date(start, 4, 1) <= meeting_date < date(start + 1, 4, 1))
        result.append(
            {
                "observation_id": hashlib.sha256(
                    f"{url}#{ordinal}".encode()
                ).hexdigest(),
                "edition": context["edition"],
                "financial_year": year,
                "state_code": context["state_code"],
                "state_name": context["state_name"],
                "hierarchy": context.get("hierarchy", "[]"),
                "report_scope": context.get("report_scope"),
                "local_body_code": str(row.get("code", row.get("local_body_code"))),
                "local_body_name": row["name"],
                "source_row_id": str(row["id"]) if row.get("id") is not None else None,
                "row_ordinal": ordinal,
                "meeting_date_raw": raw_date,
                "meeting_date": meeting_date,
                "date_error": date_error,
                "outside_financial_year": outside,
                "meeting_type": row.get("meeting_type", row.get("meetingType")),
                "requested_meeting_type": context.get("requested_meeting_type"),
                "source_url": url,
                "fetched_at": datetime.fromisoformat(fetched_at),
                "raw_row": json.dumps(row, ensure_ascii=False),
            }
        )
    return result
