"""Extract dated facilitator reports while preserving their original answers."""

import json
import re
from datetime import date

from bs4 import BeautifulSoup

ATTENDANCE = {
    "people": "people_present",
    "SC's": "sc_present",
    "ST's": "st_present",
    "SHG members": "shg_present",
    "women": "women_present",
}
BOOLEAN_QUESTIONS = {
    "Presentation and validation of Mission": "mission_antyodaya_presentation",
    "Presentation by SHG": "shg_presentation",
    "Review of current year fund": "funds_utilized_discussion",
    "Discussion on resource": "resource_discussion",
    "Discussion on Gaps": "gaps_discussion",
    "Resolution passed": "resolution_passed_recorded",
    "Quorum of the Sabha": "quorum_attended",
    "Mahila Sabha Held": "mahila_sabha_held",
    "Bal Sabha Held": "bal_sabha_held",
    "COVID appropriate behavior": "covid_behavior_discussed",
}


def normalized(text: str) -> str:
    """Collapse display whitespace without changing an answer's words."""
    return " ".join(text.split()).strip(" :")


def boolean_icon(cell):
    """Read yes/no icons independently of missing text."""
    if cell.select_one("i.fa-check, i.fa-check-square-o"):
        return True
    if cell.select_one("i.fa-times"):
        return False
    return None


def parse_feedback(html: str) -> list[dict]:
    """Parse the displayed form, retaining every labelled answer and table row."""
    soup = BeautifulSoup(html, "html.parser")
    form = soup.find("form", id="FACILITATOR_MODEL")
    if form is None:
        raise ValueError("Facilitator report form is absent")
    answers = []
    for label in form.find_all("label"):
        question = normalized(label.get_text(" ", strip=True))
        if not question:
            continue
        parent = label.parent
        text = normalized(parent.get_text(" ", strip=True))
        answer = text[len(question) :].strip(" :") if text.startswith(question) else ""
        cell = parent
        if not answer:
            sibling = parent.find_next_sibling("div")
            if sibling is not None and sibling.find("label") is None:
                cell = sibling
                answer = normalized(cell.get_text(" ", strip=True))
        answers.append(
            {
                "question": question,
                "text": answer or None,
                "boolean": boolean_icon(cell),
            }
        )
    counts = dict.fromkeys(ATTENDANCE.values())
    for source, target in ATTENDANCE.items():
        matching = [
            answer
            for answer in answers
            if re.match(
                rf"Number of {re.escape(source)} present\b",
                answer["question"],
                re.IGNORECASE,
            )
        ]
        if len(matching) > 1:
            raise ValueError(f"Repeated attendance question: {target}")
        if matching and matching[0]["text"] is not None:
            value = matching[0]["text"].replace(",", "")
            if not value.isdecimal():
                raise ValueError(f"Invalid attendance count: {target}={value}")
            counts[target] = int(value)
    raw_date = next(
        (
            answer["text"]
            for answer in answers
            if answer["question"] in {"Sabha Held On", "Gram Sabha Held On"}
        ),
        None,
    )
    parsed_date = None
    if raw_date is not None:
        try:
            day, month, year = (int(part) for part in raw_date.split("-"))
            parsed_date = date(year, month, day).isoformat()
        except ValueError:
            pass
    tables = []
    for table in form.find_all("table"):
        headers = [
            normalized(cell.get_text(" ", strip=True))
            for cell in table.select("thead th")
        ]
        for tr in table.select("tbody tr"):
            cells = [
                {
                    "text": normalized(td.get_text(" ", strip=True)) or None,
                    "boolean": boolean_icon(td),
                }
                for td in tr.find_all("td", recursive=False)
            ]
            if cells:
                tables.append({"headers": headers, "cells": cells})
    record = {
        **counts,
        **{
            target: next(
                (
                    answer["boolean"]
                    for answer in answers
                    if answer["question"].startswith(prefix)
                ),
                None,
            )
            for prefix, target in BOOLEAN_QUESTIONS.items()
        },
        "report_available": raw_date is not None
        or any(value is not None for value in counts.values()),
        "meeting_date_raw": raw_date,
        "meeting_date": parsed_date,
        "date_error": "unparseable_date" if raw_date and parsed_date is None else None,
        "feedback_type": next(
            (
                answer["text"]
                for answer in answers
                if answer["question"] == "Feedback Type"
            ),
            None,
        ),
        "answers": json.dumps(answers, ensure_ascii=False),
        "tables": json.dumps(tables, ensure_ascii=False),
        "document_links": json.dumps(
            [link["href"] for link in form.find_all("a", href=True)], ensure_ascii=False
        ),
    }
    record["attendance_inconsistent"] = any(
        record[field] is not None
        and record["people_present"] is not None
        and record[field] > record["people_present"]
        for field in ["sc_present", "st_present", "shg_present", "women_present"]
    )
    return [record]
