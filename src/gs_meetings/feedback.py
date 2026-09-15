"""Extract dated facilitator reports while preserving their original answers."""

import json
import re
from datetime import date

from bs4 import BeautifulSoup

FORM_ABSENT = "Facilitator report form is absent"

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


def cell_text(cell) -> str:
    """Keep a textarea's line breaks: they are the only separator between items."""
    textarea = cell.find("textarea")
    if textarea is None:
        return normalized(cell.get_text(" ", strip=True))
    lines = (" ".join(line.split()) for line in textarea.get_text().splitlines())
    return "\n".join(line for line in lines if line)


def unlabelled_icon_row(node):
    """Return a question and its icon cell when rendered without a <label>.

    Archive editions (PPC, PPC2018, PPC2019) show the GPDP discussion questions
    as plain text beside a yes/no icon; the current portal wraps them in labels.
    """
    if "row" not in (node.get("class") or []):
        return None
    cells = node.find_all("div", recursive=False)
    if len(cells) != 2 or node.find(["label", "img", "textarea", "table"]):
        return None
    question = normalized(cells[0].get_text(" ", strip=True))
    if not question or normalized(cells[1].get_text(" ", strip=True)):
        return None
    return question, cells[1]


def parse_feedback(html: str) -> list[dict]:
    """Parse the displayed form, retaining every question, table row and image."""
    soup = BeautifulSoup(html, "html.parser")
    form = soup.find("form", id="FACILITATOR_MODEL")
    if form is None:
        raise ValueError(FORM_ABSENT)
    answers = []
    for node in form.find_all(["label", "div"]):
        if node.name == "div":
            row = unlabelled_icon_row(node)
            if row is not None:
                question, cell = row
                answers.append(
                    {"question": question, "text": None, "boolean": boolean_icon(cell)}
                )
            continue
        question = normalized(node.get_text(" ", strip=True))
        if not question:
            continue
        parent = node.parent
        text = normalized(parent.get_text(" ", strip=True))
        answer = text[len(question) :].strip(" :") if text.startswith(question) else ""
        cell = parent
        if not answer:
            sibling = parent.find_next_sibling("div")
            if sibling is not None and sibling.find("label") is None:
                cell = sibling
                answer = cell_text(cell)
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
        "images": json.dumps(
            [
                {
                    "src": image["src"],
                    "caption": normalized(image.parent.get_text(" ", strip=True))
                    or None,
                }
                for image in form.find_all("img", src=True)
            ],
            ensure_ascii=False,
        ),
    }
    record["attendance_inconsistent"] = any(
        record[field] is not None
        and record["people_present"] is not None
        and record[field] > record["people_present"]
        for field in ["sc_present", "st_present", "shg_present", "women_present"]
    )
    return [record]
