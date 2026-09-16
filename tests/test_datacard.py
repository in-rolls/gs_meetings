"""Generate the deposit's data card from the exported manifests, never by hand."""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from gs_meetings.datacard import write_data_card


def stage(root, name, manifest, tables):
    folder = root / name / "tables"
    folder.mkdir(parents=True)
    (folder / "manifest.json").write_text(json.dumps(manifest))
    for table, rows in tables.items():
        pq.write_table(pa.table({"n": list(range(rows))}), folder / f"{table}.parquet")


def test_data_card_reports_every_stage_from_its_manifest(tmp_path):
    stage(
        tmp_path,
        "national",
        {
            "rows": 3,
            "rows_by_edition": {"PPC": 2, "current": 1},
            "capture_start": "2026-09-11T19:18:36+00:00",
            "capture_end": "2026-09-11T23:13:45+00:00",
            "source_errors": [{"url": "u", "error": "500"}],
            "empty_responses": ["a", "b"],
            "measurement": "Source-reported aggregates",
            "coverage": "Completed discovered summary routes",
        },
        {"gp_reports": 3, "request_coverage": 5},
    )
    stage(
        tmp_path,
        "meetings",
        {
            "rows": 4,
            "rows_by_period": {"PPC:archive": 1, "current:2024-2025": 3},
            "date_ranges": {"current:2024-2025": ["2024-10-01", "2025-03-30"]},
            "source_errors": [],
            "empty_responses": [],
            "measurement": "Source meeting listings",
            "coverage": "Discovered report routes",
        },
        {"meetings": 4},
    )
    stage(
        tmp_path,
        "feedback",
        {
            "rows": {"feedback": 2, "feedback_links": 4, "feedback_coverage": 3},
            "outcomes": {"report": 2, "form_absent": 1, "fetch_error": 1},
            "outcomes_by_edition": {"current": {"report": 2, "form_absent": 1}},
            "forms_absent_by_state": {"27": 1},
            "source_errors": [{"url": "u", "error": "timeout"}],
            "outcome_rule": "form_absent is the portal's stable no-form response",
        },
        {"feedback": 2, "feedback_links": 4, "feedback_coverage": 3},
    )
    (tmp_path / "zenodo.json").write_text(
        json.dumps(
            {"parts": {"meetings-meetings.parquet": ["meetings-meetings-01.parquet"]}}
        )
    )
    schema = tmp_path / "SCHEMA.md"
    schema.write_text("# Schema\n")
    report = write_data_card(tmp_path, schema=schema, version="0.3.1")
    card = (tmp_path / "deposit" / "README_DATA.md").read_text()
    assert (tmp_path / "deposit" / "SCHEMA.md").read_text() == "# Schema\n"
    assert report["stages"] == ["national", "meetings", "feedback"]
    assert "gs_meetings 0.3.1" in card
    assert "| `national-gp_reports.parquet` | 3 |" in card
    assert "| `meetings-meetings.parquet` | 4 |" in card
    assert ".DS_Store" not in card
    assert "half.parquet" not in card
    assert "2026-09-11" in card
    assert "| PPC | 2 |" in card
    assert "| current:2024-2025 | 3 | 2024-10-01 | 2025-03-30 |" in card
    assert "| form_absent | 1 |" in card
    assert "| 27 | 1 |" in card
    assert "1 source errors" in card
    assert "2 empty responses" in card
    assert "Source-reported aggregates" in card
    assert "stable no-form response" in card


def test_data_card_names_missing_stages(tmp_path):
    stage(tmp_path, "national", {"rows": 1, "rows_by_edition": {"PPC": 1}}, {"x": 1})
    schema = tmp_path / "SCHEMA.md"
    schema.write_text("# Schema\n")
    report = write_data_card(tmp_path, schema=schema, version="0")
    card = (tmp_path / "deposit" / "README_DATA.md").read_text()
    assert report["stages"] == ["national"]
    assert report["missing"] == ["meetings", "feedback"]
    assert "not included" in card
    with pytest.raises(ValueError, match="No exported"):
        write_data_card(tmp_path / "empty", schema=schema, version="0")


def test_data_card_knows_the_parts_before_the_first_upload(tmp_path, monkeypatch):
    stage(tmp_path, "meetings", {"rows": 3, "rows_by_period": {}}, {})
    table = pa.table({"n": list(range(30000)), "s": ["x" * 40] * 30000})
    path = tmp_path / "meetings/tables/meetings.parquet"
    with pq.ParquetWriter(path, table.schema) as writer:
        for start in range(0, 30000, 5000):
            writer.write_table(table.slice(start, 5000))
    monkeypatch.setattr("gs_meetings.datacard.PART_BYTES", 100_000)
    schema = tmp_path / "SCHEMA.md"
    schema.write_text("# Schema\n")
    write_data_card(tmp_path, schema=schema, version="0")
    card = (tmp_path / "deposit" / "README_DATA.md").read_text()
    assert "deposited as parts: `meetings-meetings-01.parquet`" in card
