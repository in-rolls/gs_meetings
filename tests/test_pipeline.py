"""Exercise real report shapes, hierarchy, recovery and offline deliverables."""

import gzip
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import requests

from gs_meetings.cli import main
from gs_meetings.fetch import Client, read_capture
from gs_meetings.frame import FRAME_SCHEMA, enumerate_units, fetch_units, write_parquet
from gs_meetings.parse import SCHEMA, attendance_issues, convert, parse_rows
from gs_meetings.source import METRICS, endpoint, validate_rows

FIXTURES = Path(__file__).parent / "fixtures"


def fixture(name):
    return json.loads((FIXTURES / f"{name}.json").read_text())


@pytest.mark.parametrize(
    "name",
    ["current", "PPC", "PPC2020", "PPC2019", "PPC2018", "district", "block", "gp"],
)
def test_real_report_contract(name):
    assert validate_rows(fixture(name))


@pytest.mark.parametrize(
    "bad",
    [
        None,
        {},
        "<html>Error</html>",
        [None],
        [{"name": "X", "code": True}],
        [{"name": "X", "code": 1}],
    ],
)
def test_bad_payload_rejected(bad):
    with pytest.raises(ValueError, match=r"Expected|Report row|Invalid|Missing"):
        validate_rows(bad)


def test_duplicate_and_invalid_metrics_rejected():
    row = fixture("gp")[0]
    with pytest.raises(ValueError, match="duplicate"):
        validate_rows([row, row])
    for value in [-1, 2.3, True, "4"]:
        with pytest.raises(ValueError, match="Invalid count"):
            validate_rows([{**row, "peoplePresent": value}])


def test_null_and_zero_are_distinct():
    row = {**fixture("gp")[0], "scPresent": None, "stPresent": 0}
    parsed = parse_rows(json.dumps([row]), {"url": "source"}, "2026-09-11T00:00:00Z")[0]
    assert parsed["sc_present"] is None
    assert parsed["st_present"] == 0
    assert json.loads(parsed["raw_row"]) == row
    assert parsed["feedback_submitted"] == 4


class FakeClient:
    def __init__(self, root, states=None, districts=None):
        self.root = root
        self.edition = "current"
        self.states = states or fixture("current")
        self.districts = districts if districts is not None else fixture("district")
        self.urls = []

    def get(self, url):
        self.urls.append(url)
        if "/stateSummary" in url:
            rows = self.states
        elif "/districtSummary" in url:
            rows = self.districts
        elif "/blockSummary" in url:
            rows = fixture("block")
        else:
            rows = fixture("gp")
        return rows, self.root / "fake"


def test_haryana_enumeration_and_filter(tmp_path):
    client = FakeClient(tmp_path)
    units = enumerate_units(client, 6, 58)
    assert len(units) == 6
    assert units[0]["block_code"] == "1030"
    assert units[0]["district_code"] == "58"
    assert pq.read_schema(tmp_path / "frame.parquet") == FRAME_SCHEMA
    assert len(client.urls) == 3
    with pytest.raises(ValueError, match="No matching"):
        enumerate_units(client, 6, 9999)


@pytest.mark.parametrize("code", [13, 15, 17])
def test_direct_state_route(tmp_path, code):
    units = enumerate_units(FakeClient(tmp_path), code)
    assert len(units) == 1
    assert units[0]["parent_level"] == "state"
    assert units[0]["url"].endswith(f"stateId={code}&code={code}")


def test_puducherry_route_and_district_to_gp(tmp_path):
    client = FakeClient(tmp_path)
    units = enumerate_units(client, 34)
    assert "stateId=34&zpCode=34" in client.urls[1]
    assert "district_code" not in units[0]
    district = {**fixture("district")[0], "level": "V"}
    units = enumerate_units(FakeClient(tmp_path, districts=[district]), 6)
    assert len(units) == 1
    assert units[0]["parent_level"] == "district"
    assert "block_code" not in units[0]


class Response:
    def __init__(self, payload, status=200):
        self.text = json.dumps(payload)
        self.status_code = status
        self.url = "https://gpdp.nic.in/test"

    def json(self):
        return json.loads(self.text)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))


def test_capture_resume_failure_and_truncation(tmp_path, monkeypatch):
    client = Client(tmp_path, "current")
    calls = []

    def get(url, **kwargs):
        calls.append(url)
        return Response(fixture("gp"))

    monkeypatch.setattr(client.session, "get", get)
    url = endpoint("current", "gp", stateId=6, code=1030)
    rows, path = client.get(url)
    assert len(rows) == 103
    assert read_capture(path)["body"] == json.dumps(fixture("gp"))
    client.get(url)
    assert len(calls) == 1
    path.write_bytes(path.read_bytes()[:50])
    assert read_capture(path) is None
    client.get(url)
    assert len(calls) == 2
    monkeypatch.setattr(
        client.session, "get", lambda *a, **kw: Response({"error": "expired"})
    )
    failed_url = endpoint("current", "gp", stateId=6, code=1031)
    with pytest.raises(ValueError, match=r"Expected|Report row|Invalid|Missing"):
        client.get(failed_url)
    captures = [
        json.loads(gzip.decompress(p.read_bytes()))
        for p in (tmp_path / "raw/current").glob("*.gz")
    ]
    assert any(not record["ok"] and "expired" in record["body"] for record in captures)
    monkeypatch.setattr(client.session, "get", get)
    client.get(failed_url)
    assert len(calls) == 3
    assert not list(tmp_path.rglob("*.part"))
    client.close()


def test_retry_policy(tmp_path):
    client = Client(tmp_path, "current")
    retry = client.session.get_adapter("https://gpdp.nic.in").max_retries
    assert retry.is_retry("GET", 429)
    assert retry.is_retry("GET", 503)
    assert not retry.is_retry("GET", 403)
    assert retry.respect_retry_after_header
    assert client.session.verify is True
    client.close()


def test_offline_conversion_and_reconciliation(tmp_path, monkeypatch):
    units = enumerate_units(FakeClient(tmp_path), 6, 58)
    client = Client(tmp_path, "current")
    monkeypatch.setattr(client.session, "get", lambda *a, **kw: Response(fixture("gp")))
    report = fetch_units(client, tmp_path / "frame.parquet", limit=1)
    assert report["succeeded"] == 1
    client.close()
    monkeypatch.setattr(
        requests.Session,
        "get",
        lambda *a, **kw: pytest.fail("Offline parser accessed network"),
    )
    report = convert(tmp_path)
    assert report["rows"] == 103
    assert len(report["missing_units"]) == 5
    table = pq.read_table(tmp_path / "records.parquet")
    assert table.schema == SCHEMA
    assert table["gp_code"][0].as_py() == "27783"
    expected = json.loads(units[0]["expected"])
    actual = sum(row["peoplePresent"] for row in fixture("gp"))
    assert (
        report["reconciliation"][0]["child_sum_minus_parent"]["people_present"]
        == actual - expected["peoplePresent"]
    )
    assert (tmp_path / "CHECKSUMS").exists()
    assert main(["parse", "--root", str(tmp_path)]) == 1


def test_fetch_continues_after_failure(tmp_path):
    units = enumerate_units(FakeClient(tmp_path), 6, 58)
    client = FakeClient(tmp_path)

    def get(url):
        if url == units[0]["url"]:
            raise requests.Timeout("timeout")
        return [], tmp_path

    client.get = get
    report = fetch_units(client, tmp_path / "frame.parquet")
    assert report["succeeded"] == 5
    assert len(report["failures"]) == 1


def test_duplicate_frame_fails_conversion(tmp_path, monkeypatch):
    units = enumerate_units(FakeClient(tmp_path), 6, 58)
    write_parquet(tmp_path / "frame.parquet", [units[0], units[0]], FRAME_SCHEMA)
    client = Client(tmp_path, "current")
    monkeypatch.setattr(client.session, "get", lambda *a, **kw: Response(fixture("gp")))
    fetch_units(client, tmp_path / "frame.parquet")
    with pytest.raises(ValueError, match="Duplicate GP"):
        convert(tmp_path)
    client.close()


def test_cli_validation_and_empty_parse(tmp_path):
    with pytest.raises(SystemExit) as error:
        main(["fetch", "--limit", "0"])
    assert error.value.code == 2
    assert main(["parse", "--root", str(tmp_path)]) == 1


def test_metrics_cover_fixture():
    assert set(fixture("gp")[0]) - set(METRICS) == {"code", "name", "level", "tlb"}


@pytest.mark.parametrize("sample", fixture("historical_anomalies"))
def test_real_attendance_inconsistency_is_flagged_without_rewriting(sample):
    rows = parse_rows(
        json.dumps([sample["row"]]),
        {"url": sample["source_url"], "edition": sample["edition"]},
        sample["fetched_at"],
    )
    issues = attendance_issues(rows)
    assert len(issues) == 1
    assert issues[0]["gp_code"] == str(sample["row"]["code"])
    assert all(
        value > sample["row"]["peoplePresent"]
        for value in issues[0]["subgroups_exceeding_total"].values()
    )
    assert json.loads(rows[0]["raw_row"]) == sample["row"]


def test_missing_attendance_is_not_zero():
    row = {**fixture("gp")[0], "peoplePresent": None}
    parsed = parse_rows(json.dumps([row]), {"url": "source"}, "capture")
    assert attendance_issues(parsed) == []
    assert attendance_issues(
        parse_rows(
            json.dumps([{**row, "peoplePresent": 0}]), {"url": "source"}, "capture"
        )
    )
