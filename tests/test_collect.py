"""Test national enumeration, failure recovery and scope preservation."""

import json
import sqlite3
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar

import pytest
import requests

from gs_meetings import collect as module

FIXTURES = Path(__file__).parent / "fixtures"


class Client:
    calls: ClassVar[list] = []
    fail = False

    def __init__(self, root, edition, retries):
        self.root = root

    def get(self, url):
        self.calls.append(url)
        if "/stateSummary" in url:
            rows = [{"code": 6, "name": "HARYANA"}, {"code": 4, "name": "CHANDIGARH"}]
        elif "/districtSummary" in url:
            rows = [{"code": 58, "name": "AMBALA", "level": "I"}]
        elif "/blockSummary" in url:
            if self.fail:
                raise requests.Timeout("Temporary failure")
            rows = [{"code": 1030, "name": "AMBALA-I", "level": "V"}]
        else:
            rows = json.loads((FIXTURES / "gp.json").read_text())
        return rows, self.root / "unused"

    def close(self):
        pass


def test_resume_enumerates_all_children_and_keeps_excluded_state_source(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(module, "Client", Client)
    monkeypatch.setattr(module, "EDITIONS", {"current": ""})
    Client.calls = []
    result = module.collect(tmp_path, max_requests=2)
    assert len(Client.calls) == 2
    assert any(row["status"] == "pending" for row in result["groups"])
    Client.fail = True
    result = module.collect(tmp_path)
    assert len(result["errors"]) == 1
    Client.fail = False
    result = module.collect(tmp_path)
    assert result["errors"] == []
    assert all(row["status"] == "done" for row in result["groups"])
    assert next(row["rows"] for row in result["groups"] if row["level"] == "gp") == 103
    assert sum("/stateSummary" in url for url in Client.calls) == 1
    assert not any("stateId=4" in url for url in Client.calls)
    assert result["display_excluded_states"] == [4, 7]


@pytest.mark.parametrize("state", [13, 15, 17, 34])
def test_direct_state_routes(tmp_path, monkeypatch, state):
    monkeypatch.setattr(module, "EDITIONS", {"current": ""})
    db = module.open_queue(tmp_path)
    task = dict(db.execute("SELECT * FROM requests").fetchone())
    module.children(db, task, [{"code": state, "name": "State"}])
    rows = [
        dict(row) for row in db.execute("SELECT * FROM requests WHERE level!='state'")
    ]
    assert len(rows) == 1
    assert rows[0]["level"] == ("block" if state == 34 else "gp")
    db.close()


def test_conflicting_route_fails_instead_of_losing_a_parent(tmp_path, monkeypatch):
    monkeypatch.setattr(module, "EDITIONS", {"current": ""})
    db = module.open_queue(tmp_path)
    module.add_request(db, "current", "gp", {"district_code": "1"}, stateId=6, code=10)
    with pytest.raises(ValueError, match="Conflicting geography"):
        module.add_request(
            db, "current", "gp", {"district_code": "2"}, stateId=6, code=10
        )
    db.close()


def test_low_storage_stops_before_fetching_and_keeps_pending_work(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(module, "Client", Client)
    monkeypatch.setattr(
        module.shutil, "disk_usage", lambda _path: SimpleNamespace(free=0)
    )
    Client.calls = []
    report = module.collect(tmp_path)
    assert report["storage_limited"] is True
    assert all(group["status"] == "pending" for group in report["groups"])
    assert Client.calls == []


class Clock:
    """Stand in for `time` so that a sleep passes instantly and is recorded."""

    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def queue_of(urls, archived=""):
    def initialize(root):
        db = module.open_queue(root, seed_summaries=False)
        for url in urls:
            edition = "PPC" if url in archived else "current"
            module.add_request(db, edition, "gp", {"url": url}, url=url)
        db.commit()
        return db

    return initialize


def unavailable():
    return requests.HTTPError("503", response=SimpleNamespace(status_code=503))


def scripted_client(fails):
    """Build a client whose `fails(url, call_number)` decides each 503."""

    class Scripted:
        calls: ClassVar[list] = []

        def __init__(self, root, edition, retries):
            pass

        def get(self, url):
            self.calls.append(url)
            if fails(url, len(self.calls)):
                raise unavailable()
            return [{}], None

        def close(self):
            pass

    return Scripted


def statuses(root):
    with closing(sqlite3.connect(root / "collection.sqlite")) as db:
        return dict(db.execute("SELECT url,status FROM requests"))


def test_confirmed_misses_are_not_refetched_on_resume(tmp_path):
    db = queue_of("abcd")(tmp_path)
    for url, status, error in [
        ("a", "error", "absent"),
        ("b", "pending", "absent"),
        ("c", "error", "503"),
        ("d", "running", None),
    ]:
        db.execute(
            "UPDATE requests SET status=?,error=? WHERE url=?", (status, error, url)
        )
    db.commit()
    db.close()
    module.open_queue(
        tmp_path, seed_summaries=False, terminal_errors=("absent",)
    ).close()
    assert statuses(tmp_path) == {
        "a": "error",
        "b": "error",
        "c": "pending",
        "d": "pending",
    }


def test_outage_is_waited_out_without_failing_the_queue(tmp_path, monkeypatch):
    clock = Clock()
    monkeypatch.setattr(module, "time", clock)
    client = scripted_client(lambda _url, call: call <= 5)
    result = module.run_queue(
        tmp_path,
        1,
        2,
        None,
        initialize=queue_of("abcdef"),
        expand=lambda *_: None,
        client_factory=client,
        outage_after=3,
    )
    assert result["errors"] == []
    assert set(statuses(tmp_path).values()) == {"done"}
    assert clock.sleeps == [60, 120, 240]
    assert client.calls[3:6] == ["d", "e", "f"]


def test_a_failing_url_among_successes_is_not_an_outage(tmp_path, monkeypatch):
    clock = Clock()
    monkeypatch.setattr(module, "time", clock)
    module.run_queue(
        tmp_path,
        1,
        2,
        None,
        initialize=queue_of("abcdef"),
        expand=lambda *_: None,
        client_factory=scripted_client(lambda url, _call: url in "ace"),
        outage_after=3,
    )
    assert clock.sleeps == []
    found = statuses(tmp_path)
    assert [url for url in found if found[url] == "error"] == ["a", "c", "e"]


def test_an_outage_longer_than_the_limit_ends_with_recorded_errors(
    tmp_path, monkeypatch
):
    clock = Clock()
    monkeypatch.setattr(module, "time", clock)
    result = module.run_queue(
        tmp_path,
        1,
        2,
        None,
        initialize=queue_of("abcdef"),
        expand=lambda *_: None,
        client_factory=scripted_client(lambda *_: True),
        outage_after=3,
        outage_limit=100,
    )
    assert clock.sleeps == [60, 120]
    assert len(result["errors"]) == 6


def test_a_zero_outage_limit_records_failures_without_waiting(tmp_path, monkeypatch):
    clock = Clock()
    monkeypatch.setattr(module, "time", clock)
    result = module.run_queue(
        tmp_path,
        1,
        2,
        None,
        initialize=queue_of("abcdef"),
        expand=lambda *_: None,
        client_factory=scripted_client(lambda *_: True),
        outage_after=3,
        outage_limit=0,
    )
    assert clock.sleeps == []
    assert len(result["errors"]) == 6


def test_an_archive_outage_does_not_hold_up_the_live_report(tmp_path, monkeypatch):
    clock = Clock()
    monkeypatch.setattr(module, "time", clock)
    client = scripted_client(lambda url, _call: url in "abcd")
    module.run_queue(
        tmp_path,
        1,
        2,
        None,
        initialize=queue_of("abcdwxyz", archived="abcd"),
        expand=lambda *_: None,
        client_factory=client,
        outage_after=3,
        outage_limit=100,
    )
    assert client.calls[:7] == ["a", "b", "c", "w", "x", "y", "z"]
    assert clock.sleeps == [60, 120]
    found = statuses(tmp_path)
    assert {found[url] for url in "wxyz"} == {"done"}
    assert {found[url] for url in "abcd"} == {"error"}
