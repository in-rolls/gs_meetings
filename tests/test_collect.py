"""Test national enumeration, failure recovery and scope preservation."""

import json
from pathlib import Path
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
