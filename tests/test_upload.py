"""Deposit exported tables on Zenodo as a draft first and publish only on request."""

import hashlib
import json
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from gs_meetings.upload import deposit, load_token

BASE = "https://sandbox.zenodo.org"


class Session:
    """Record every deposition call and answer like the Zenodo REST API."""

    def __init__(self):
        self.calls = []
        self.files = {}
        self.metadata = None
        self.published = False
        self.headers = {}
        self.current_id = 41

    def response(self, status, body):
        return SimpleNamespace(
            status_code=status,
            ok=status < 300,
            json=lambda: body,
            text=json.dumps(body),
            raise_for_status=lambda: None,
        )

    def deposition(self):
        return {
            "id": self.current_id,
            "metadata": {
                **(self.metadata or {}),
                "prereserve_doi": {"doi": f"10.5072/zenodo.{self.current_id}"},
            },
            "links": {
                "bucket": f"{BASE}/api/files/bucket-{self.current_id}",
                "html": f"{BASE}/deposit/{self.current_id}",
                "latest_draft": f"{BASE}/api/deposit/depositions/{self.current_id}",
            },
            "files": [
                {
                    "filename": name,
                    "id": f"id-{name}",
                    "checksum": hashlib.md5(data).hexdigest(),  # noqa: S324
                }
                for name, data in self.files.items()
            ],
            "submitted": self.published,
        }

    def post(self, url, json=None, timeout=None):
        self.calls.append(("POST", url))
        if url.endswith("/actions/publish"):
            self.published = True
            return self.response(
                202,
                {
                    **self.deposition(),
                    "doi": f"10.5072/zenodo.{self.current_id}",
                    "conceptdoi": "10.5072/zenodo.40",
                },
            )
        if url.endswith("/actions/newversion"):
            self.current_id += 1
            self.published = False
            return self.response(201, self.deposition())
        return self.response(201, self.deposition())

    def get(self, url, timeout=None):
        self.calls.append(("GET", url))
        return self.response(200, self.deposition())

    def delete(self, url, timeout=None):
        self.calls.append(("DELETE", url))
        name = url.rsplit("/id-", 1)[1]
        self.files.pop(name)
        return self.response(204, {})

    fail_after = None
    flaky = 0
    aborts = 0

    def put(self, url, data=None, json=None, timeout=None):
        self.calls.append(("PUT", url))
        if "/api/files/" in url:
            if self.fail_after is not None and len(self.files) >= self.fail_after:
                return self.response(500, {"message": "connection reset"})
            if self.aborts:
                self.aborts -= 1
                raise requests.ConnectionError("Connection aborted")
            if self.flaky:
                self.flaky -= 1
                data.read()
                return self.response(502, {"message": "Bad Gateway"})
            self.files[url.rsplit("/", 1)[1]] = data.read()
            return self.response(201, {})
        self.metadata = json["metadata"]
        return self.response(200, self.deposition())


def tables(tmp_path):
    for stage in ["national", "meetings"]:
        folder = tmp_path / stage / "tables"
        folder.mkdir(parents=True)
        (folder / f"{stage}.parquet").write_bytes(stage.encode())
        (folder / "manifest.json").write_text("{}")
    (tmp_path / "feedback" / "raw").mkdir(parents=True)
    (tmp_path / "deposit").mkdir()
    (tmp_path / "deposit" / "SCHEMA.md").write_text("# Tables\n")
    return tmp_path


def test_deposit_creates_a_draft_and_uploads_every_table_without_publishing(tmp_path):
    session = Session()
    root = tables(tmp_path)
    report = deposit(root, session=session, base_url=BASE, version="0.3.0")
    assert not session.published
    assert ("POST", f"{BASE}/api/deposit/depositions") in session.calls
    assert sorted(session.files) == [
        "SCHEMA.md",
        "meetings-manifest.json",
        "meetings-meetings.parquet",
        "national-manifest.json",
        "national-national.parquet",
    ]
    assert session.metadata["upload_type"] == "dataset"
    assert session.metadata["version"] == "0.3.0"
    assert session.metadata["license"] == "cc0-1.0"
    assert report["deposition_id"] == 41
    assert report["doi"] == "10.5072/zenodo.41"
    assert report["uploaded"] == 5
    assert report["published"] is False
    saved = json.loads((root / "zenodo.json").read_text())
    assert saved["deposition_id"] == 41
    assert saved["html"] == f"{BASE}/deposit/41"


def test_deposit_reuses_the_draft_and_skips_unchanged_files(tmp_path):
    session = Session()
    root = tables(tmp_path)
    deposit(root, session=session, base_url=BASE, version="0.3.0")
    (root / "national/tables/national.parquet").write_bytes(b"changed")
    (root / "feedback/tables").mkdir()
    (root / "feedback/tables/feedback.parquet").write_bytes(b"new")
    session.calls.clear()
    report = deposit(root, session=session, base_url=BASE, version="0.3.1")
    assert ("POST", f"{BASE}/api/deposit/depositions") not in session.calls
    assert ("GET", f"{BASE}/api/deposit/depositions/41") in session.calls
    uploads = [url.rsplit("/", 1)[1] for kind, url in session.calls if kind == "PUT"]
    assert uploads == ["41", "feedback-feedback.parquet", "national-national.parquet"]
    assert report["uploaded"] == 2
    assert report["skipped"] == 4


def test_publish_only_when_requested(tmp_path):
    session = Session()
    root = tables(tmp_path)
    report = deposit(root, session=session, base_url=BASE, version="1.0", publish=True)
    assert session.published
    assert session.calls[-1] == (
        "POST",
        f"{BASE}/api/deposit/depositions/41/actions/publish",
    )
    assert report["published"] is True
    with pytest.raises(ValueError, match="already published"):
        deposit(root, session=session, base_url=BASE, version="1.0")


def test_deposit_refuses_a_root_without_tables(tmp_path):
    with pytest.raises(ValueError, match="No exported tables"):
        deposit(tmp_path, session=Session(), base_url=BASE, version="0.3.0")


def test_token_comes_from_the_environment_or_the_config_file(tmp_path, monkeypatch):
    monkeypatch.delenv("ZENODO_TOKEN", raising=False)
    monkeypatch.delenv("ZENODO_SANDBOX_TOKEN", raising=False)
    config = tmp_path / "zenodo.ini"
    with pytest.raises(ValueError, match="ZENODO_TOKEN"):
        load_token(sandbox=False, config=config)
    config.write_text("[zenodo]\napi_token = live\nsandbox_api_token = box\n")
    assert load_token(sandbox=False, config=config) == "live"
    assert load_token(sandbox=True, config=config) == "box"
    monkeypatch.setenv("ZENODO_SANDBOX_TOKEN", "env")
    assert load_token(sandbox=True, config=config) == "env"


def test_failed_upload_keeps_the_draft_so_a_retry_reuses_it(tmp_path):
    session = Session()
    session.fail_after = 1
    root = tables(tmp_path)
    with pytest.raises(ValueError, match="connection reset"):
        deposit(root, session=session, base_url=BASE, version="0.3.0", backoff=0)
    saved = json.loads((root / "zenodo.json").read_text())
    assert saved["deposition_id"] == 41
    assert saved["published"] is False
    session.fail_after = None
    session.calls.clear()
    report = deposit(root, session=session, base_url=BASE, version="0.3.0")
    assert ("POST", f"{BASE}/api/deposit/depositions") not in session.calls
    assert report["uploaded"] == 4
    assert report["skipped"] == 1


def test_sandbox_state_never_overwrites_the_live_deposition(tmp_path):
    root = tables(tmp_path)
    deposit(root, session=Session(), base_url="https://zenodo.org", version="1")
    deposit(root, session=Session(), base_url=BASE, sandbox=True, version="1")
    live = json.loads((root / "zenodo.json").read_text())
    box = json.loads((root / "zenodo-sandbox.json").read_text())
    assert live["base_url"] == "https://zenodo.org"
    assert box["base_url"] == BASE


def test_large_parquet_files_are_split_into_parts_under_the_limit(tmp_path):
    root = tables(tmp_path)
    table = pa.table({"n": list(range(30000)), "s": ["x" * 40] * 30000})
    path = root / "meetings/tables/meetings.parquet"
    with pq.ParquetWriter(path, table.schema) as writer:
        for start in range(0, 30000, 5000):
            writer.write_table(table.slice(start, 5000))
    session = Session()
    report = deposit(
        root, session=session, base_url=BASE, version="1", part_bytes=100_000
    )
    parts = sorted(name for name in session.files if "meetings-meetings" in name)
    assert len(parts) > 1
    assert parts == [
        f"meetings-meetings-{i:02d}.parquet" for i in range(1, len(parts) + 1)
    ]
    assert "meetings-meetings.parquet" not in session.files
    rebuilt = pa.concat_tables(
        [pq.read_table(pa.BufferReader(session.files[name])) for name in parts]
    )
    assert rebuilt.equals(table)
    assert all(len(session.files[name]) <= 100_000 * 1.5 for name in parts)
    assert report["parts"] == {"meetings-meetings.parquet": parts}


def test_a_failed_put_is_retried_from_the_start_of_the_file(tmp_path):
    root = tables(tmp_path)
    session = Session()
    session.flaky = 2
    report = deposit(root, session=session, base_url=BASE, version="1", backoff=0)
    assert report["uploaded"] == 5
    assert session.files["SCHEMA.md"] == b"# Tables\n"
    assert sum(1 for kind, url in session.calls if "/api/files/" in url) == 7


def test_a_dropped_connection_is_retried_like_a_gateway_error(tmp_path):
    root = tables(tmp_path)
    session = Session()
    session.aborts = 3
    report = deposit(root, session=session, base_url=BASE, version="1", backoff=0)
    assert report["uploaded"] == 5
    session = Session()
    session.aborts = 4
    with pytest.raises(ValueError, match="Connection aborted"):
        deposit(root, session=session, base_url=BASE, version="1", backoff=0)


def test_dotfiles_empty_files_and_partial_writes_are_not_deposited(tmp_path):
    root = tables(tmp_path)
    (root / "deposit" / ".Rhistory").write_bytes(b"")
    (root / "deposit" / "notes.txt").write_bytes(b"")
    (root / "national" / "tables" / "gp.parquet.part").write_bytes(b"half")
    session = Session()
    deposit(root, session=session, base_url=BASE, version="1")
    assert ".Rhistory" not in session.files
    assert "notes.txt" not in session.files
    assert not any(name.endswith(".part") for name in session.files)
    assert "SCHEMA.md" in session.files


def test_a_published_record_gets_a_new_version_only_when_asked(tmp_path):
    root = tables(tmp_path)
    session = Session()
    deposit(root, session=session, base_url=BASE, version="1", publish=True)
    (root / "feedback/tables").mkdir()
    (root / "feedback/tables/feedback.parquet").write_bytes(b"new")
    with pytest.raises(ValueError, match="new-version"):
        deposit(root, session=session, base_url=BASE, version="2")
    session.calls.clear()
    report = deposit(
        root, session=session, base_url=BASE, version="2", new_version=True
    )
    assert ("POST", f"{BASE}/api/deposit/depositions/41/actions/newversion") in (
        session.calls
    )
    assert report["deposition_id"] == 42
    assert report["published"] is False
    assert report["uploaded"] == 1
    assert report["skipped"] == 5
    assert session.metadata["version"] == "2"
    saved = json.loads((root / "zenodo.json").read_text())
    assert saved["deposition_id"] == 42
    report = deposit(root, session=session, base_url=BASE, version="2", publish=True)
    assert report["doi"] == "10.5072/zenodo.42"


def big_table(path, rows=30000, group=5000):
    table = pa.table({"n": list(range(rows)), "s": ["x" * 40] * rows})
    with pq.ParquetWriter(path, table.schema) as writer:
        for start in range(0, rows, group):
            writer.write_table(table.slice(start, group))
    return table


def test_switching_between_whole_and_parts_removes_superseded_files(tmp_path):
    root = tables(tmp_path)
    big_table(root / "meetings/tables/meetings.parquet")
    session = Session()
    deposit(root, session=session, base_url=BASE, version="1", part_bytes=10**9)
    assert "meetings-meetings.parquet" in session.files
    report = deposit(
        root, session=session, base_url=BASE, version="1", part_bytes=60_000
    )
    assert "meetings-meetings.parquet" not in session.files
    assert report["removed"] == ["meetings-meetings.parquet"]
    many = report["parts"]["meetings-meetings.parquet"]
    assert len(many) >= 3
    report = deposit(
        root, session=session, base_url=BASE, version="1", part_bytes=100_000
    )
    fewer = report["parts"]["meetings-meetings.parquet"]
    assert 1 < len(fewer) < len(many)
    assert report["removed"] == sorted(set(many) - set(fewer))
    assert all(name in session.files for name in fewer)
    report = deposit(
        root, session=session, base_url=BASE, version="1", part_bytes=10**9
    )
    assert report["parts"] == {}
    assert report["removed"] == fewer
    assert "meetings-meetings.parquet" in session.files
    assert not any(name.startswith("meetings-meetings-") for name in session.files)


def test_more_than_ninety_nine_parts_is_refused(tmp_path):
    root = tables(tmp_path)
    big_table(root / "meetings/tables/meetings.parquet", rows=120000, group=1000)
    with pytest.raises(ValueError, match="raise the part size"):
        deposit(root, session=Session(), base_url=BASE, version="1", part_bytes=1)


def test_terminal_upload_failures_name_the_file(tmp_path):
    root = tables(tmp_path)
    session = Session()
    session.aborts = 4
    with pytest.raises(ValueError, match="failed after 4 attempts"):
        deposit(root, session=session, base_url=BASE, version="1", backoff=0)
    session = Session()
    session.fail_after = 0
    with pytest.raises(ValueError, match=r"Upload of .* failed: Zenodo returned 500"):
        deposit(root, session=session, base_url=BASE, version="1", backoff=0)


def test_a_sibling_table_named_like_a_part_is_never_removed(tmp_path):
    root = tables(tmp_path)
    big_table(root / "meetings/tables/meetings.parquet")
    (root / "meetings/tables/meetings-2024.parquet").write_bytes(b"sibling")
    session = Session()
    deposit(root, session=session, base_url=BASE, version="1", part_bytes=10**9)
    report = deposit(
        root, session=session, base_url=BASE, version="1", part_bytes=100_000
    )
    assert report["removed"] == ["meetings-meetings.parquet"]
    assert session.files["meetings-meetings-2024.parquet"] == b"sibling"


def test_concept_doi_comes_from_the_publish_response_and_is_never_lost(tmp_path):
    root = tables(tmp_path)
    session = Session()
    report = deposit(root, session=session, base_url=BASE, version="1")
    assert report["concept_doi"] is None
    report = deposit(root, session=session, base_url=BASE, version="1", publish=True)
    assert report["concept_doi"] == "10.5072/zenodo.40"
    report = deposit(
        root, session=session, base_url=BASE, version="2", new_version=True
    )
    assert report["concept_doi"] == "10.5072/zenodo.40"


def test_uploader_and_card_split_on_the_same_plan(tmp_path):
    root = tables(tmp_path)
    path = root / "meetings/tables/meetings.parquet"
    table = pa.table({"n": list(range(2000))})
    pq.write_table(table, path)
    size = path.stat().st_size
    source = pq.ParquetFile(path)
    chunks = sum(
        source.metadata.row_group(g).column(c).total_compressed_size
        for g in range(source.num_row_groups)
        for c in range(source.metadata.row_group(g).num_columns)
    )
    assert chunks < size
    session = Session()
    report = deposit(
        root, session=session, base_url=BASE, version="1", part_bytes=chunks
    )
    assert report["parts"] == {}
    assert "meetings-meetings.parquet" in session.files
