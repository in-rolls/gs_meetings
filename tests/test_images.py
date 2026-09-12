"""Queue, download, deduplicate and export report photos without touching the portal."""

import fcntl
import gzip
import hashlib
import io
import json
import sqlite3
from contextlib import closing
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import requests
from PIL import Image

import gs_meetings.images as images_module
from gs_meetings.cli import main
from gs_meetings.collect import add_request, open_queue
from gs_meetings.images import (
    ImageClient,
    collect_images,
    export_images,
    plan_images,
    seed_images,
)

FIXTURES = Path(__file__).parent / "fixtures"


def encoded(size=(8, 6), color="white", kind="JPEG"):
    stream = io.BytesIO()
    Image.new("RGB", size, color).save(stream, kind)
    return stream.getvalue()


JPEG = encoded()
REPORTS = {
    "https://gpdp.nic.in/PPC2018/facilitatorFeedbackDetails.html?gpCode=27783": (
        "PPC2018",
        "feedback_2018",
    ),
    "https://gpdp.nic.in/facilitatorFeedbackDetails.html?lbCode=27783": (
        "current",
        "feedback_detail_current",
    ),
}


def feedback_root(tmp_path, *, extra_error=False):
    """Two stored reports with four photos, as the feedback collector saves them."""
    root = tmp_path / "feedback"
    db = open_queue(root, seed_summaries=False)
    for url, (edition, fixture) in REPORTS.items():
        add_request(db, edition, "gp", {"edition": edition}, url=url)
        path = (
            root
            / "raw"
            / edition
            / f"{hashlib.sha256(url.encode()).hexdigest()}.jsonl.gz"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wt", encoding="utf-8") as stream:
            json.dump(
                {
                    "url": url,
                    "edition": edition,
                    "ok": True,
                    "done": True,
                    "fetched_at": "2026-09-11T10:00:00+00:00",
                    "body": (FIXTURES / f"{fixture}.html").read_text(),
                },
                stream,
            )
    db.execute("UPDATE requests SET status='done',rows=1")
    if extra_error:
        add_request(
            db, "PPC", "gp", {"edition": "PPC"}, url="https://gpdp.nic.in/PPC/x"
        )
        db.execute("UPDATE requests SET status='error' WHERE url LIKE '%/PPC/x'")
    db.commit()
    db.close()
    return root


class Response:
    def __init__(self, body, content_type="image/jpeg;charset=UTF-8", status=200):
        self.content = body
        self.status_code = status
        self.headers = {"Content-Type": content_type}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Server Error")


def fake_portal(monkeypatch, bodies):
    calls = []

    def get(self, url, **kwargs):
        calls.append(url)
        return bodies(url)

    monkeypatch.setattr(requests.Session, "get", get)
    return calls


def test_seeding_resolves_every_photo_and_is_idempotent(tmp_path):
    source = feedback_root(tmp_path, extra_error=True)
    for workers in [2, 1]:
        db = seed_images(tmp_path / "images", feedback_root=source, workers=workers)
        urls = sorted(row[0] for row in db.execute("SELECT url FROM requests"))
        refs = db.execute(
            "SELECT report_url,image_ordinal,image_url,caption FROM image_refs "
            "ORDER BY report_url,image_ordinal"
        ).fetchall()
        db.close()
        assert urls == [
            "https://gpdp.nic.in/PPC2018/file/image/810091",
            "https://gpdp.nic.in/PPC2018/file/image/810092",
            "https://gpdp.nic.in/file/image/8990472",
            "https://gpdp.nic.in/file/image/8990473",
        ]
        assert [tuple(row)[1:] for row in refs] == [
            (1, "https://gpdp.nic.in/PPC2018/file/image/810091", "Gram Sabha Image"),
            (
                2,
                "https://gpdp.nic.in/PPC2018/file/image/810092",
                "Public Information Board Image",
            ),
            (1, "https://gpdp.nic.in/file/image/8990473", "Gram Sabha Image"),
            (
                2,
                "https://gpdp.nic.in/file/image/8990472",
                "Public Information Board Image",
            ),
        ]


def test_dry_run_plans_without_network(tmp_path, monkeypatch):
    calls = fake_portal(monkeypatch, lambda url: pytest.fail("dry run fetched"))
    plan = plan_images(tmp_path / "images", feedback_root(tmp_path), workers=1)
    assert calls == []
    assert plan["reports_seeded"] == 2
    assert plan["image_references"] == 4
    assert plan["unique_image_urls"] == 4
    assert plan["by_edition"] == {
        "PPC2018": {"pending": 2},
        "current": {"pending": 2},
    }
    assert plan["downloaded"]["images"] == 0
    assert plan["projected_remaining_bytes"] is None
    assert not (tmp_path / "images" / "objects").exists()


def test_download_deduplicates_rejects_non_images_and_resumes(tmp_path, monkeypatch):
    source = feedback_root(tmp_path)
    root = tmp_path / "images"

    def bodies(url):
        if url.endswith("8990472"):
            return Response(b"<html><title>500</title></html>", "text/html")
        return Response(JPEG)

    calls = fake_portal(monkeypatch, bodies)
    report = collect_images(root, source, workers=2, retries=1, seed_workers=1)
    assert len(calls) == 4
    groups = {(g["edition"], g["status"]): g["requests"] for g in report["groups"]}
    assert groups == {
        ("PPC2018", "done"): 2,
        ("current", "done"): 1,
        ("current", "error"): 1,
    }
    digest = hashlib.sha256(JPEG).hexdigest()
    objects = list((root / "objects").rglob("*.jpg"))
    assert objects == [root / "objects" / digest[:2] / f"{digest}.jpg"]
    assert objects[0].read_bytes() == JPEG
    db = open_queue(root, seed_summaries=False)
    error = db.execute(
        "SELECT error FROM requests WHERE url LIKE '%8990472'"
    ).fetchone()
    stored = db.execute("SELECT DISTINCT sha256,bytes,path FROM images").fetchall()
    db.close()
    assert "Not an image" in error[0]
    assert [tuple(row) for row in stored] == [
        (digest, len(JPEG), f"objects/{digest[:2]}/{digest}.jpg")
    ]

    calls.clear()
    collect_images(root, source, workers=2, retries=1, seed_workers=1)
    assert calls == ["https://gpdp.nic.in/file/image/8990472"]


@pytest.mark.parametrize(
    "body",
    [
        b"GIF-ish but not",
        b"GIF89a",
        encoded()[:40],
        encoded(kind="PNG")[:-20],
    ],
    ids=["not-image", "bare-gif-signature", "truncated-jpeg", "truncated-png"],
)
def test_image_client_rejects_undecodable_bytes(tmp_path, monkeypatch, body):
    fake_portal(monkeypatch, lambda url: Response(body, "image/jpeg"))
    client = ImageClient(tmp_path, "PPC")
    with pytest.raises(ValueError, match="Not an image"):
        client.get("https://gpdp.nic.in/PPC/file/image/1")
    client.close()
    assert not list(tmp_path.rglob("*"))


def test_image_client_records_dimensions_and_png(tmp_path, monkeypatch):
    png = encoded((5, 3), "black", "PNG")
    fake_portal(monkeypatch, lambda url: Response(png, "image/png"))
    client = ImageClient(tmp_path, "PPC")
    rows, path = client.get("https://gpdp.nic.in/PPC/file/image/1")
    client.close()
    assert (rows[0]["width"], rows[0]["height"]) == (5, 3)
    assert path.suffix == ".png"
    assert path.read_bytes() == png


def test_failed_object_write_leaves_no_temporary_file(tmp_path, monkeypatch):
    fake_portal(monkeypatch, lambda url: Response(JPEG))

    def fail(self, target):
        raise OSError("disk full")

    monkeypatch.setattr(Path, "replace", fail)
    client = ImageClient(tmp_path, "PPC")
    with pytest.raises(OSError, match="disk full"):
        client.get("https://gpdp.nic.in/PPC/file/image/1")
    client.close()
    assert not [path for path in tmp_path.rglob("*") if path.is_file()]


def test_same_photo_url_from_two_editions_is_queued_once(tmp_path):
    root = tmp_path / "feedback"
    db = open_queue(root, seed_summaries=False)
    body = (
        '<form id="FACILITATOR_MODEL"><div><img src="https://gpdp.nic.in/file/image/7"/>'
        "Gram Sabha Image</div></form>"
    )
    for edition in ["PPC", "PPC2019"]:
        url = f"https://gpdp.nic.in/{edition}/facilitatorFeedbackDetails.html?x=1"
        add_request(db, edition, "gp", {"edition": edition}, url=url)
        key = hashlib.sha256(url.encode()).hexdigest()
        path = root / "raw" / edition / f"{key}.jsonl.gz"
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wt", encoding="utf-8") as stream:
            json.dump(
                {
                    "url": url,
                    "edition": edition,
                    "ok": True,
                    "done": True,
                    "body": body,
                },
                stream,
            )
    db.execute("UPDATE requests SET status='done',rows=1")
    db.commit()
    db.close()
    images = seed_images(tmp_path / "images", feedback_root=root, workers=1)
    assert images.execute("SELECT count(*) FROM requests").fetchone()[0] == 1
    assert images.execute("SELECT count(*) FROM image_refs").fetchone()[0] == 2
    images.close()


def test_seeding_in_small_batches_matches(tmp_path, monkeypatch):
    monkeypatch.setattr(images_module, "SEED_BATCH", 1)
    db = seed_images(tmp_path / "images", feedback_root=feedback_root(tmp_path))
    assert db.execute("SELECT count(*) FROM image_refs").fetchone()[0] == 4
    db.close()


def test_export_keeps_failures_and_does_not_reschedule(tmp_path, monkeypatch):
    source = feedback_root(tmp_path)
    root = tmp_path / "images"

    def bodies(url):
        if url.endswith("8990472"):
            return Response(b"<html>500</html>", "text/html")
        return Response(JPEG)

    fake_portal(monkeypatch, bodies)
    collect_images(root, source, workers=2, retries=1, seed_workers=1)
    with pytest.raises(ValueError, match="incomplete"):
        export_images(root)
    report = export_images(root, allow_source_errors=True)
    assert report["requests"] == {"done": 3, "error": 1}
    assert report["all_discovered_requests_succeeded"] is False
    with closing(sqlite3.connect(root / "collection.sqlite")) as db:
        statuses = dict(db.execute("SELECT status,count(*) FROM requests GROUP BY 1"))
    assert statuses == {"done": 3, "error": 1}


def test_export_refuses_while_collection_holds_the_lock(tmp_path, monkeypatch):
    source = feedback_root(tmp_path)
    root = tmp_path / "images"
    fake_portal(monkeypatch, lambda url: Response(JPEG))
    collect_images(root, source, workers=2, retries=1, seed_workers=1)
    with (root / "collection.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(ValueError, match="running"):
            export_images(root)


def test_interrupted_export_leaves_no_manifest(tmp_path, monkeypatch):
    source = feedback_root(tmp_path)
    root = tmp_path / "images"
    fake_portal(monkeypatch, lambda url: Response(JPEG))
    collect_images(root, source, workers=2, retries=1, seed_workers=1)
    export_images(root)
    before = (root / "tables/images.parquet").read_bytes()
    original = images_module.pq.ParquetWriter
    opened = []

    def writer(path, *args, **kwargs):
        opened.append(path)
        if len(opened) == 2:
            raise OSError("interrupted")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(images_module.pq, "ParquetWriter", writer)
    with pytest.raises(OSError, match="interrupted"):
        export_images(root)
    assert not (root / "tables/manifest.json").exists()
    assert (root / "tables/images.parquet").read_bytes() == before


def test_export_refuses_incomplete_queue_then_writes_tables(tmp_path, monkeypatch):
    source = feedback_root(tmp_path)
    root = tmp_path / "images"
    seed_images(root, feedback_root=source, workers=1).close()
    with pytest.raises(ValueError, match="incomplete"):
        export_images(root)
    fake_portal(monkeypatch, lambda url: Response(JPEG))
    collect_images(root, source, workers=2, retries=1, seed_workers=1)
    report = export_images(root)
    assert report["rows"] == {"images": 4, "image_refs": 4}
    assert report["unique_files"] == 1
    images = pq.read_table(root / "tables/images.parquet").to_pylist()
    assert {row["image_id"] for row in images} == {
        "810091",
        "810092",
        "8990472",
        "8990473",
    }
    assert all(row["status"] == "done" and row["bytes"] == len(JPEG) for row in images)
    assert {(row["width"], row["height"]) for row in images} == {(8, 6)}
    refs = pq.read_table(root / "tables/image_refs.parquet").column("caption")
    assert "Public Information Board Image" in refs.to_pylist()


def test_cli_dry_run(tmp_path, monkeypatch, capsys):
    fake_portal(monkeypatch, lambda url: pytest.fail("dry run fetched"))
    source = feedback_root(tmp_path)
    status = main(
        [
            "collect-images",
            "--root",
            str(tmp_path / "images"),
            "--feedback-root",
            str(source),
            "--seed-workers",
            "1",
            "--dry-run",
        ]
    )
    assert status == 0
    assert (
        json.loads((tmp_path / "images/image-plan.json").read_text())[
            "unique_image_urls"
        ]
        == 4
    )
