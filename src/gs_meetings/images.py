"""Queue and download the photos attached to stored facilitator reports."""

import fcntl
import hashlib
import io
import json
import os
import sqlite3
import tempfile
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from contextlib import closing
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from urllib.parse import urljoin

import pyarrow as pa
import pyarrow.parquet as pq
from PIL import Image, ImageSequence, UnidentifiedImageError

from gs_meetings.collect import add_request, open_queue, run_queue
from gs_meetings.feedback import parse_feedback
from gs_meetings.fetch import Client, atomic_json, read_capture

# Decoding, not the Content-Type header or a signature, decides what was served:
# the portal answers missing photos with HTML, and a bare signature is not a photo.
IMAGE_FORMATS = {"JPEG": ".jpg", "PNG": ".png", "GIF": ".gif"}
SEED_BATCH = 10000
SCHEMAS = {
    "images": pa.schema(
        [
            ("image_url", pa.string()),
            ("edition", pa.string()),
            ("image_id", pa.string()),
            ("status", pa.string()),
            ("error", pa.string()),
            ("sha256", pa.string()),
            ("bytes", pa.int64()),
            ("content_type", pa.string()),
            ("width", pa.int64()),
            ("height", pa.int64()),
            ("path", pa.string()),
            ("fetched_at", pa.timestamp("us", tz="UTC")),
        ]
    ),
    "image_refs": pa.schema(
        [
            ("report_url", pa.string()),
            ("image_ordinal", pa.int64()),
            ("image_url", pa.string()),
            ("src", pa.string()),
            ("caption", pa.string()),
        ]
    ),
}


def report_images(path: str) -> list[dict] | None:
    """Return a stored report's photo references, or None for a damaged capture."""
    capture = read_capture(Path(path), parser=parse_feedback)
    if capture is None:
        return None
    return json.loads(capture["parsed_rows"][0]["images"])


def open_image_queue(root: Path) -> sqlite3.Connection:
    """Add photo tables to the shared request queue."""
    db = open_queue(root, seed_summaries=False)
    db.execute("CREATE TABLE IF NOT EXISTS seeded_reports (url TEXT PRIMARY KEY)")
    db.execute(
        "CREATE TABLE IF NOT EXISTS image_refs (report_url TEXT, "
        "image_ordinal INTEGER, image_url TEXT, src TEXT, caption TEXT, "
        "PRIMARY KEY(report_url,image_ordinal))"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS images (url TEXT PRIMARY KEY, sha256 TEXT, "
        "bytes INTEGER, content_type TEXT, width INTEGER, height INTEGER, "
        "path TEXT, fetched_at TEXT)"
    )
    db.commit()
    return db


def seed_images(root: Path, *, feedback_root: Path, workers: int = 4):
    """Queue photos from completed reports; already seeded reports are not reread."""
    if not (feedback_root / "collection.sqlite").is_file():
        raise ValueError("No feedback queue found")
    db = open_image_queue(root)
    metadata = root / "source.json"
    source = {"feedback_root": str(feedback_root.resolve())}
    if metadata.exists() and json.loads(metadata.read_text()) != source:
        db.close()
        raise ValueError("Image queue belongs to a different feedback collection")
    atomic_json(metadata, source)
    try:
        seeded = {row[0] for row in db.execute("SELECT url FROM seeded_reports")}
        with closing(sqlite3.connect(feedback_root / "collection.sqlite")) as source_db:
            tasks = [
                (url, edition)
                for url, edition in source_db.execute(
                    "SELECT url,edition FROM requests WHERE status='done' ORDER BY url"
                )
                if url not in seeded
            ]
        with ProcessPoolExecutor(workers) as pool:
            # Bounded batches keep pending parse results from filling memory.
            for start in range(0, len(tasks), SEED_BATCH):
                batch = tasks[start : start + SEED_BATCH]
                paths = [
                    str(
                        feedback_root
                        / "raw"
                        / edition
                        / f"{hashlib.sha256(url.encode()).hexdigest()}.jsonl.gz"
                    )
                    for url, edition in batch
                ]
                parsed = (
                    pool.map(report_images, paths, chunksize=64)
                    if workers > 1
                    else map(report_images, paths)
                )
                for (url, edition), images in zip(batch, parsed, strict=True):
                    if images is None:
                        raise ValueError(
                            f"Missing or damaged feedback capture while seeding: {url}"
                        )
                    for ordinal, image in enumerate(images, 1):
                        image_url = urljoin(url, image["src"])
                        # The URL alone identifies a photo, whichever report links it.
                        add_request(db, edition, "image", {}, url=image_url)
                        db.execute(
                            "INSERT OR REPLACE INTO image_refs VALUES (?,?,?,?,?)",
                            (url, ordinal, image_url, image["src"], image["caption"]),
                        )
                    db.execute("INSERT INTO seeded_reports VALUES (?)", (url,))
                db.commit()
        db.commit()
        return db
    except Exception:
        db.close()
        raise


def decode_image(body: bytes, content_type: str) -> tuple[str, int, int]:
    """Return the file suffix and size of a complete, decodable photo."""
    try:
        with Image.open(io.BytesIO(body)) as image:
            image.verify()
        with Image.open(io.BytesIO(body)) as image:
            kind, (width, height) = image.format, image.size
            # load() decodes only the first frame of an animated GIF.
            for frame in ImageSequence.Iterator(image):
                frame.load()
    except (UnidentifiedImageError, OSError, SyntaxError, ValueError) as exc:
        raise ValueError(
            f"Not an image: {content_type or 'no content type'}, "
            f"{len(body)} bytes ({exc})"
        ) from exc
    if kind not in IMAGE_FORMATS:
        raise ValueError(f"Not an image: unsupported format {kind}")
    return IMAGE_FORMATS[kind], width, height


class ImageClient(Client):
    """Store each distinct photo once, named by the SHA-256 of its bytes."""

    def get(self, url: str) -> tuple[list[dict], Path]:
        """Download one photo and reject anything whose bytes are not an image."""
        response = self.session.get(url, timeout=(15, 60))
        response.raise_for_status()
        body = response.content
        content_type = response.headers.get("Content-Type", "").split(";")[0].strip()
        suffix, width, height = decode_image(body, content_type)
        digest = hashlib.sha256(body).hexdigest()
        path = self.root / "objects" / digest[:2] / f"{digest}{suffix}"
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            # Threads can fetch identical bytes from different URLs at once.
            handle, name = tempfile.mkstemp(dir=path.parent, suffix=".part")
            part_path = Path(name)
            try:
                # Closing flushes buffered bytes, so it can fail as well.
                with os.fdopen(handle, "wb") as part:
                    part.write(body)
                part_path.replace(path)
            except BaseException:
                part_path.unlink(missing_ok=True)
                raise
        row = {
            "sha256": digest,
            "bytes": len(body),
            "content_type": content_type,
            "width": width,
            "height": height,
            "path": str(path.relative_to(self.root)),
            "fetched_at": datetime.now(UTC).isoformat(),
        }
        return [row], path


def store_image(db: sqlite3.Connection, task: dict, rows: list[dict]) -> None:
    """Record where a downloaded photo's bytes live."""
    row = rows[0]
    db.execute(
        "INSERT OR REPLACE INTO images VALUES (?,?,?,?,?,?,?,?)",
        (
            task["url"],
            row["sha256"],
            row["bytes"],
            row["content_type"],
            row["width"],
            row["height"],
            row["path"],
            row["fetched_at"],
        ),
    )


def collect_images(
    root: Path,
    feedback_root: Path,
    workers: int = 4,
    retries: int = 2,
    max_requests: int | None = None,
    seed_workers: int = 4,
) -> dict:
    """Seed newly completed reports, then download pending photos."""
    root.mkdir(parents=True, exist_ok=True)
    with (root / "collection.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return run_queue(
            root,
            workers,
            retries,
            max_requests,
            initialize=partial(
                seed_images, feedback_root=feedback_root, workers=seed_workers
            ),
            expand=store_image,
            client_factory=ImageClient,
        )


def plan_images(root: Path, feedback_root: Path, workers: int = 4) -> dict:
    """Seed the queue and project the download without contacting the portal."""
    root.mkdir(parents=True, exist_ok=True)
    with (root / "collection.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with closing(
            seed_images(root, feedback_root=feedback_root, workers=workers)
        ) as db:
            by_edition: dict = {}
            for edition, status, count in db.execute(
                "SELECT edition,status,count(*) FROM requests GROUP BY 1,2"
            ):
                by_edition.setdefault(edition, {})[status] = count
            images, total_bytes, unique_files = db.execute(
                "SELECT count(*),sum(bytes),count(DISTINCT sha256) FROM images"
            ).fetchone()
            remaining = db.execute(
                "SELECT count(*) FROM requests WHERE status!='done'"
            ).fetchone()[0]
            plan = {
                "updated_at": datetime.now(UTC).isoformat(),
                "reports_seeded": db.execute(
                    "SELECT count(*) FROM seeded_reports"
                ).fetchone()[0],
                "image_references": db.execute(
                    "SELECT count(*) FROM image_refs"
                ).fetchone()[0],
                "unique_image_urls": db.execute(
                    "SELECT count(*) FROM requests"
                ).fetchone()[0],
                "by_edition": by_edition,
                "downloaded": {
                    "images": images,
                    "bytes": total_bytes or 0,
                    "unique_files": unique_files,
                },
                "projected_remaining_bytes": round(total_bytes / images * remaining)
                if images
                else None,
            }
    atomic_json(root / "image-plan.json", plan)
    return plan


def export_images(root: Path, *, allow_source_errors: bool = False) -> dict:
    """Write photo and report-reference tables from a drained, idle queue.

    The queue is read as is: failed and running requests are not rescheduled.
    `manifest.json` is written last, so its presence marks a consistent export.
    """
    if not (root / "collection.sqlite").is_file():
        raise ValueError("No image queue found")
    with (root / "collection.lock").open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError(
                "Image collection is running; export after it stops"
            ) from exc
        uri = f"{(root / 'collection.sqlite').resolve().as_uri()}?mode=ro"
        with closing(sqlite3.connect(uri, uri=True)) as db:
            db.row_factory = sqlite3.Row
            groups = dict(
                tuple(row)
                for row in db.execute("SELECT status,count(*) FROM requests GROUP BY 1")
            )
            allowed = {"done", "error"} if allow_source_errors else {"done"}
            if not groups or set(groups) - allowed:
                raise ValueError("Image collection is incomplete")
            output = root / "tables"
            output.mkdir(exist_ok=True)
            (output / "manifest.json").unlink(missing_ok=True)
            queries = {
                "images": (
                    "SELECT r.url AS image_url,r.edition,r.status,r.error,i.sha256,"
                    "i.bytes,i.content_type,i.width,i.height,i.path,i.fetched_at "
                    "FROM requests r LEFT JOIN images i ON r.url=i.url ORDER BY r.url"
                ),
                "image_refs": (
                    "SELECT * FROM image_refs ORDER BY report_url,image_ordinal"
                ),
            }
            counts = Counter()
            for name, query in queries.items():
                with pq.ParquetWriter(
                    output / f"{name}.parquet.part", SCHEMAS[name], compression="zstd"
                ) as writer:
                    cursor = db.execute(query)
                    while batch := cursor.fetchmany(10000):
                        rows = [dict(row) for row in batch]
                        for row in rows:
                            if name == "images":
                                row["image_id"] = row["image_url"].rsplit("/", 1)[-1]
                                if row["fetched_at"]:
                                    row["fetched_at"] = datetime.fromisoformat(
                                        row["fetched_at"]
                                    )
                        writer.write_table(
                            pa.Table.from_pylist(rows, schema=SCHEMAS[name])
                        )
                        counts[name] += len(rows)
            for name in queries:
                (output / f"{name}.parquet.part").replace(output / f"{name}.parquet")
            unique_files = db.execute(
                "SELECT count(DISTINCT sha256) FROM images"
            ).fetchone()[0]
        report = {
            "rows": dict(counts),
            "unique_files": unique_files,
            "requests": groups,
            "all_discovered_requests_succeeded": not groups.get("error"),
            "image_key": "image_url",
            "join_rule": "image_refs.image_url to images.image_url, many-to-one",
        }
        atomic_json(output / "manifest.json", report)
    return report
