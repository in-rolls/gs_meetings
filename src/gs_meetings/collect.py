"""Collect national report hierarchies through a durable SQLite request queue."""

import json
import logging
import shutil
import sqlite3
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import UTC, datetime
from pathlib import Path

import requests

from gs_meetings.fetch import Client, atomic_json
from gs_meetings.source import EDITIONS, endpoint

LOG = logging.getLogger(__name__)
PRIORITY = {"state": 0, "district": 1, "block": 2, "gp": 3}


def add_request(
    db: sqlite3.Connection,
    edition: str,
    level: str,
    context: dict,
    parent: dict | None = None,
    url: str | None = None,
    **params: int,
) -> None:
    """Queue a unique source URL, rejecting conflicting geography assignments."""
    url = url or endpoint(edition, level, **params)
    encoded = json.dumps(context, sort_keys=True)
    existing = db.execute("SELECT context FROM requests WHERE url=?", (url,)).fetchone()
    if existing and existing[0] != encoded:
        raise ValueError(f"Conflicting geography for {url}")
    db.execute(
        "INSERT OR IGNORE INTO requests "
        "(url,edition,level,priority,context,parent,status) VALUES (?,?,?,?,?,?,?)",
        (
            url,
            edition,
            level,
            PRIORITY[level],
            encoded,
            json.dumps(parent, ensure_ascii=False),
            "pending",
        ),
    )


def open_queue(root: Path, *, seed_summaries: bool = True) -> sqlite3.Connection:
    """Open the checkpoint and recover requests interrupted by a previous run."""
    root.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(root / "collection.sqlite")
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute(
        "CREATE TABLE IF NOT EXISTS requests (url TEXT PRIMARY KEY, edition TEXT, "
        "level TEXT, priority INTEGER, context TEXT, parent TEXT, status TEXT, "
        "rows INTEGER, error TEXT, attempts INTEGER DEFAULT 0)"
    )
    db.execute(
        "CREATE INDEX IF NOT EXISTS requests_pending "
        "ON requests(status,priority,attempts,url)"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS coverage_gaps "
        "(url TEXT, raw_row TEXT, reason TEXT, PRIMARY KEY(url,raw_row,reason))"
    )
    db.execute(
        "UPDATE requests SET status='pending' WHERE status IN ('running','error')"
    )
    if seed_summaries:
        for edition in EDITIONS:
            add_request(db, edition, "state", {"edition": edition})
    db.commit()
    return db


def children(db: sqlite3.Connection, task: dict, rows: list[dict]) -> None:
    """Follow the exact geographic routes used by the official controller."""
    level, edition = task["level"], task["edition"]
    context = json.loads(task["context"])
    for row in rows:
        if level == "gp":
            break
        if level == "state":
            state = row["code"]
            path = {**context, "state_code": str(state), "state_name": row["name"]}
            if state in {4, 7}:
                continue
            if state in {13, 15, 17}:
                path.update(parent_level="state", parent_code=str(state))
                add_request(db, edition, "gp", path, row, stateId=state, code=state)
            elif state == 34:
                add_request(
                    db, edition, "block", path, row, stateId=state, zpCode=state
                )
            else:
                add_request(db, edition, "district", path, row, stateId=state)
        elif level == "district":
            path = {
                **context,
                "district_code": str(row["code"]),
                "district_name": row["name"],
            }
            state = int(path["state_code"])
            if row["level"] == "I":
                add_request(
                    db, edition, "block", path, row, stateId=state, zpCode=row["code"]
                )
            elif row["level"] == "V":
                path.update(parent_level="district", parent_code=str(row["code"]))
                add_request(
                    db, edition, "gp", path, row, stateId=state, code=row["code"]
                )
            else:
                db.execute(
                    "INSERT OR IGNORE INTO coverage_gaps VALUES (?,?,?)",
                    (
                        task["url"],
                        json.dumps(row, ensure_ascii=False),
                        "Missing district routing level",
                    ),
                )
        elif level == "block":
            path = {
                **context,
                "block_code": str(row["code"]),
                "block_name": row["name"],
                "parent_level": "block",
                "parent_code": str(row["code"]),
            }
            add_request(
                db,
                edition,
                "gp",
                path,
                row,
                stateId=int(path["state_code"]),
                code=row["code"],
            )


def progress(db: sqlite3.Connection, root: Path) -> dict:
    """Write completion counts and retained failures for monitoring and handoff."""
    groups = [
        dict(row)
        for row in db.execute(
            "SELECT edition,level,status,COUNT(*) AS requests,SUM(rows) AS rows "
            "FROM requests GROUP BY edition,level,status "
            "ORDER BY edition,priority,status"
        )
    ]
    errors = [
        dict(row)
        for row in db.execute(
            "SELECT edition,url,error FROM requests WHERE status='error'"
        )
    ]
    result = {
        "updated_at": datetime.now(UTC).isoformat(),
        "groups": groups,
        "errors": errors,
        "display_excluded_states": [4, 7],
    }
    atomic_json(root / "collection-progress.json", result)
    return result


def collect(
    root: Path, workers: int = 8, retries: int = 8, max_requests: int | None = None
) -> dict:
    """Drain all available report requests, preserving failed requests for resume."""
    import fcntl

    root.mkdir(parents=True, exist_ok=True)
    with (root / "collection.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return run_queue(root, workers, retries, max_requests)


def run_queue(
    root: Path,
    workers: int,
    retries: int,
    max_requests: int | None,
    *,
    initialize=None,
    expand=None,
    client_factory=None,
) -> dict:
    """Use one session per thread and edition; mutate the queue only in this thread."""
    db = (initialize or open_queue)(root)
    expand = expand or children
    client_factory = client_factory or Client
    local = threading.local()
    clients = []
    client_lock = threading.Lock()

    def fetch(task: dict) -> list[dict]:
        if not hasattr(local, "clients"):
            local.clients = {}
        if task["edition"] not in local.clients:
            client = client_factory(root, task["edition"], retries)
            local.clients[task["edition"]] = client
            with client_lock:
                clients.append(client)
        return local.clients[task["edition"]].get(task["url"])[0]

    submitted = finished = 0
    last_progress = 0.0
    storage_limited = False
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            pending = {}
            while True:
                if (
                    time.monotonic() - last_progress >= 30
                    and shutil.disk_usage(root).free < 2 * 1024**3
                ):
                    storage_limited = True
                    LOG.error("Less than 2 GiB free; stopping new requests")
                while (
                    not storage_limited
                    and len(pending) < workers
                    and (max_requests is None or submitted < max_requests)
                ):
                    task = db.execute(
                        "SELECT * FROM requests WHERE status='pending' "
                        "ORDER BY priority,attempts,url LIMIT 1"
                    ).fetchone()
                    if task is None:
                        break
                    task = dict(task)
                    db.execute(
                        "UPDATE requests SET status='running',attempts=attempts+1 "
                        "WHERE url=?",
                        (task["url"],),
                    )
                    db.commit()
                    pending[pool.submit(fetch, task)] = task
                    submitted += 1
                if not pending:
                    break
                done, _ = wait(pending, timeout=30, return_when=FIRST_COMPLETED)
                for future in done:
                    task = pending.pop(future)
                    try:
                        rows = future.result()
                        db.execute(
                            "DELETE FROM coverage_gaps WHERE url=?", (task["url"],)
                        )
                        expand(db, task, rows)
                        db.execute(
                            "UPDATE requests SET status='done',rows=?,error=NULL "
                            "WHERE url=?",
                            (len(rows), task["url"]),
                        )
                    except (requests.RequestException, ValueError) as exc:
                        db.rollback()
                        db.execute(
                            "UPDATE requests SET status='error',error=? WHERE url=?",
                            (str(exc), task["url"]),
                        )
                        LOG.error("Failed %s: %s", task["url"], exc)
                    db.commit()
                    finished += 1
                if time.monotonic() - last_progress >= 30:
                    progress(db, root)
                    last_progress = time.monotonic()
                if finished and finished % 100 == 0:
                    LOG.info("Completed %s requests in this invocation", finished)
        result = progress(db, root)
        result["storage_limited"] = storage_limited
        atomic_json(root / "collection-progress.json", result)
        return result
    finally:
        for client in clients:
            client.close()
        db.close()
