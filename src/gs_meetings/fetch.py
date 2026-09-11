"""Save complete raw responses atomically and resume successful requests."""

import gzip
import hashlib
import json
import logging
import zlib
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from gs_meetings.source import EDITIONS, validate_rows

LOG = logging.getLogger(__name__)


def atomic_json(path: Path, value: object) -> None:
    """Replace a JSON document only after the new version is complete."""
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_suffix(path.suffix + ".part")
    part.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    part.replace(path)


def read_capture(path: Path) -> dict | None:
    """Return a complete successful capture; interrupted writes are retried."""
    try:
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            record = json.loads(stream.read())
        if record.get("ok") is True and record.get("done") is True:
            validate_rows(json.loads(record["body"]))
            return record
    except (
        OSError,
        EOFError,
        zlib.error,
        ValueError,
        TypeError,
        KeyError,
        AttributeError,
    ):
        return None
    return None


class Client:
    """One sequential HTTP session with bounded, configurable retries."""

    def __init__(self, root: Path, edition: str, retries: int = 4):
        """Set up TLS-verified requests and a directory for this snapshot."""
        self.root = root
        self.edition = edition
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    f"gs-meetings/{version('gs-meetings')} "
                    "(research; contact@gsood.com)"
                ),
                "Referer": f"https://gpdp.nic.in/{EDITIONS[edition]}summaryAnalysisReport.html",
            }
        )
        policy = Retry(
            total=retries,
            backoff_factor=2,
            backoff_max=600,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods={"GET"},
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=policy))

    def close(self) -> None:
        """Release the connection pool."""
        self.session.close()

    def get(self, url: str) -> tuple[list[dict], Path]:
        """Keep response text and provenance even when validation fails."""
        key = hashlib.sha256(url.encode()).hexdigest()
        path = self.root / "raw" / self.edition / f"{key}.jsonl.gz"
        cached = read_capture(path)
        if cached is not None and cached["url"] == url:
            return json.loads(cached["body"]), path
        record = {
            "url": url,
            "edition": self.edition,
            "fetched_at": datetime.now(UTC).isoformat(),
            "ok": False,
            "done": False,
            "body": None,
        }
        try:
            response = self.session.get(url, timeout=(15, 60))
            record.update(
                status=response.status_code, body=response.text, final_url=response.url
            )
            response.raise_for_status()
            rows = validate_rows(response.json())
            record.update(ok=True, done=True)
        except (requests.RequestException, ValueError) as exc:
            record["reason"] = str(exc)
            raise
        finally:
            path.parent.mkdir(parents=True, exist_ok=True)
            part = path.with_suffix(path.suffix + ".part")
            with gzip.open(part, "wt", encoding="utf-8") as stream:
                stream.write(json.dumps(record, ensure_ascii=False) + "\n")
            part.replace(path)
            LOG.info("url=%s ok=%s", url, record["ok"])
        return rows, path
