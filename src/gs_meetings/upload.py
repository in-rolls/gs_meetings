"""Deposit exported tables on Zenodo as a draft, then publish on request.

The deposition API is four calls, and the upload has to run in two steps:
create a draft with the finished stages, add later stages, publish once.
The maintained wrapper (zenodo-client) publishes on create and update and
cannot add files to an existing draft, so the calls are made directly.
"""

import configparser
import hashlib
import json
import logging
import os
from importlib.metadata import version as package_version
from pathlib import Path

import requests

from gs_meetings.fetch import atomic_json

LOG = logging.getLogger(__name__)
CONFIG = Path.home() / ".config" / "zenodo.ini"
HOSTS = {False: "https://zenodo.org", True: "https://sandbox.zenodo.org"}
REPOSITORY = "https://github.com/in-rolls/gs_meetings"
TIMEOUT = (15, 1800)


def load_token(*, sandbox: bool, config: Path = CONFIG) -> str:
    """Read the API token from the environment or the pystow-style config file."""
    variable = "ZENODO_SANDBOX_TOKEN" if sandbox else "ZENODO_TOKEN"
    if os.environ.get(variable):
        return os.environ[variable]
    parser = configparser.ConfigParser()
    parser.read(config)
    key = "sandbox_api_token" if sandbox else "api_token"
    token = parser.get("zenodo", key, fallback=None)
    if not token:
        raise ValueError(f"Set {variable} or [zenodo] {key} in {config}")
    return token


def metadata(version: str) -> dict:
    """Describe the deposit; the tables carry their own manifests and schemas."""
    return {
        "upload_type": "dataset",
        "title": "Gram Sabha participation reports from gpdp.nic.in",
        "creators": [{"name": "Sood, Gaurav"}],
        "description": (
            "<p>Official Gram Sabha participation reports collected from the "
            "Ministry of Panchayati Raj portal (gpdp.nic.in): GP-level summary "
            "aggregates for the live report and four campaign archives, dated "
            "meeting listings, and individual facilitator reports with their "
            "links to the listings. Each stage ships a manifest, a schema and "
            "checksums. Counts are administrative reports as published by the "
            "portal, not verified attendance.</p>"
            f"<p>Collected with <a href='{REPOSITORY}'>gs_meetings</a> "
            f"{version}; see SCHEMA.md for row units, keys and caveats.</p>"
        ),
        "access_right": "open",
        "license": "cc0-1.0",
        "version": version,
        "language": "eng",
        "keywords": ["India", "Gram Sabha", "panchayat", "participation", "GPDP"],
        "related_identifiers": [
            {"identifier": REPOSITORY, "relation": "isSupplementTo"},
            {
                "identifier": "https://gpdp.nic.in/summaryAnalysisReport.html",
                "relation": "isDerivedFrom",
            },
        ],
    }


def deposit_files(root: Path) -> list[tuple[str, Path]]:
    """Name every stage's table with its stage; extra deposit files keep their names."""
    files = [
        (f"{path.parent.parent.name}-{path.name}", path)
        for path in sorted(root.glob("*/tables/*"))
        if path.is_file() and not path.name.endswith(".part")
    ]
    files.extend((path.name, path) for path in sorted(root.glob("deposit/*")))
    if not files:
        raise ValueError(f"No exported tables under {root}")
    return files


def md5(path: Path) -> str:
    """Zenodo reports MD5 checksums for existing files; match them to skip uploads."""
    digest = hashlib.md5()  # noqa: S324
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def deposit(
    root: Path,
    *,
    session: requests.Session | None = None,
    base_url: str | None = None,
    sandbox: bool = False,
    version: str | None = None,
    publish: bool = False,
    deposition_id: int | None = None,
) -> dict:
    """Create or reuse the draft named in root/zenodo.json and upload changed files.

    The draft's identity is saved as soon as it exists so that an interrupted
    upload resumes into the same deposition instead of stranding it.
    """
    base_url = base_url or HOSTS[sandbox]
    version = version or package_version("gs-meetings")
    files = deposit_files(root)
    if session is None:
        session = requests.Session()
        session.headers["Authorization"] = f"Bearer {load_token(sandbox=sandbox)}"
    state_path = root / ("zenodo-sandbox.json" if sandbox else "zenodo.json")
    if deposition_id is None and state_path.is_file():
        state = json.loads(state_path.read_text())
        if state.get("base_url") == base_url:
            deposition_id = state["deposition_id"]
    endpoint = f"{base_url}/api/deposit/depositions"
    if deposition_id is None:
        response = session.post(endpoint, json={}, timeout=TIMEOUT)
        checked(response)
        record = response.json()
        deposition_id = record["id"]
        LOG.info("Created draft deposition %s", deposition_id)
        atomic_json(
            state_path,
            {
                "deposition_id": deposition_id,
                "doi": record["metadata"].get("prereserve_doi", {}).get("doi"),
                "html": record["links"]["html"],
                "base_url": base_url,
                "published": False,
            },
        )
    else:
        response = session.get(f"{endpoint}/{deposition_id}", timeout=TIMEOUT)
        checked(response)
        record = response.json()
    if record.get("submitted"):
        raise ValueError(f"Deposition {deposition_id} is already published")
    response = session.put(
        f"{endpoint}/{deposition_id}",
        json={"metadata": metadata(version)},
        timeout=TIMEOUT,
    )
    checked(response)
    record = response.json()
    existing = {
        entry["filename"]: entry["checksum"].removeprefix("md5:")
        for entry in record.get("files", [])
    }
    uploaded = skipped = 0
    for name, path in files:
        if existing.get(name) == md5(path):
            skipped += 1
            continue
        with path.open("rb") as stream:
            response = session.put(
                f"{record['links']['bucket']}/{name}", data=stream, timeout=TIMEOUT
            )
        checked(response)
        uploaded += 1
        LOG.info("Uploaded %s (%s bytes)", name, path.stat().st_size)
    doi = record["metadata"].get("prereserve_doi", {}).get("doi")
    if publish:
        response = session.post(
            f"{endpoint}/{deposition_id}/actions/publish", timeout=TIMEOUT
        )
        checked(response)
        doi = response.json().get("doi", doi)
    report = {
        "deposition_id": deposition_id,
        "doi": doi,
        "html": record["links"]["html"],
        "base_url": base_url,
        "version": version,
        "uploaded": uploaded,
        "skipped": skipped,
        "published": publish,
    }
    atomic_json(state_path, report)
    return report


def checked(response: requests.Response) -> None:
    """Surface Zenodo's error message rather than a bare status code."""
    if not response.ok:
        raise ValueError(f"Zenodo returned {response.status_code}: {response.text}")
