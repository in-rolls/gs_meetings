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
import shutil
import time
from importlib.metadata import version as package_version
from pathlib import Path

import pyarrow.parquet as pq
import requests

from gs_meetings.fetch import atomic_json

LOG = logging.getLogger(__name__)
CONFIG = Path.home() / ".config" / "zenodo.ini"
HOSTS = {False: "https://zenodo.org", True: "https://sandbox.zenodo.org"}
REPOSITORY = "https://github.com/in-rolls/gs_meetings"
TIMEOUT = (15, 1800)
# Zenodo's proxy dropped a 100 MiB PUT after about thirty minutes and a
# 92 MiB one succeeded, at the 60 KB/s uploads from here get; 50 MiB stays
# clear of both a size and a duration cutoff.
PART_BYTES = 50 * 1024**2
ATTEMPTS = 4


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
            "portal, not verified attendance. Requests the portal did not answer "
            "are listed under source_errors in each manifest and counted in "
            "README_DATA.md.</p>"
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


def depositable(path: Path) -> bool:
    """Skip partial writes, editor droppings and empty files, which Zenodo rejects."""
    return (
        path.is_file()
        and not path.name.startswith(".")
        and not path.name.endswith(".part")
        and path.stat().st_size > 0
    )


def deposit_files(root: Path) -> list[tuple[str, Path]]:
    """Name every stage's table with its stage; extra deposit files keep their names."""
    files = [
        (f"{path.parent.parent.name}-{path.name}", path)
        for path in sorted(root.glob("*/tables/*"))
        if depositable(path)
    ]
    files.extend(
        (path.name, path)
        for path in sorted(root.glob("deposit/*"))
        if depositable(path)
    )
    if not files:
        raise ValueError(f"No exported tables under {root}")
    return files


def part_groups(source: pq.ParquetFile, part_bytes: int) -> list[list[int]]:
    """Group row-group indices into parts of about the limit each.

    A part closes once the next row group would push it past the limit, so a
    part can exceed it by one row group and by re-encoding.
    """
    groups: list[list[int]] = []
    written = 0
    for index in range(source.num_row_groups):
        meta = source.metadata.row_group(index)
        estimate = sum(
            meta.column(column).total_compressed_size
            for column in range(meta.num_columns)
        )
        if not groups or written + estimate > part_bytes:
            groups.append([])
            written = 0
        groups[-1].append(index)
        written += estimate
    return groups


def part_names(name: str, count: int) -> list[str]:
    """Number a table's parts so name order is row order; two digits suffice."""
    if count > 99:
        raise ValueError(f"{name} would need {count} parts; raise the part size")
    stem = name.removesuffix(".parquet")
    return [f"{stem}-{index:02d}.parquet" for index in range(1, count + 1)]


def plan_parts(name: str, path: Path, part_bytes: int) -> list[str]:
    """Name the parts a table would be deposited as, without writing them."""
    if path.suffix != ".parquet" or path.stat().st_size <= part_bytes:
        return []
    groups = part_groups(pq.ParquetFile(path), part_bytes)
    return part_names(name, len(groups)) if len(groups) > 1 else []


def split_parquet(name: str, path: Path, folder: Path, part_bytes: int) -> list[Path]:
    """Write the planned parts; they share the source schema, in name order."""
    source = pq.ParquetFile(path)
    groups = part_groups(source, part_bytes)
    parts = [folder / part for part in part_names(name, len(groups))]
    for part, indices in zip(parts, groups, strict=True):
        with pq.ParquetWriter(part, source.schema_arrow, compression="zstd") as writer:
            for index in indices:
                writer.write_table(source.read_row_group(index))
    return parts


def staged_files(
    root: Path, part_bytes: int
) -> tuple[list[tuple[str, Path]], dict[str, list[str]]]:
    """Replace oversized Parquet files with parts written under root/deposit-parts."""
    folder = root / "deposit-parts"
    if folder.exists():
        shutil.rmtree(folder)
    staged: list[tuple[str, Path]] = []
    parts: dict[str, list[str]] = {}
    for name, path in deposit_files(root):
        if plan_parts(name, path, part_bytes):
            folder.mkdir(exist_ok=True)
            pieces = split_parquet(name, path, folder, part_bytes)
            parts[name] = [piece.name for piece in pieces]
            staged.extend((piece.name, piece) for piece in pieces)
        else:
            staged.append((name, path))
    return staged, parts


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
    new_version: bool = False,
    part_bytes: int = PART_BYTES,
    backoff: float = 30,
) -> dict:
    """Create or reuse the draft named in root/zenodo.json and upload changed files.

    The draft's identity is saved as soon as it exists so that an interrupted
    upload resumes into the same deposition instead of stranding it. A published
    record is never modified; with new_version its files are carried into a new
    draft, which then receives the changed files and, on request, is published.
    """
    base_url = base_url or HOSTS[sandbox]
    version = version or package_version("gs-meetings")
    files, parts = staged_files(root, part_bytes)
    if session is None:
        session = requests.Session()
        session.headers["Authorization"] = f"Bearer {load_token(sandbox=sandbox)}"
    state_path = root / ("zenodo-sandbox.json" if sandbox else "zenodo.json")
    previous = json.loads(state_path.read_text()) if state_path.is_file() else {}
    if deposition_id is None and previous.get("base_url") == base_url:
        deposition_id = previous["deposition_id"]
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
                "concept_doi": previous.get("concept_doi"),
                "published": False,
            },
        )
    else:
        response = session.get(f"{endpoint}/{deposition_id}", timeout=TIMEOUT)
        checked(response)
        record = response.json()
    if record.get("submitted"):
        if not new_version:
            raise ValueError(
                f"Deposition {deposition_id} is already published; "
                "pass --new-version to add a version"
            )
        response = session.post(
            f"{endpoint}/{deposition_id}/actions/newversion", timeout=TIMEOUT
        )
        checked(response)
        draft_url = response.json()["links"]["latest_draft"]
        response = session.get(draft_url, timeout=TIMEOUT)
        checked(response)
        record = response.json()
        deposition_id = record["id"]
        LOG.info("Opened new version draft %s", deposition_id)
        atomic_json(
            state_path,
            {
                "deposition_id": deposition_id,
                "doi": record["metadata"].get("prereserve_doi", {}).get("doi"),
                "html": record["links"]["html"],
                "base_url": base_url,
                "concept_doi": previous.get("concept_doi"),
                "published": False,
            },
        )
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
        put_file(session, f"{record['links']['bucket']}/{name}", path, backoff)
        uploaded += 1
        LOG.info("Uploaded %s (%s bytes)", name, path.stat().st_size)
    removed = remove_superseded(
        session,
        f"{endpoint}/{deposition_id}",
        record.get("files", []),
        [name for name, _ in files],
        parts,
    )
    doi = record["metadata"].get("prereserve_doi", {}).get("doi")
    concept_doi = record.get("conceptdoi") or previous.get("concept_doi")
    if publish:
        response = session.post(
            f"{endpoint}/{deposition_id}/actions/publish", timeout=TIMEOUT
        )
        checked(response)
        published = response.json()
        doi = published.get("doi", doi)
        concept_doi = published.get("conceptdoi") or concept_doi
    report = {
        "deposition_id": deposition_id,
        "doi": doi,
        "html": record["links"]["html"],
        "base_url": base_url,
        "version": version,
        "uploaded": uploaded,
        "skipped": skipped,
        "removed": removed,
        "parts": parts,
        "concept_doi": concept_doi,
        "published": publish,
    }
    atomic_json(state_path, report)
    return report


def is_part_of(name: str, whole: str) -> bool:
    """Match the numbered part names that split_parquet derives from a table."""
    stem = whole.removesuffix(".parquet")
    return (
        name.startswith(f"{stem}-")
        and name.endswith(".parquet")
        and name[len(stem) + 1 : -len(".parquet")].isdigit()
    )


def superseded_names(
    remote: list[str], staged: list[str], parts: dict[str, list[str]]
) -> list[str]:
    """Name deposited files the current staging replaces.

    A table now deposited as parts supersedes its whole file and any part not in
    the current split; a table deposited whole supersedes its earlier parts.
    """
    stale = set()
    for whole, pieces in parts.items():
        stale.update(
            name
            for name in remote
            if name == whole or (is_part_of(name, whole) and name not in pieces)
        )
    for whole in staged:
        if whole.endswith(".parquet") and whole not in parts:
            stale.update(name for name in remote if is_part_of(name, whole))
    return sorted(stale - set(staged))


def remove_superseded(
    session: requests.Session,
    deposition_url: str,
    files: list[dict],
    staged: list[str],
    parts: dict,
) -> list[str]:
    """Delete deposited files that the current staging replaces."""
    stale = superseded_names([entry["filename"] for entry in files], staged, parts)
    ids = {entry["filename"]: entry["id"] for entry in files}
    for name in stale:
        checked(session.delete(f"{deposition_url}/files/{ids[name]}", timeout=TIMEOUT))
        LOG.info("Removed %s, superseded by parts", name)
    return stale


def put_file(session: requests.Session, url: str, path: Path, backoff: float) -> None:
    """Retry a dropped PUT from the start; Zenodo only keeps completed objects."""
    for attempt in range(1, ATTEMPTS + 1):
        try:
            with path.open("rb") as stream:
                response = session.put(url, data=stream, timeout=TIMEOUT)
        except requests.RequestException as exc:
            if attempt == ATTEMPTS:
                raise ValueError(
                    f"Upload of {path.name} failed after {ATTEMPTS} attempts: {exc}"
                ) from exc
            failure: object = exc
        else:
            if response.ok:
                return
            if attempt == ATTEMPTS or response.status_code < 500:
                raise ValueError(
                    f"Upload of {path.name} failed: Zenodo returned "
                    f"{response.status_code}: {response.text}"
                )
            failure = response.status_code
        LOG.warning("Upload of %s failed with %s; retrying", path.name, failure)
        time.sleep(backoff * attempt)


def checked(response: requests.Response) -> None:
    """Surface Zenodo's error message rather than a bare status code."""
    if not response.ok:
        raise ValueError(f"Zenodo returned {response.status_code}: {response.text}")
