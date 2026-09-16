"""Write the deposit's data card from the exported manifests, so it cannot drift."""

import json
import shutil
from pathlib import Path

import pyarrow.parquet as pq

from gs_meetings.upload import PART_BYTES, depositable, staged_files

STAGES = ["national", "meetings", "feedback"]
REPOSITORY = "https://github.com/in-rolls/gs_meetings"


def table_rows(folder: Path, stage: str) -> list[tuple[str, int | None]]:
    """List every deposited file of a stage with its Parquet row count."""
    rows = []
    for path in sorted(folder.iterdir()):
        if not depositable(path):
            continue
        count = (
            pq.ParquetFile(path).metadata.num_rows
            if path.suffix == ".parquet"
            else None
        )
        rows.append((f"{stage}-{path.name}", count))
    return rows


def counts_table(title: str, counts: dict, ranges: dict | None = None) -> list[str]:
    """Render a key/count mapping, with optional first/last dates per key."""
    if not counts:
        return []
    header = f"| {title} | rows |" + (" first date | last date |" if ranges else "")
    rule = "|---|---|" + ("---|---|" if ranges else "")
    lines = ["", header, rule]
    for key, value in counts.items():
        line = f"| {key} | {value:,} |"
        if ranges:
            first, last = ranges.get(key, [None, None])
            line += f" {first or ''} | {last or ''} |"
        lines.append(line)
    return lines


def stage_section(stage: str, folder: Path, manifest: dict, parts: dict) -> list[str]:
    """Describe one stage: files, capture window, counts, and the manifest's caveats."""
    lines = [f"## {stage}", "", "| file | rows |", "|---|---|"]
    for name, count in table_rows(folder, stage):
        cell = f"{count:,}" if count is not None else ""
        lines.append(f"| `{name}` | {cell} |")
        if name in parts:
            joined = ", ".join(f"`{part}`" for part in parts[name])
            lines.append(f"| deposited as parts: {joined} | |")
    if manifest.get("capture_start"):
        lines += [
            "",
            f"Captured {manifest['capture_start'][:10]} to "
            f"{manifest['capture_end'][:10]} (UTC).",
        ]
    lines += counts_table("edition", manifest.get("rows_by_edition", {}))
    lines += counts_table(
        "edition:year", manifest.get("rows_by_period", {}), manifest.get("date_ranges")
    )
    lines += counts_table("outcome", manifest.get("outcomes", {}))
    absent = manifest.get("forms_absent_by_state", {})
    if absent:
        top = dict(sorted(absent.items(), key=lambda item: -item[1])[:10])
        lines += counts_table("state code (top ten by absent forms)", top)
    errors = len(manifest.get("source_errors", []))
    empties = len(manifest.get("empty_responses", []))
    lines += [
        "",
        f"The manifest lists {errors:,} source errors and {empties:,} empty "
        "responses; neither is evidence that nothing exists at that route.",
    ]
    for key in ["measurement", "coverage", "outcome_rule"]:
        if manifest.get(key):
            lines.append(f"{key.replace('_', ' ').capitalize()}: {manifest[key]}.")
    return [*lines, ""]


def write_data_card(root: Path, *, schema: Path, version: str) -> dict:
    """Write deposit/README_DATA.md and copy the schema beside it."""
    stages = [
        stage
        for stage in STAGES
        if (root / stage / "tables" / "manifest.json").is_file()
    ]
    if not stages:
        raise ValueError(f"No exported manifests under {root}")
    missing = [stage for stage in STAGES if stage not in stages]
    # Stage the split the same way upload will, so the card knows the parts
    # before the first upload and regardless of which state file exists.
    _, parts = staged_files(root, PART_BYTES)
    lines = [
        "# Gram Sabha participation reports from gpdp.nic.in",
        "",
        f"Tables exported by gs_meetings {version} ({REPOSITORY}) from the Ministry "
        "of Panchayati Raj portal. Each stage carries its own `manifest.json`, "
        "`SCHEMA.json` and `CHECKSUMS`; `SCHEMA.md` defines row units, keys and "
        "caveats. Counts are administrative reports as published, not verified "
        "attendance. A file deposited as numbered parts is one table split by row "
        "group; read the parts together to rebuild it.",
        "",
    ]
    if missing:
        lines += [
            "Stages not included in this deposit: " + ", ".join(missing) + ".",
            "",
        ]
    for stage in stages:
        folder = root / stage / "tables"
        manifest = json.loads((folder / "manifest.json").read_text())
        lines += stage_section(stage, folder, manifest, parts)
    lines += [
        "## Citation",
        "",
        "Cite the portal's report URL, edition and capture date for the records, "
        f"and the software via its CITATION.cff at {REPOSITORY}.",
        "",
    ]
    deposit = root / "deposit"
    deposit.mkdir(exist_ok=True)
    (deposit / "README_DATA.md").write_text("\n".join(lines))
    shutil.copyfile(schema, deposit / "SCHEMA.md")
    return {
        "stages": stages,
        "missing": missing,
        "path": str(deposit / "README_DATA.md"),
    }
