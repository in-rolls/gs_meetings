"""Enumerate GP report requests using the portal's panchayat hierarchy."""

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from gs_meetings.fetch import Client, atomic_json
from gs_meetings.source import endpoint

FRAME_FIELDS = [
    "edition",
    "state_code",
    "state_name",
    "district_code",
    "district_name",
    "block_code",
    "block_name",
    "parent_level",
    "parent_code",
    "url",
    "expected",
]
FRAME_SCHEMA = pa.schema([(key, pa.string()) for key in FRAME_FIELDS])


def write_parquet(path: Path, rows: list[dict], schema: pa.Schema) -> None:
    """Write a typed Parquet file without exposing a partial replacement."""
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_suffix(".parquet.part")
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), part)
    part.replace(path)


def enumerate_units(
    client: Client, state_code: int, district_code: int | None = None
) -> list[dict]:
    """Enumerate one state, optionally restricted to a district panchayat."""
    edition = client.edition
    states, _ = client.get(endpoint(edition, "state"))
    state = next((row for row in states if row["code"] == state_code), None)
    if state is None:
        raise ValueError(f"State {state_code} is absent from {edition}")
    context = {
        "edition": edition,
        "state_code": str(state_code),
        "state_name": state["name"],
    }
    units = []

    def add(parent: dict, level: str, path: dict) -> None:
        units.append(
            {
                **path,
                "parent_level": level,
                "parent_code": str(parent["code"]),
                "url": endpoint(edition, "gp", stateId=state_code, code=parent["code"]),
                "expected": json.dumps(parent, ensure_ascii=False),
            }
        )

    def blocks(parent: dict, path: dict) -> None:
        rows, _ = client.get(
            endpoint(edition, "block", stateId=state_code, zpCode=parent["code"])
        )
        if not rows:
            raise ValueError(
                f"Empty block listing for {parent['code']}; coverage unverified"
            )
        for block in rows:
            add(
                block,
                "block",
                {**path, "block_code": str(block["code"]), "block_name": block["name"]},
            )

    if state_code in {13, 15, 17, 34} and district_code is not None:
        raise ValueError("This state's portal route does not use a district filter")
    if state_code in {13, 15, 17}:
        add(state, "state", context)
    elif state_code == 34:
        blocks(state, context)
    else:
        districts, _ = client.get(endpoint(edition, "district", stateId=state_code))
        if district_code is not None:
            districts = [row for row in districts if row["code"] == district_code]
        if not districts:
            raise ValueError("No matching district panchayats; coverage unverified")
        for district in districts:
            path = {
                **context,
                "district_code": str(district["code"]),
                "district_name": district["name"],
            }
            if district["level"] == "I":
                blocks(district, path)
            elif district["level"] == "V":
                add(district, "district", path)
            else:
                raise ValueError(f"Unknown district routing level: {district['level']}")
    write_parquet(client.root / "frame.parquet", units, FRAME_SCHEMA)
    atomic_json(
        client.root / "frame-manifest.json",
        {
            "edition": edition,
            "state_code": state_code,
            "district_code": district_code,
            "gp_report_requests": len(units),
            "state_summary": state,
            "coverage": "Only the selected hierarchy; not national coverage",
        },
    )
    return units


def fetch_units(client: Client, frame: Path, limit: int | None = None) -> dict:
    """Fetch each enumerated GP report; save failures and continue other units."""
    units = pq.read_table(frame).to_pylist()
    if any(unit["edition"] != client.edition for unit in units):
        raise ValueError("Frame edition does not match the client")
    import requests

    result = {"units_in_frame": len(units), "succeeded": 0, "failures": []}
    for unit in units[:limit]:
        try:
            client.get(unit["url"])
            result["succeeded"] += 1
        except (requests.RequestException, ValueError) as exc:
            result["failures"].append({"url": unit["url"], "reason": str(exc)})
    atomic_json(client.root / "fetch-manifest.json", result)
    return result
