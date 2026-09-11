# Data schema and interpretation

## Row units and keys

`gp_reports.parquet` contains one reported GP/TLB within one parent-report URL
and edition. Its key is (`edition`, `source_url`, `gp_code`). `observation_id`
hashes the URL and GP code and is stable across repeated downloads; it is not a
meeting identifier. Separate collection directories are separate snapshots.
Combining snapshots requires an additional snapshot identifier in the key.

Source rows are never collapsed across parent reports. `ambiguous_gp_keys.json`
lists repeated (`edition`, `state_code`, `gp_code`) keys. Resolve those cases
before using that shorter key in joins. Each source response must itself contain
unique codes, and the export checks that every queued GP row is represented.

`report_editions.parquet` has one row per edition. Join it to GP reports
many-to-one on `edition`, retaining every GP row and requiring all editions to
match. Its homepage dates describe campaign announcements, not verified bounds
on attendance or feedback submissions. A cross-edition join on state and GP code
is only a candidate panel: boundaries, codes and reporting coverage may change.
No crosswalk to dated LGD boundaries has been verified.

## Time fields

| Field | Type | Definition |
|---|---|---|
| `fetched_at` | UTC timestamp, microseconds | When the request was made; not the meeting date |
| `homepage_campaign_start`, `homepage_campaign_end` | Nullable date | Inclusive window as stated on the edition homepage, without asserting that every underlying record falls inside it |
| `homepage_plan_year` | String | Target plan year label, retained verbatim as a year pair |
| `scope_note` | String | Limitations on interpreting each edition's time scope |

The summary source has no meeting dates. Meeting-level records from the separate
year-selectable reports will require a distinct table with the requested financial
year, original date text, parsed meeting date, source geography, meeting type and
capture timestamp. An event key cannot be specified safely until the live response
shows whether the portal supplies an event ID or allows multiple meetings for one
GP on the same date. No annual attendance series is inferred from the summaries.

Homepage metadata was verified on 2026-09-11 against the links in
`src/gs_meetings/editions.json`. The `PPC2019` homepage's 2020 campaign label is
preserved without relabelling the archive or its aggregate counts.

## Geography and recodes

Source `code` becomes the string `gp_code` and `name` becomes `gp_name` in GP
reports. State, district and block context comes from the requests used to reach
that GP. District codes identify district panchayats, not necessarily administrative
districts. Missing intermediate levels remain null. `source_level` and
`source_tlb` preserve the source category and TLB flag. Names are not normalized.

All count fields are nullable signed 64-bit integers. JSON null stays null;
zero stays zero. Missing count columns, negative counts, noninteger counts and
unknown hierarchy values are validation failures saved for investigation. Raw
failed responses remain available in the capture directory. Nothing is imputed.

The [README column dictionary](README.md#columns) describes each count.
`gpdpFundUtilized` is renamed `funds_utilized_discussion` because the portal counts
discussion of funds, not rupees. Counts are not converted to rates: denominators
and repeat submissions have not been validated. Subgroups may overlap; attendance
inconsistencies are flagged without changing or dropping values. No winsorizing,
deduplication across parents, year assignment or geographic crosswalk is applied.

## Coverage and validation outputs

`request_coverage.parquet` has one row per discovered URL. Empty successful
responses remain visible. A complete queue means all discovered requests succeeded;
it does not establish that every GP or meeting in India was reported.
`hierarchy_summaries.parquet` preserves state, district and block rows as JSON
alongside their route context and capture timestamp.

`column_profiles.parquet` reports row count, null count, zero count, minimum and
maximum for each edition, state and metric. `reconciliation.parquet` compares
the GP sum with the reported immediate-parent value for each metric except
`totalGp`, whose meaning varies by hierarchy level. A null in either side prevents
a numeric comparison. A zero difference is a consistency check, not evidence that
the portal covers every GP. Parent and child captures can occur at different times.

`manifest.json` records the capture interval, row conservation, coverage gaps and
issue counts. `CHECKSUMS` verifies the exported artifacts. Original response text,
request URLs and capture timestamps are retained in compressed raw files so the
tables can be regenerated without another download.
