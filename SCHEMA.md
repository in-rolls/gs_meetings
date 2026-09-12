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

The summary source has no meeting dates. The separate dated listings are stored in
`meetings.parquet`. No annual attendance series is inferred from the summaries.

| Dated-table field | Type | Definition |
|---|---|---|
| `edition`, `financial_year` | String, nullable year label | Source edition and requested financial year; archive years are left null |
| `meeting_date_raw`, `meeting_date` | String and nullable date | Original day-month-year text and parsed date |
| `date_error` | Nullable string | Unparseable dates are retained and flagged |
| `outside_financial_year` | Nullable boolean | Date falls outside April–March of the requested year; a check, not a correction |
| `local_body_code`, `local_body_name` | String | Source local body; its category depends on `report_scope` |
| `report_scope` | String | GP (`G`), district (`Z`) or block (`B`) report requested |
| `hierarchy` | JSON string | Ordered source parent codes, names and levels |
| `meeting_type`, `requested_meeting_type` | Nullable string | Returned type and archive selector; retained separately |
| `source_row_id`, `row_ordinal` | Nullable string, integer | Source ID and one-based position in the response |
| `observation_id` | String | Hash of URL and row position within this snapshot |
| `candidate_event_key` | String | Hash of edition, state, scope, local body, raw date and returned type; repeated appearances under different requested years remain detectable |
| `source_url`, `fetched_at`, `raw_row` | String, UTC timestamp, JSON string | Provenance and complete original row |

The dated-table key is (`source_url`, `row_ordinal`) within one snapshot. Its source
IDs reset across reports and are not treated as permanent meeting IDs. Repeated
candidate event keys are reported without merging rows. Multiple meetings may
occur on the same date, and the available fields do not always distinguish them.
Do not join attendance aggregates to individual meetings as if they were that
meeting's attendance. A GP aggregate joined to several dates would repeat its total.

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

The dated export reports observed date ranges by edition/requested year, missing
and invalid dates, and dates outside the requested financial year. It retains
unknown hierarchy rows as routing gaps. With `--allow-source-errors`, either export
includes failed URLs and sets `all_discovered_requests_succeeded` to false.
It still refuses a queue with pending or running work. Missing children of failed
or uninterpretable parents cannot be counted from that response.

## Facilitator reports and meeting links

`feedback.parquet` has one row per report URL. It retains the edition, state and
local-body code, the date stated inside the report, UTC capture timestamp, five
nullable attendance counts, and nullable yes/no indicators for presentations,
discussions, quorum, Mahila Sabha and Bal Sabha. These indicators describe one
report, whereas similarly named fields in the summary data are aggregate counts.
Missing fields remain null; an absent checkbox or answer is not a reported no.
The archive editions (`PPC`, `PPC2018`, `PPC2019`) show the GPDP discussion
questions as unlabelled text beside a yes/no icon; the parser reads them like the
labelled questions on the current portal. Questions an edition does not ask,
such as quorum and Mahila/Bal Sabha in the archives, stay null.
For HTML answers, whitespace is collapsed and blank text becomes null. Multi-line
answers such as the Sankalp focus areas and SDGs keep one item per line. Comma
separators are removed before parsing attendance counts; the raw HTML is retained.
`images` is a JSON list of the report's photos, each with its portal-relative
`src` (for example `file/image/810091`) and displayed caption; the photos
themselves are not downloaded.

`feedback_answers.parquet` preserves every labelled question and its text or
boolean response. Its key is (`source_url`, `answer_ordinal`). Question wording
can vary with edition, year and local-body category. `feedback_tables.parquet`
preserves headers and ordered cells as Arrow lists/structs; its key is
(`source_url`, `row_ordinal`). Department tables include the representative's name
and whether they attended or presented. Document links are retained as metadata;
linked media files are not downloaded by this package.

`feedback_links.parquet` has one row per dated listing row. Join it to meetings on
(`meeting_url` = `source_url`, `row_ordinal`), one-to-one within a snapshot. Then
join `feedback_url` to the feedback table's `source_url`, many-to-one, retaining
unmatched rows. Inspect `date_match`, `type_match` and
`same_report_for_multiple_rows` before assigning attendance to individual meetings.
Neither repeated observations nor repeated report links are silently collapsed.

The 2018 feedback URL omits the meeting date; later archives and the current
portal use different URL parameters. The report's own date is always retained and
compared with the listing. Date comparison accepts equivalent display padding,
but invalid or missing dates remain unmatched. Type comparison is limited to
verified Sabha/Meeting labels. HTTP-200 error pages fail validation and never
become zero-attendance observations.
