# Gram Sabha participation reports

[![CI](https://github.com/in-rolls/gs_meetings/actions/workflows/ci.yml/badge.svg)](https://github.com/in-rolls/gs_meetings/actions/workflows/ci.yml)

Collect and standardize the Ministry of Panchayati Raj's reports on Gram Sabhas
held under the People's Plan Campaign. The reports summarize participation and
discussions at Gram Panchayat (GP) and traditional local body (TLB) level. They
can support research on local participation and planning, subject to the reporting
and coverage limits below.

## Data

The collector covers the current report and four archived editions. National
collection is not yet complete, and no Dataverse deposit has been published.
Output stays under `data/`.

| File | Contents |
|---|---|
| `collection.sqlite` | Resumable queue of national summary requests and their geographic context |
| `raw/<edition>/*.jsonl.gz` | Original JSON response text, URL, UTC timestamp and success/failure status |
| `tables/gp_reports.parquet` | One row per GP/TLB returned by each parent report and edition |
| `tables/report_editions.parquet` | Campaign dates and target plan years stated on each edition's homepage |
| `tables/request_coverage.parquet` | Every discovered request, including successful empty responses |
| `tables/hierarchy_summaries.parquet` | Original state, district and block summary rows with geographic context |
| `tables/column_profiles.parquet` | Missingness, zeros and ranges by edition, state and count field |
| `tables/reconciliation.parquet` | GP sums compared with the immediate parent's reported totals |
| `tables/attendance_issues.json`, `tables/ambiguous_gp_keys.json` | Attendance inconsistencies and GP codes appearing under multiple parents |
| `tables/manifest.json`, `tables/SCHEMA.json`, `tables/CHECKSUMS` | Coverage, row counts, output types and SHA-256 checksums |

See [SCHEMA.md](SCHEMA.md) for units, keys and join rules, and
[PILOT.md](PILOT.md) for the verified Ambala sample and source comparison.

## Columns

| Columns | Meaning |
|---|---|
| `edition` | Source path: `current`, `PPC`, `PPC2020`, `PPC2019` or `PPC2018`; not an inferred observation year |
| `state_code`, `state_name` | State/UT identifier and name supplied by the portal |
| `district_code`, `district_name` | District panchayat identifier/name when present in the route |
| `block_code`, `block_name` | Intermediate/block panchayat identifier/name when present |
| `parent_level`, `parent_code` | Immediate parent used to request the GP report |
| `gp_code`, `gp_name` | GP/TLB identifier and name |
| `feedback_submitted` | Source `feedbackSubmitted`; its label suggests GPs, but values can exceed the number of GPs |
| `people_present`, `sc_present`, `st_present`, `women_present`, `shg_present` | Reported attendance totals; SHG means self-help group |
| `shg_presentation`, `mission_antyodaya_presentation` | Source counts for SHG and Mission Antyodaya presentations |
| `funds_utilized_discussion` | Source `gpdpFundUtilized`: discussion of funds utilized, **not a monetary amount** |
| `resource_discussion`, `gaps_discussion`, `resolution_passed_recorded` | Source counts for resource/gap discussions and resolutions |
| `total_gp_reported` | Source `totalGp`, populated at state level but often zero below it; not a reliable GP-row denominator |
| `source_level`, `source_tlb` | Unmodified portal hierarchy/category fields |
| `source_url`, `fetched_at` | Request URL and UTC capture timestamp |
| `raw_row` | Original row fields serialized as JSON for reprocessing |
| `observation_id` | SHA-256 of the report URL and GP code; identifies a row within a snapshot |

Codes are stored as strings and counts as nullable 64-bit integers. Missing values
remain null, separately from zero. The record key uses `edition`, `source_url`
and `gp_code`. Codes have not yet been validated against a dated LGD release;
district panchayats should not automatically be treated as administrative districts.

## Time coverage

Campaign dates, target plan years and download timestamps describe different things.
The summary responses have no meeting-date field or year parameter. The homepage
labels below are retained as metadata; they do not establish the period over which
the reported counts accumulated.

| Edition | Campaign window on homepage | Target plan year |
|---|---|---|
| `PPC2018` | 2018-10-02 to 2018-12-31 | 2019–2020 |
| `PPC2019` | 2020-05-01 to 2020-06-15 | 2020–2021 |
| `PPC2020` | 2020-10-02 to 2021-01-31 | 2021–2022 |
| `PPC` | 2021-10-02 to 2022-01-31 | 2022–2023 |
| `current` | Unverified | 2026–2027 |

These labels were checked on 2026-09-11. In particular, the archive called
`PPC2019` describes a campaign in 2020. Do not infer calendar year from its name.

Separate [Gram Sabhas held](https://gpdp.nic.in/gramSabhasHeldReport.html) and
[facilitator feedback](https://gpdp.nic.in/facilitatorFeedbackReport.html) pages
offer financial years 2022–2023 through 2025–2026. Their page templates include
meeting dates. Those responses still need verification and collection; they are
not included in the summary tables. Actual meeting dates will be stored separately
from the requested financial year and the UTC download timestamp.

## Coverage and interpretation

- Counts are administrative reports entered by field agencies. They do not establish
  independently observed attendance, unique participants or a census of meetings.
- Historical pilot responses include five rows where a subgroup exceeds total
  attendance. `manifest.json` identifies each in `attendance_issues`; values are
  preserved, and parsing logs a warning. These source issues do not change the exit code.
- Attendance categories overlap: a participant may be a woman, SC and an SHG member.
  Do not add these columns to estimate total attendance.
- The current homepage names plan year 2026–2027, while the summary page contains
  older dates. Neither establishes the observation period of its live counts.
- `feedbackSubmitted` is greater than one for some individual GPs. We preserve
  it without relabelling it as either distinct GPs or a verified meeting count.
- A zero report can reflect no submitted feedback; it is not proof that no meeting
  occurred. Empty arrays are saved but reported as coverage requiring investigation.
- Parent and child reports are fetched at different times. Their differences are
  reported explicitly; matching totals alone do not establish full GP coverage.
- The portal omits state codes 4 and 7 from its displayed state table. Raw state
  responses retain them; national traversal follows the displayed hierarchy.

## How collected

The official page's [service JavaScript](https://gpdp.nic.in/resources/js/report/analysis/summaryAnalysis-service.js)
defines four GET requests: state, district, block and GP summaries. The collector
uses those same public requests. No login is required for these aggregate reports.

| Edition | Official page | Verification |
|---|---|---|
| `current` | [Current report](https://gpdp.nic.in/summaryAnalysisReport.html) | Populated state and Haryana GP responses |
| `PPC` | [PPC archive](https://gpdp.nic.in/PPC/summaryAnalysisReport.html) | Populated state and Ambala GP responses; historically linked as 2021–2022 |
| `PPC2020` | [2020 archive](https://gpdp.nic.in/PPC2020/summaryAnalysisReport.html) | Populated state and Ambala GP responses |
| `PPC2019` | [2019 archive](https://gpdp.nic.in/PPC2019/summaryAnalysisReport.html) | Populated state and Ambala GP responses |
| `PPC2018` | [2018 archive](https://gpdp.nic.in/PPC2018/summaryAnalysisReport.html) | Populated state and Ambala GP responses |

### Handling

The hierarchy follows the portal controller: Nagaland, Mizoram and Meghalaya go
directly from state to GP/TLB; Puducherry goes from state to block; district rows
with level `V` go directly to GP rather than through a block. Other districts use
level `I` and a block listing. Unknown levels stop enumeration instead of skipping
geographies. Archive paths use the same service request shapes; geographic coverage
must still be checked separately for each edition.

Each successful response is saved atomically and reused on resume. Failed or damaged
captures are retried; HTTP errors and HTML error pages never become empty data.
National collection uses 16 workers, with one session per thread and edition.
Eight retries are the national default, with exponential backoff and server
`Retry-After` handling for throttling and temporary server errors.
Use `--retries 20` for a collection that should wait through a longer outage.
TLS verification remains enabled. Use a new output directory for a fresh snapshot:
resume deliberately retains the first successful response for each URL.

## Usage

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```sh
uv sync --frozen --group dev
uv run gs-meetings collect --root data/national
uv run gs-meetings export --root data/national
```

`collect` traverses all five editions and checkpoints each completed request in
SQLite. Repeat it to retry failures or resume an interrupted run. Read
`collection-progress.json` for counts and unresolved errors. `--workers` controls
concurrency; `--max-requests` limits new requests in an invocation. Incomplete
queues produce a nonzero exit code. `export` reads saved responses without network
access and refuses to export a queue with pending or failed requests. A completed
queue can still contain empty source responses; those remain explicit coverage gaps.

For a selected state or district:

```sh
uv run gs-meetings list --state 6 --district 58 --root data/ambala-current
uv run gs-meetings fetch --root data/ambala-current
uv run gs-meetings parse --root data/ambala-current
```

This enumerates Haryana's Ambala district panchayat, fetches its GP reports and
creates `frame.parquet` and `records.parquet`, with separate frame/fetch manifests.
Omit `--district` to enumerate the selected state. `fetch --limit 2`
limits the invocation to the first two frame units, including already cached units.
Run without that limit to finish the frame. Each stage is safe to repeat in the same
directory. A failed request makes `fetch` exit nonzero; incomplete or empty units
make `parse` exit nonzero while retaining any valid partial output and its manifest.

For an archive, pass the same `--edition` to both network stages and use a separate
directory, for example `--edition PPC2018 --root data/ambala-2018`. The parser reads
only saved files and does not access the network.

```sh
make ci
make ci-docker
uv run pre-commit run --all-files
```

## Citation

Cite the Ministry of Panchayati Raj's report URL, edition and capture date for the
data, and [CITATION.cff](CITATION.cff) for this software. There is no dataset DOI.
The original source notes remain available at commit
[`3d95a90`](https://github.com/in-rolls/gs_meetings/tree/3d95a90).

## License

The code is available under the [MIT License](LICENSE). Government responses retain
their source attribution; the code license does not assign a license to those records.
