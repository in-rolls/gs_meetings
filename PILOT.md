# Ambala pilot

Captured on 2026-09-11 from the Ministry of Panchayati Raj's public reports.
Scope: Haryana (state code 6), Ambala district panchayat (code 58), all six
block-panchayat GP reports. This is a geographically restricted validation sample.

| Edition | GP rows | Reported attendance | Feedback submissions | GP requests |
|---|---:|---:|---:|---:|
| current | 400 | 111,320 | 1,574 | 6 |
| PPC | 400 | 13,610 | 400 | 6 |
| PPC2020 | 395 | 19,724 | 399 | 6 |
| PPC2019 | 397 | 12,360 | 789 | 6 |
| PPC2018 | 408 | 17,308 | 408 | 6 |

Total: 2,000 GP-by-edition rows, not 2,000 distinct panchayats. All 30 GP requests
succeeded. Within each edition GP codes are unique across the selected blocks.
For all 30 parent reports, the sums of all 12 attendance/feedback/discussion
metrics exactly match the saved parent block totals. `totalGp` is excluded from
that comparison because it is not populated as a GP count at these levels.
No GP response was empty. These checks validate extraction for this scope, not
historical completeness or the accuracy of the agency's attendance reports.

Five source rows have subgroup attendance above total attendance: one in `PPC`
and four in `PPC2018`. These are preserved and flagged in `manifest.json`. Five
`PPC2019` rows report zero total attendance. All other editions have no zero-total
rows in this pilot. Source excerpts for the five inconsistencies are retained in
[historical_anomalies.json](tests/fixtures/historical_anomalies.json).

The report service returned 36 state/UT rows in each edition. That establishes
the national summary's available rows, not a complete nationwide GP frame.
The five editions required 45 requests: three hierarchy requests and six GP
requests per edition. Reconnaissance measured about 1.2 seconds for a new
connection and 0.4–0.8 seconds for later requests. The pilot therefore uses one
sequential session. National request counts and runtime remain unmeasured; they
must be enumerated before a national collection is scheduled.

## Two source rows

Both rows come from the [current Ambala-I GP report](https://gpdp.nic.in/gpSummaryAnalysisReport.html?stateId=6&code=1030),
also reached through [the official report page](https://gpdp.nic.in/summaryAnalysisReport.html)
by selecting Haryana, Ambala and Ambala-I. The JSON objects below retain every
source field and value; only whitespace has been expanded. The complete response
is saved in [tests/fixtures/gp.json](tests/fixtures/gp.json), with capture provenance
in [SOURCES.md](tests/fixtures/SOURCES.md).

```json
{
  "code": 27783,
  "name": "ADHO MAJRA",
  "level": "V",
  "totalGp": 0,
  "feedbackSubmitted": 4,
  "peoplePresent": 578,
  "scPresent": 41,
  "stPresent": 10,
  "shgPresent": 12,
  "shgPresentation": 0,
  "womenPresent": 164,
  "missionAntyodayaPresentation": 2,
  "gpdpFundUtilized": 4,
  "discussionOnResource": 4,
  "discussionOnGaps": 3,
  "resolutionPassedRecorded": 4,
  "tlb": false
}
```

```json
{
  "code": 27784,
  "name": "AHEMA",
  "level": "V",
  "totalGp": 0,
  "feedbackSubmitted": 4,
  "peoplePresent": 141,
  "scPresent": 43,
  "stPresent": 10,
  "shgPresent": 19,
  "shgPresentation": 0,
  "womenPresent": 35,
  "missionAntyodayaPresentation": 3,
  "gpdpFundUtilized": 3,
  "discussionOnResource": 4,
  "discussionOnGaps": 3,
  "resolutionPassedRecorded": 4,
  "tlb": false
}
```

The parser retains `feedbackSubmitted` as `feedback_submitted`, `peoplePresent`
as `people_present`, and all other counts without recoding their values. Raw
responses and Parquet remain in the local `data/pilot/<edition>/` directories.
No national collection or data publication has been performed.
