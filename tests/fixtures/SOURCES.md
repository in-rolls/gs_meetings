# Fixture sources
Verbatim public aggregate report responses, captured for parser and hierarchy tests. No individual attendee records. Times below are UTC capture-file completion times.
| File | Source | Captured |
|---|---|---|
| `current.json` | https://gpdp.nic.in/stateSummaryAnalysisReport.html | 2026-09-11T19:10:01.669976+00:00 |
| `district.json` | https://gpdp.nic.in/districtSummaryAnalysisReport.html?stateId=6 | 2026-09-11T19:10:35.128815+00:00 |
| `block.json` | https://gpdp.nic.in/blockSummaryAnalysisReport.html?stateId=6&zpCode=58 | 2026-09-11T19:10:35.734931+00:00 |
| `gp.json` | https://gpdp.nic.in/gpSummaryAnalysisReport.html?stateId=6&code=1030 | 2026-09-11T19:10:36.535771+00:00 |
| `PPC.json` | https://gpdp.nic.in/PPC/stateSummaryAnalysisReport.html | 2026-09-11T19:10:02.129878+00:00 |
| `PPC2020.json` | https://gpdp.nic.in/PPC2020/stateSummaryAnalysisReport.html | 2026-09-11T19:10:02.574267+00:00 |
| `PPC2019.json` | https://gpdp.nic.in/PPC2019/stateSummaryAnalysisReport.html | 2026-09-11T19:10:03.038815+00:00 |
| `PPC2018.json` | https://gpdp.nic.in/PPC2018/stateSummaryAnalysisReport.html | 2026-09-11T19:10:03.464905+00:00 |

`historical_anomalies.json` contains five GP-row excerpts from the live Ambala pilot. Each entry includes its exact request URL and UTC capture timestamp; source row fields and values are unchanged. Whitespace is expanded.

## Dated meeting reports

- `annual_gp.json`: https://gpdp.nic.in/gramSabhaHeldDetails.html?stateId=6&code=1030&level=G&finYear=2024-2025; captured 2026-09-11T22:15:07.917742+00:00. First two source rows.
- `annual_states.json`: https://gpdp.nic.in/stateGSHeldReport.html?finYear=2024-2025; captured 2026-09-11T22:15:09.681622+00:00. First two source rows.
- `annual_state_6.json`: https://gpdp.nic.in/gramSabhaHeldDetails.html?stateId=6&code=0&level=G&finYear=2024-2025; captured 2026-09-11T22:15:59.094596+00:00. First two source rows.
- `annual_state_17.json`: https://gpdp.nic.in/gramSabhaHeldDetails.html?stateId=17&code=0&level=G&finYear=2024-2025; captured 2026-09-11T22:15:58.426833+00:00. First two source rows.
- `annual_district_58.json`: https://gpdp.nic.in/gramSabhaHeldDetails.html?stateId=6&code=58&level=G&finYear=2024-2025; captured 2026-09-11T22:16:00.215695+00:00. First two source rows.
- `archive_2018_states.json`: https://gpdp.nic.in/PPC2018/stateGSHeldReport.html; captured 2026-09-11 (exact timestamp not retained). First two source rows.
- `archive_ppc_gp.json`: https://gpdp.nic.in/PPC/gsHeldDetailsReport.html?stateId=6&code=1030&meetingType=S; captured 2026-09-11 (exact timestamp not retained). First two source rows.
- `feedback_detail_current.html`: https://gpdp.nic.in/facilitatorFeedbackDetails.html?lbCode=27783&stateCode=6&date=22-11-2024; captured 2026-09-11T22:29:25.604637+00:00. Form excerpt with session-bearing form action removed, or original error page.
- `feedback_ppc.html`: https://gpdp.nic.in/PPC/facilitatorFeedbackDetails.html?gpCode=27783&dpName=AMBALA&bpName=AMBALA-I&gpName=ADHO+MAJRA&date=01-12-2021&meetingType=S; captured 2026-09-11T22:35:07.342134+00:00. Form excerpt with session-bearing form action removed, or original error page.
- `feedback_absent.html`: https://gpdp.nic.in/facilitatorFeedbackDetails.html?lbCode=27783&stateCode=6&date=23-11-2024; captured 2026-09-11T22:35:06.978060+00:00. Form excerpt with session-bearing form action removed, or original error page.
- `feedback_2018.html`: https://gpdp.nic.in/PPC2018/facilitatorFeedbackDetails.html?gpCode=27783&dpName=AMBALA&bpName=AMBALA-I&gpName=ADHO+MAJRA; captured 2026-09-11T22:38:38.812275+00:00. Form excerpt with session-bearing action removed.
