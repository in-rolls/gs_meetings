"""Report contracts taken from the portal's own Angular service and controller."""

from urllib.parse import urlencode

EDITIONS = {
    "current": "",
    "PPC": "PPC/",
    "PPC2020": "PPC2020/",
    "PPC2019": "PPC2019/",
    "PPC2018": "PPC2018/",
}
METRICS = {
    "totalGp": "total_gp_reported",
    "feedbackSubmitted": "feedback_submitted",
    "peoplePresent": "people_present",
    "scPresent": "sc_present",
    "stPresent": "st_present",
    "womenPresent": "women_present",
    "shgPresent": "shg_present",
    "shgPresentation": "shg_presentation",
    "missionAntyodayaPresentation": "mission_antyodaya_presentation",
    "gpdpFundUtilized": "funds_utilized_discussion",
    "discussionOnResource": "resource_discussion",
    "discussionOnGaps": "gaps_discussion",
    "resolutionPassedRecorded": "resolution_passed_recorded",
}


def endpoint(edition: str, level: str, **params: int) -> str:
    """Build only the documented public report URLs."""
    if level not in {"state", "district", "block", "gp"}:
        raise ValueError(f"Unknown report level: {level}")
    url = f"https://gpdp.nic.in/{EDITIONS[edition]}{level}SummaryAnalysisReport.html"
    return url + ("?" + urlencode(params) if params else "")


def validate_rows(value: object) -> list[dict]:
    """Reject error pages, schema drift, duplicate codes and invalid counts."""
    if not isinstance(value, list):
        raise ValueError("Expected a JSON array of report rows")
    codes = set()
    for row in value:
        if not isinstance(row, dict) or not isinstance(row.get("name"), str):
            raise ValueError("Report row must contain a name")
        code = row.get("code")
        if type(code) is not int or code <= 0 or code in codes:
            raise ValueError(f"Invalid or duplicate geographic code: {code}")
        codes.add(code)
        if row.get("level") not in {None, "I", "V"}:
            raise ValueError(f"Unknown hierarchy level: {row.get('level')}")
        if row.get("tlb") is not None and type(row["tlb"]) is not bool:
            raise ValueError("Invalid TLB flag")
        for field in METRICS:
            if field not in row:
                raise ValueError(f"Missing metric: {field}")
            count = row[field]
            if count is not None and (type(count) is not int or count < 0):
                raise ValueError(f"Invalid count in {field}: {count}")
    return value
