"""Reconcile pipeline outputs with figures published independently by the projects / platforms.

Checks (configured in ``verification/published_figures.json``):

* ``release_dates``: release dates in ``marts.fct_releases`` (from Jira versions) against each
  project's official download / news page, fetched live and parsed.
* ``release_notes``: the number of issues listed in a Kafka release's official
  ``RELEASE_NOTES.html`` (ASF archive) against the number of issues Jira reports for that fix
  version (live JQL total, the same API the extractor uses).
* ``jira_throughput``: issues resolved per week in ``marts.mart_issue_flow_weekly`` against Jira's
  own server-side count for the same JQL window.
* ``github_merged``: PRs merged per UTC day in ``marts.fct_pull_requests`` against GitHub search's
  ``total_count`` for the same repository and window.

Mismatches are reported, not hidden: a mismatch is often a real discrepancy between sources
(for example a Jira release date that differs from the announcement date).
"""

from __future__ import annotations

import html
import json
import re
import urllib.parse
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

from dx_productivity.config import Settings
from dx_productivity.http import HttpClient, UrllibTransport

MONTHS = {
    m: i
    for i, m in enumerate(
        ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"],
        start=1,
    )
}


@dataclass
class Outcome:
    check: str
    subject: str
    ours: Any
    published: Any
    status: str  # match | mismatch | missing_ours | missing_published | skipped
    source: str
    note: str = ""


@dataclass
class Report:
    outcomes: list[Outcome] = field(default_factory=list)

    def add(self, *items: Outcome) -> None:
        self.outcomes.extend(items)


def _text(page: str) -> str:
    return html.unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", page)))


def _parse_month_date(text: str) -> date:
    month, day, year = re.match(r"([A-Za-z]+)\.? (\d+), (\d{4})", text).groups()
    return date(int(year), MONTHS[month[:3].lower()], int(day))


def parse_release_page(kind: str, page: str) -> dict[str, date]:
    """Version -> release date from an official page (formats as of October 2026)."""
    text = _text(page)
    out: dict[str, date] = {}
    if kind == "kafka_downloads":  # "4.1.0 Released September 2, 2025"
        pattern = r"(?<![\d.])(\d+\.\d+\.\d+) Released ([A-Za-z]+\.? \d+, \d{4})"
        for m in re.finditer(pattern, text):
            out.setdefault(m.group(1), _parse_month_date(m.group(2)))
    elif kind == "spark_news":  # "Spark 4.0.1 released September 6, 2025"
        for m in re.finditer(r"Spark (\d+\.\d+\.\d+) released\s+([A-Z][a-z]+ \d+, \d{4})", text):
            out.setdefault(m.group(1), _parse_month_date(m.group(2)))
    elif kind == "flink_downloads":  # "Flink 2.1.0 - 2025-07-31"
        for m in re.finditer(r"Flink (\d+\.\d+\.\d+) - (\d{4}-\d{2}-\d{2})", text):
            out.setdefault(m.group(1), date.fromisoformat(m.group(2)))
    else:
        raise ValueError(f"unknown release page kind {kind!r}")
    return out


def parse_release_notes(page: str, project: str) -> set[str]:
    return set(re.findall(rf"\b{project}-\d+\b", page))


class Live:
    def __init__(self, settings: Settings):
        self.settings = settings
        headers = {"User-Agent": settings.user_agent, "Accept": "application/json"}
        self.http = HttpClient(
            UrllibTransport(),
            headers,
            timeout_s=settings.http_timeout_s,
            retries=settings.http_retries,
            min_interval_s=1.0,
        )

    def page(self, url: str) -> str:
        from dx_productivity.http import Request

        req = Request("GET", url, (), {"User-Agent": self.settings.user_agent})
        return self.http.send(req).body.decode("utf-8", "replace")

    def jira_total(self, jql: str) -> int:
        body, _, _ = self.http.get_json(
            f"{self.settings.jira_base_url}/rest/api/2/search", {"jql": jql, "maxResults": 0}
        )
        return int(body["total"])

    def github_search_total(self, query: str) -> int:
        headers = {"Accept": "application/vnd.github+json"}
        if self.settings.github_token:
            headers["Authorization"] = f"Bearer {self.settings.github_token}"
        body, _, _ = self.http.get_json(
            f"{self.settings.github_api_url}/search/issues", {"q": query, "per_page": 1}, headers
        )
        return int(body["total_count"])


def _rows(con: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> list[tuple]:
    return con.execute(sql, params or []).fetchall()


def check_release_dates(con, live: Live | None, cfg: dict, report: Report) -> None:
    since = date.fromisoformat(cfg.get("since", "2024-01-01"))
    for source in cfg["sources"]:
        project, url = source["project"], source["url"]
        ours = dict(
            _rows(
                con,
                "SELECT version_name, release_date FROM marts.fct_releases"
                " WHERE project_key = ? AND release_type <> 'subproject'",
                [project],
            )
        )
        if live is None:
            report.add(
                Outcome("release_dates", project, len(ours), None, "skipped", url, "offline")
            )
            continue
        published = parse_release_page(source["kind"], live.page(url))
        for version in sorted(
            set(published) | set(ours),
            key=lambda v: (
                tuple(int(x) for x in v.split(".")) if re.fullmatch(r"\d+\.\d+\.\d+", v) else (0,)
            ),
        ):
            pub, our = published.get(version), ours.get(version)
            if (
                (pub or our) is None
                or (pub or our) < since
                or not re.fullmatch(r"\d+\.\d+\.\d+", version)
            ):
                continue
            if our is None:
                status, note = "missing_ours", "published but not a released Jira version"
            elif pub is None:
                status, note = "missing_published", "released in Jira but not on the page"
            elif our == pub:
                status, note = "match", ""
            else:
                status, note = "mismatch", f"Jira date is {(our - pub).days:+d} days vs published"
            report.add(
                Outcome("release_dates", f"{project} {version}", our, pub, status, url, note)
            )


def check_release_notes(con, live: Live | None, cfg: dict, report: Report) -> None:
    for item in cfg["items"]:
        project, version, url = item["project"], item["version"], item["url"]
        subject = f"{project} {version}"
        if live is None:
            report.add(Outcome("release_notes", subject, None, None, "skipped", url, "offline"))
            continue
        published = len(parse_release_notes(live.page(url), project))
        ours = live.jira_total(f'project = "{project}" AND fixVersion = "{version}"')
        status = "match" if ours == published else "mismatch"
        note = (
            "" if status == "match" else f"Jira now lists {ours - published:+d} issues vs the notes"
        )
        report.add(Outcome("release_notes", subject, ours, published, status, url, note))


def check_jira_throughput(con, live: Live | None, cfg: dict, report: Report) -> None:
    weeks = cfg.get("weeks", 2)
    rows = _rows(
        con,
        "SELECT project_key, week, throughput FROM ("
        " SELECT *, row_number() OVER (PARTITION BY project_key ORDER BY week DESC) AS rn"
        " FROM marts.mart_issue_flow_weekly WHERE NOT is_partial_week) WHERE rn <= ? ORDER BY 1, 2",
        [weeks],
    )
    for project, week, ours in rows:
        end = week + timedelta(days=7)
        jql = (
            f'project = "{project}" AND statusCategory = Done AND resolved >= "{week:%Y/%m/%d}"'
            f' AND resolved < "{end:%Y/%m/%d}"'
        )
        url = f"{cfg.get('jira_url', 'https://issues.apache.org/jira')}/issues/?jql={urllib.parse.quote(jql)}"
        if live is None:
            report.add(
                Outcome(
                    "jira_throughput", f"{project} week of {week}", int(ours), None, "skipped", url
                )
            )
            continue
        published = live.jira_total(jql)
        status = "match" if int(ours) == published else "mismatch"
        note = (
            ""
            if status == "match"
            else "issues resolved/reopened after extraction, or outside the window"
        )
        report.add(
            Outcome(
                "jira_throughput",
                f"{project} week of {week}",
                int(ours),
                published,
                status,
                url,
                note,
            )
        )


def check_github_merged(con, live: Live | None, cfg: dict, as_of: datetime, report: Report) -> None:
    day = as_of.date() - timedelta(days=cfg.get("days_back", 1))
    start, end = (
        datetime.combine(day, datetime.min.time()),
        datetime.combine(day + timedelta(days=1), datetime.min.time()),
    )
    for repo in cfg["repos"]:
        (ours,) = _rows(
            con,
            "SELECT count(*) FROM marts.fct_pull_requests WHERE repo = ? AND merge_source = 'github_merge'"
            " AND merged_at >= ? AND merged_at < ?",
            [repo, start, end],
        )[0]
        query = (
            f"repo:{repo} is:pr is:merged merged:{day:%Y-%m-%d}T00:00:00Z..{day:%Y-%m-%d}T23:59:59Z"
        )
        url = "https://github.com/search?type=pullrequests&q=" + urllib.parse.quote_plus(query)
        if live is None:
            report.add(Outcome("github_merged", f"{repo} {day}", ours, None, "skipped", url))
            continue
        published = live.github_search_total(query)
        status = "match" if ours == published else "mismatch"
        report.add(
            Outcome(
                "github_merged",
                f"{repo} {day}",
                ours,
                published,
                status,
                url,
                "" if status == "match" else "PRs merged after the extraction ran, or not listed",
            )
        )


def to_markdown(report: Report, as_of: str) -> str:
    lines = [f"Run evaluated as of {as_of}.", ""]
    by_check: dict[str, list[Outcome]] = {}
    for o in report.outcomes:
        by_check.setdefault(o.check, []).append(o)
    for check, items in by_check.items():
        counts = {
            s: sum(1 for o in items if o.status == s)
            for s in dict.fromkeys(o.status for o in items)
        }
        lines += [
            f"### {check}",
            "",
            ", ".join(f"{k}: {v}" for k, v in counts.items()),
            "",
            "| Subject | Ours | Published | Status | Note | Source |",
            "|---|---|---|---|---|---|",
        ]
        for o in items:
            lines.append(
                f"| {o.subject} | {o.ours} | {o.published} | {o.status} | {o.note} | [link]({o.source}) |"
            )
        lines.append("")
    return "\n".join(lines)


def run_verification(settings: Settings, figures: Path, *, offline: bool = False) -> dict[str, Any]:
    from dx_productivity.extract.runner import effective_settings

    settings = effective_settings(settings)
    cfg = json.loads(Path(figures).read_text(encoding="utf-8"))
    live = None if offline else Live(settings)
    report = Report()
    con = duckdb.connect(str(settings.warehouse_db))
    try:
        check_release_dates(con, live, cfg["release_dates"], report)
        check_release_notes(con, live, cfg["release_notes"], report)
        check_jira_throughput(con, live, cfg["jira_throughput"], report)
        check_github_merged(con, live, cfg["github_merged"], settings.now(), report)
    finally:
        con.close()
    statuses = [o.status for o in report.outcomes]
    summary = {s: statuses.count(s) for s in dict.fromkeys(statuses)}
    summary["mismatches"] = statuses.count("mismatch")
    return {
        "summary": summary,
        "outcomes": [o.__dict__ for o in report.outcomes],
        "markdown": to_markdown(report, settings.now().isoformat()),
    }
