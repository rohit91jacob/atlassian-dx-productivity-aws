"""Reconcile pipeline outputs with figures published independently by the projects / platforms.

Checks (configured in ``verification/published_figures.json``). Each check compares a pipeline
output with an independent figure; where the two sources would otherwise describe different
populations, the comparison is scoped until they describe the same one (see docs/verification.md):

* ``release_dates_jira``: release dates in ``marts.fct_releases`` against the live Jira versions
  API. Like-for-like: the pipeline claims to report Jira's release date.
* ``release_dates_published``: the same dates against each project's official download / news
  page, fetched live and parsed. Source agreement: Jira's "version released" date against the
  announcement date, which are different steps of a release, so differences are expected.
* ``release_notes``: the issues the pipeline holds for a Kafka fix version
  (``staging.stg_jira__issue_versions``) against the official ``RELEASE_NOTES.html`` on the ASF
  archive, restricted to the release-note issues the pipeline could see: those whose last Jira
  update lies inside the extracted Jira window and that still carry the fix version.
* ``jira_throughput``: issues resolved per week in ``marts.mart_issue_flow_weekly`` against Jira's
  own server-side count for the same JQL window.
* ``github_merged``: PRs merged per UTC day in ``marts.fct_pull_requests`` against GitHub search's
  ``total_count`` for the same repository and day, for each recent day that lies wholly inside the
  extracted GitHub window (``intermediate.int_github_coverage``).

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
from dx_productivity.http import HttpClient, RateLimited, Request, UrllibTransport

MONTHS = {
    m: i
    for i, m in enumerate(
        ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"],
        start=1,
    )
}
SEMVER = re.compile(r"\d+\.\d+\.\d+")
JIRA_VERSIONS_URL = "https://issues.apache.org/jira/rest/api/2/project/{}/versions"


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


def _semver_key(version: str) -> tuple[int, ...]:
    return tuple(int(x) for x in version.split(".")) if SEMVER.fullmatch(version) else (0,)


def minute_ceil(ts: datetime) -> datetime:
    floor = ts.replace(second=0, microsecond=0)
    return floor if floor == ts else floor + timedelta(minutes=1)


def minute_floor(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)


class Live:
    """The live independent sources. Tests substitute an object with the same methods."""

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
        # GitHub search allows 30 requests/minute with a token and 10 without; pace under both.
        self.search_http = HttpClient(
            UrllibTransport(),
            headers,
            timeout_s=settings.http_timeout_s,
            retries=settings.http_retries,
            min_interval_s=2.5 if settings.github_token else 6.5,
        )

    def page(self, url: str) -> str:
        req = Request("GET", url, (), {"User-Agent": self.settings.user_agent})
        return self.http.send(req).body.decode("utf-8", "replace")

    def jira_total(self, jql: str) -> int:
        body, _, _ = self.http.get_json(
            f"{self.settings.jira_base_url}/rest/api/2/search", {"jql": jql, "maxResults": 0}
        )
        return int(body["total"])

    def jira_keys(self, jql: str) -> set[str]:
        keys: set[str] = set()
        start = 0
        while True:
            body, _, _ = self.http.get_json(
                f"{self.settings.jira_base_url}/rest/api/2/search",
                {"jql": jql, "fields": "updated", "startAt": start, "maxResults": 1000},
            )
            issues = body.get("issues", [])
            keys.update(i["key"] for i in issues)
            start += len(issues)
            if not issues or start >= int(body["total"]):
                return keys

    def jira_versions(self, project: str) -> dict[str, dict[str, Any]]:
        body, _, _ = self.http.get_json(
            f"{self.settings.jira_base_url}/rest/api/2/project/{project}/versions"
        )
        return {v["name"]: v for v in body}

    def github_search_total(self, query: str) -> int:
        headers = {"Accept": "application/vnd.github+json"}
        if self.settings.github_token:
            headers["Authorization"] = f"Bearer {self.settings.github_token}"
        body, _, _ = self.search_http.get_json(
            f"{self.settings.github_api_url}/search/issues", {"q": query, "per_page": 1}, headers
        )
        return int(body["total_count"])


def _rows(con: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> list[tuple]:
    return con.execute(sql, params or []).fetchall()


def _core_release_dates(con, project: str) -> dict[str, date]:
    return dict(
        _rows(
            con,
            "SELECT version_name, release_date FROM marts.fct_releases"
            " WHERE project_key = ? AND release_type <> 'subproject'",
            [project],
        )
    )


def _in_scope(version: str, *dates: date | None, since: date) -> bool:
    known = [d for d in dates if d is not None]
    return bool(SEMVER.fullmatch(version)) and bool(known) and max(known) >= since


def check_release_dates_jira(con, live: Live | None, cfg: dict, report: Report) -> None:
    """Like-for-like: ``fct_releases.release_date`` against the date the Jira API serves now, for
    every core (x.y.z) version released since ``since``."""
    since = date.fromisoformat(cfg.get("since", "2024-01-01"))
    for source in cfg["sources"]:
        project = source["project"]
        url = JIRA_VERSIONS_URL.format(project)
        ours = _core_release_dates(con, project)
        if live is None:
            report.add(
                Outcome("release_dates_jira", project, len(ours), None, "skipped", url, "offline")
            )
            continue
        jira = {
            name: date.fromisoformat(v["releaseDate"])
            for name, v in live.jira_versions(project).items()
            if v.get("released") and v.get("releaseDate")
        }
        for version in sorted(set(jira) | set(ours), key=_semver_key):
            ref, our = jira.get(version), ours.get(version)
            if not _in_scope(version, ref, our, since=since):
                continue
            if our is None:
                status, note = "missing_ours", "released in Jira but not in fct_releases"
            elif ref is None:
                status, note = "missing_published", "in fct_releases but not released in Jira"
            elif our == ref:
                status, note = "match", ""
            else:
                status, note = "mismatch", f"pipeline date is {(our - ref).days:+d} days vs Jira"
            report.add(
                Outcome("release_dates_jira", f"{project} {version}", our, ref, status, url, note)
            )


def check_release_dates_published(con, live: Live | None, cfg: dict, report: Report) -> None:
    """Source agreement: Jira's "version released" date (as reported by the pipeline) against the
    announcement on the project's download / news page. These are different steps of an ASF
    release, so a difference is a fact about the sources, reported as found."""
    since = date.fromisoformat(cfg.get("since", "2024-01-01"))
    for source in cfg["sources"]:
        project, url = source["project"], source["url"]
        ours = _core_release_dates(con, project)
        if live is None:
            report.add(
                Outcome(
                    "release_dates_published", project, len(ours), None, "skipped", url, "offline"
                )
            )
            continue
        published = parse_release_page(source["kind"], live.page(url))
        jira: dict[str, dict[str, Any]] | None = None
        for version in sorted(set(published) | set(ours), key=_semver_key):
            pub, our = published.get(version), ours.get(version)
            if not _in_scope(version, pub, our, since=since):
                continue
            if our is None:
                jira = live.jira_versions(project) if jira is None else jira
                status, in_jira = "missing_ours", jira.get(version)
                if in_jira is None:
                    note = "announced, but Jira has no version of that name"
                elif not in_jira.get("released"):
                    note = "announced, but the Jira version is not marked released (checked live)"
                else:
                    note = "announced and released in Jira, but missing from fct_releases"
            elif pub is None:
                status, note = "missing_published", "released in Jira but not on the page"
            elif our == pub:
                status, note = "match", ""
            else:
                status = "mismatch"
                note = f"Jira release date is {(our - pub).days:+d} days vs the announcement"
            report.add(
                Outcome(
                    "release_dates_published", f"{project} {version}", our, pub, status, url, note
                )
            )


def check_release_notes(con, live: Live | None, cfg: dict, report: Report) -> None:
    """Pipeline fix-version membership against the official release notes, like-for-like.

    The pipeline only holds issues updated since its first Jira watermark. The comparable
    published population is the release-note issues whose *current* Jira ``updated`` lies inside
    the extracted window ``[covered_from, covered_to]`` (an issue last touched inside the window
    was extracted in its current state) and that still carry the fix version. Every one of them
    must be in the pipeline with that fix version; ``ours`` counts how many are.
    """
    for item in cfg["items"]:
        project, version, url = item["project"], item["version"], item["url"]
        subject = f"{project} {version}"
        ours = {
            key
            for (key,) in _rows(
                con,
                "SELECT DISTINCT issue_key FROM staging.stg_jira__issue_versions"
                " WHERE project_key = ? AND relation = 'fix' AND version_name = ?",
                [project, version],
            )
        }
        coverage = _rows(
            con,
            "SELECT covered_from, covered_to FROM intermediate.int_jira_coverage"
            " WHERE project_key = ?",
            [project],
        )
        if live is None or not coverage:
            note = "offline" if live is None else "no Jira data for the project"
            report.add(Outcome("release_notes", subject, len(ours), None, "skipped", url, note))
            continue
        lo, hi = minute_ceil(coverage[0][0]), minute_floor(coverage[0][1])
        notes = parse_release_notes(live.page(url), project)
        in_window = live.jira_keys(
            f'project = "{project}" AND fixVersion = "{version}"'
            f' AND updated >= "{lo:%Y/%m/%d %H:%M}" AND updated < "{hi:%Y/%m/%d %H:%M}"'
        )
        expected = notes & in_window
        found = ours & expected
        note = (
            f"{len(expected)} of the {len(notes)} issues in the notes were last updated inside the"
            f" extracted window ({lo:%Y-%m-%d %H:%M} to {hi:%Y-%m-%d %H:%M} UTC) and still carry"
            f" the fix version; the pipeline also holds {len(ours - notes)} issues with this fix"
            " version that the notes do not list (Jira-only, outside this comparison)"
        )
        if not expected:
            status, note = "skipped", note + "; nothing comparable"
        elif found == expected:
            status = "match"
        else:
            lost = ", ".join(sorted(expected - found)[:10])
            status, note = "mismatch", note + f"; not in the pipeline: {lost}"
        report.add(Outcome("release_notes", subject, len(found), len(expected), status, url, note))


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
        subject = f"{project} week of {week}"
        if live is None:
            report.add(Outcome("jira_throughput", subject, int(ours), None, "skipped", url))
            continue
        published = live.jira_total(jql)
        status = "match" if int(ours) == published else "mismatch"
        note = (
            ""
            if status == "match"
            else "issues resolved/reopened after extraction, or outside the window"
        )
        report.add(Outcome("jira_throughput", subject, int(ours), published, status, url, note))


def check_github_merged(con, live: Live | None, cfg: dict, as_of: datetime, report: Report) -> None:
    """Merged PRs per UTC day for each of the last ``days`` complete days. A day is compared only
    when it lies wholly inside the repository's extracted GitHub window: PRs merged before the
    window are in the data only if something touched them later, so such a day is not
    comparable and is reported as skipped."""
    days = cfg.get("days", 1)
    coverage = {
        repo: (lo, hi)
        for repo, lo, hi in _rows(
            con, "SELECT repo, covered_from, covered_to FROM intermediate.int_github_coverage"
        )
    }
    last_day = as_of.date() - timedelta(days=1)
    for repo in cfg["repos"]:
        for back in range(days):
            day = last_day - timedelta(days=back)
            start = datetime.combine(day, datetime.min.time())
            end = start + timedelta(days=1)
            query = (
                f"repo:{repo} is:pr is:merged"
                f" merged:{day:%Y-%m-%d}T00:00:00Z..{day:%Y-%m-%d}T23:59:59Z"
            )
            url = "https://github.com/search?type=pullrequests&q=" + urllib.parse.quote_plus(query)
            subject = f"{repo} {day}"
            (ours,) = _rows(
                con,
                "SELECT count(*) FROM marts.fct_pull_requests WHERE repo = ?"
                " AND merge_source = 'github_merge' AND merged_at >= ? AND merged_at < ?",
                [repo, start, end],
            )[0]
            window = coverage.get(repo)
            if window is None or start < window[0] or end > window[1]:
                note = "day not wholly inside the extracted GitHub window"
                report.add(Outcome("github_merged", subject, ours, None, "skipped", url, note))
                continue
            if live is None:
                report.add(Outcome("github_merged", subject, ours, None, "skipped", url, "offline"))
                continue
            try:
                published = live.github_search_total(query)
            except RateLimited:
                report.add(
                    Outcome("github_merged", subject, ours, None, "skipped", url, "rate limited")
                )
                continue
            status = "match" if ours == published else "mismatch"
            note = "" if status == "match" else "PRs merged after the extraction ran, or not listed"
            report.add(Outcome("github_merged", subject, ours, published, status, url, note))


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


def run_checks(
    con: duckdb.DuckDBPyConnection, live: Live | None, cfg: dict, as_of: datetime
) -> Report:
    report = Report()
    check_release_dates_jira(con, live, cfg["release_dates"], report)
    check_release_dates_published(con, live, cfg["release_dates"], report)
    check_release_notes(con, live, cfg["release_notes"], report)
    check_jira_throughput(con, live, cfg["jira_throughput"], report)
    check_github_merged(con, live, cfg["github_merged"], as_of, report)
    return report


def summarise(report: Report) -> dict[str, Any]:
    statuses = [o.status for o in report.outcomes]
    summary: dict[str, Any] = {s: statuses.count(s) for s in dict.fromkeys(statuses)}
    summary["mismatches"] = statuses.count("mismatch")
    by_check: dict[str, dict[str, int]] = {}
    for o in report.outcomes:
        counts = by_check.setdefault(o.check, {})
        counts[o.status] = counts.get(o.status, 0) + 1
    summary["by_check"] = by_check
    return summary


def run_verification(settings: Settings, figures: Path, *, offline: bool = False) -> dict[str, Any]:
    from dx_productivity.extract.runner import effective_settings

    settings = effective_settings(settings)
    cfg = json.loads(Path(figures).read_text(encoding="utf-8"))
    live = None if offline else Live(settings)
    con = duckdb.connect(str(settings.warehouse_db))
    try:
        report = run_checks(con, live, cfg, settings.now())
    finally:
        con.close()
    return {
        "summary": summarise(report),
        "outcomes": [o.__dict__ for o in report.outcomes],
        "markdown": to_markdown(report, settings.now().isoformat()),
    }
