"""Incremental extractor for the Apache Software Foundation's public Jira (no authentication).

* Issues: JQL ``updated >= <watermark - overlap>`` ordered by ``updated ASC, key ASC``, with the
  changelog expanded (status history drives cycle time and WIP). Pagination is keyset-style on the
  ``updated`` minute rather than a growing ``startAt``: an issue edited while we page through
  results moves to the end of the ordering, and plain offset paging would silently skip the issue
  that slides into its place. Re-reading a boundary minute only produces duplicates, which the
  curate step resolves.
* Versions: a full snapshot of each project's versions (name, released flag, release date) per
  run. These are the projects' releases and drive deployment frequency.

The watermark is checkpointed after every landed page, so a timeout or crash resumes where it
stopped. Requests are paced (``DX_JIRA_MIN_INTERVAL_SECONDS``) to stay polite to ASF infra.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from dx_productivity.config import Settings
from dx_productivity.extract.raw import RawWriter, page_record
from dx_productivity.extract.watermark import Watermark, parse_ts
from dx_productivity.http import HttpClient
from dx_productivity.logs import get_logger
from dx_productivity.storage.state import StateStore, watermark_key

log = get_logger(__name__)

ISSUE_FIELDS = ",".join(
    [
        "summary",
        "issuetype",
        "status",
        "priority",
        "resolution",
        "created",
        "updated",
        "resolutiondate",
        "fixVersions",
        "versions",
        "components",
        "labels",
        "assignee",
        "reporter",
        "project",
    ]
)


def jql_timestamp(ts: datetime) -> str:
    """JQL accepts minute precision ``yyyy/MM/dd HH:mm`` (interpreted in the server time zone, UTC)."""
    return ts.astimezone(UTC).strftime("%Y/%m/%d %H:%M")


def issues_jql(project: str, lower: datetime) -> str:
    return f'project = "{project}" AND updated >= "{jql_timestamp(lower)}" ORDER BY updated ASC, key ASC'


@dataclass
class JiraStats:
    project: str
    pages: int = 0
    issues: int = 0
    duplicates: int = 0
    versions: int = 0
    watermark_from: str | None = None
    watermark_to: str | None = None
    stopped: str = "complete"
    raw_keys: list[str] = field(default_factory=list)


class JiraExtractor:
    def __init__(
        self,
        settings: Settings,
        client: HttpClient,
        raw: RawWriter,
        state: StateStore,
        clock: Callable[[], datetime],
        should_stop: Callable[[], bool] = lambda: False,
    ):
        self.settings = settings
        self.client = client
        self.raw = raw
        self.state = state
        self.clock = clock
        self.should_stop = should_stop
        self.api = f"{settings.jira_base_url}/rest/api/2"

    def run(self, projects: tuple[str, ...] | None = None) -> list[JiraStats]:
        results = []
        for project in projects or self.settings.jira_projects:
            stats = JiraStats(project)
            self.extract_versions(project, stats)
            self.extract_issues(project, stats)
            results.append(stats)
            log.info(
                "jira project extracted",
                extra={k: v for k, v in vars(stats).items() if k != "raw_keys"},
            )
        return results

    # ------------------------------------------------------------------ versions (releases)
    def extract_versions(self, project: str, stats: JiraStats) -> None:
        body, response, request = self.client.get_json(f"{self.api}/project/{project}/versions")
        if not isinstance(body, list):
            raise ValueError(f"unexpected versions payload for {project}: {type(body).__name__}")
        key = self.raw.land(
            "jira",
            "versions",
            project,
            [page_record(request, response, body, self.clock())],
            meta={"project": project, "snapshot": True},
        )
        stats.versions = len(body)
        stats.raw_keys.append(key)

    # ------------------------------------------------------------------ issues
    def extract_issues(self, project: str, stats: JiraStats) -> None:
        settings = self.settings
        default_start = self.clock() - timedelta(days=settings.jira_lookback_days)
        wm = Watermark(self.state, watermark_key("jira", project, "issues"), default_start)
        wm.load()
        lower = wm.start_with_overlap(timedelta(minutes=settings.overlap_minutes))
        stats.watermark_from = wm.current.isoformat()
        seen: set[tuple[str, str]] = set()
        start_at = 0

        while stats.pages < settings.jira_max_pages:
            if self.should_stop():
                stats.stopped = "deadline"
                break
            jql = issues_jql(project, lower)
            body, response, request = self.client.get_json(
                f"{self.api}/search",
                {
                    "jql": jql,
                    "startAt": start_at,
                    "maxResults": settings.jira_page_size,
                    "fields": ISSUE_FIELDS,
                    "expand": "changelog",
                },
            )
            issues: list[dict[str, Any]] = body.get("issues", [])
            total = int(body.get("total", 0))
            fresh = [i for i in issues if (i["key"], i["fields"]["updated"]) not in seen]
            stats.duplicates += len(issues) - len(fresh)
            if fresh:
                stats.pages += 1
                key = self.raw.land(
                    "jira",
                    "issues",
                    project,
                    [page_record(request, response, body, self.clock(), jql=jql)],
                    meta={
                        "project": project,
                        "lower_bound": lower.isoformat(),
                        "start_at": start_at,
                    },
                )
                stats.raw_keys.append(key)
                seen.update((i["key"], i["fields"]["updated"]) for i in fresh)
                stats.issues += len(fresh)
                newest = max(parse_ts(i["fields"]["updated"]) for i in fresh)
                # Checkpoint after the page is durably landed: land first, then advance.
                wm.advance(newest, self.raw.run_id, last_raw_key=key)

            if not issues or start_at + len(issues) >= total:
                break
            boundary = parse_ts(issues[-1]["fields"]["updated"]).replace(second=0, microsecond=0)
            if boundary > lower:
                lower, start_at = boundary, 0  # keyset step: restart from the last minute seen
            else:
                start_at += len(issues)  # a whole page inside one minute: offset within it
        else:
            stats.stopped = "max_pages"
        stats.watermark_to = wm.current.isoformat()
