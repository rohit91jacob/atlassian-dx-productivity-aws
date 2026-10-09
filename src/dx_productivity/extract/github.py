"""Incremental extractor for GitHub REST v3: pull requests, reviews, CI check runs, default-branch
commits and releases.

Streams per repository and how each one is made incremental:

* ``pulls``: ``GET /pulls?state=all&sort=updated&direction=desc`` paged until ``updated_at``
  drops below ``watermark - overlap``. Descending order is skip-safe: a PR edited mid-listing moves
  to page 1 and can only be seen twice, never missed.
* ``reviews`` and ``check_runs``: per-PR enrichment, oldest change first. The ``pulls`` watermark
  only advances past a PR once its reviews (and, for closed PRs, its head commit's check runs)
  have been landed. If the request budget runs out, the next run re-lists from the watermark and
  carries on, so a backlog drains across runs without gaps.
* ``commits``: ``GET /commits?sha=<default branch>&since=<watermark - overlap>``. Apache Spark
  closes PRs through a merge script instead of GitHub's merge button, so "merged" is inferred
  from ``Closes #123`` / ``(#123)`` references in default-branch commits (see curate/normalize.py).
* ``releases``: a snapshot of the latest 100 releases. (Kafka, Spark and Flink publish their
  releases through Jira versions and ASF mirrors rather than GitHub Releases, so this is usually
  empty for them; it is kept for repositories that do use it.)

Authentication: ``GITHUB_TOKEN`` when set (5,000 req/h for a personal token; 1,000 req/h per repo
for the Actions token); otherwise unauthenticated (60 req/h per IP). ``DX_GITHUB_MAX_REQUESTS``
caps every run, split fairly across repositories.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from dx_productivity.config import RepoConfig, Settings
from dx_productivity.extract.raw import RawWriter, iso, page_record
from dx_productivity.extract.watermark import Watermark, parse_ts
from dx_productivity.http import BudgetExhausted, HttpClient, RateLimited
from dx_productivity.logs import get_logger
from dx_productivity.storage.state import StateStore, watermark_key

log = get_logger(__name__)

PER_PAGE = 100
ENRICH_BUNDLE = 25


@dataclass
class GitHubStats:
    repo: str
    pull_pages: int = 0
    pulls_listed: int = 0
    pulls_enriched: int = 0
    pulls_pending: int = 0
    review_calls: int = 0
    check_run_calls: int = 0
    commit_pages: int = 0
    commits: int = 0
    releases: int = 0
    requests: int = 0
    stopped: str = "complete"
    raw_keys: list[str] = field(default_factory=list)


class _RepoBudget(Exception):  # noqa: N818 - control flow
    pass


class GitHubExtractor:
    def __init__(
        self,
        settings: Settings,
        client: HttpClient,
        raw: RawWriter,
        state: StateStore,
        clock: Callable[[], datetime],
        should_stop: Callable[[], bool] = lambda: False,
        fetch_check_runs: bool = True,
    ):
        self.settings = settings
        self.client = client
        self.raw = raw
        self.state = state
        self.clock = clock
        self.should_stop = should_stop
        self.fetch_check_runs = fetch_check_runs
        self.api = settings.github_api_url
        self._repo_limit: int | None = None

    # ------------------------------------------------------------------ orchestration
    def run(self, repos: tuple[RepoConfig, ...] | None = None) -> list[GitHubStats]:
        repos = repos or self.settings.github_repos
        results: list[GitHubStats] = []
        rate_limited = False
        for index, repo in enumerate(repos):
            stats = GitHubStats(repo.full_name)
            results.append(stats)
            if rate_limited:
                stats.stopped = "rate_limited"
                continue
            remaining = self.client.remaining_budget
            share = None if remaining is None else max(1, remaining // (len(repos) - index))
            start = self.client.requests_made
            self._repo_limit = None if share is None else start + share
            try:
                self._extract_repo(repo, stats)
            except RateLimited as exc:
                stats.stopped = "rate_limited"
                rate_limited = True
                log.warning(
                    "github rate limit exhausted",
                    extra={"repo": repo.full_name, "reset_at": exc.reset_at},
                )
            except (BudgetExhausted, _RepoBudget):
                stats.stopped = "budget"
            stats.requests = self.client.requests_made - start
            log.info(
                "github repo extracted",
                extra={k: v for k, v in vars(stats).items() if k != "raw_keys"},
            )
        return results

    def _get(self, path: str, params: dict[str, Any] | None = None):
        if self._repo_limit is not None and self.client.requests_made >= self._repo_limit:
            raise _RepoBudget
        if self.should_stop():
            raise _RepoBudget
        return self.client.get_json(f"{self.api}{path}", params)

    def _extract_repo(self, repo: RepoConfig, stats: GitHubStats) -> None:
        # Cheapest, most valuable streams first: a budget stop still leaves complete snapshots.
        self.extract_releases(repo, stats)
        self.extract_commits(repo, stats)
        self.extract_pulls(repo, stats)

    # ------------------------------------------------------------------ releases
    def extract_releases(self, repo: RepoConfig, stats: GitHubStats) -> None:
        body, response, request = self._get(
            f"/repos/{repo.full_name}/releases", {"per_page": PER_PAGE}
        )
        key = self.raw.land(
            "github",
            "releases",
            repo.slug,
            [page_record(request, response, body, self.clock())],
            meta={"repo": repo.full_name, "snapshot": True},
        )
        stats.releases = len(body)
        stats.raw_keys.append(key)

    # ------------------------------------------------------------------ commits
    def extract_commits(self, repo: RepoConfig, stats: GitHubStats) -> None:
        settings = self.settings
        wm = Watermark(
            self.state,
            watermark_key("github", repo.slug, "commits"),
            self.clock() - timedelta(days=settings.github_lookback_days),
        )
        wm.load()
        since = wm.start_with_overlap(timedelta(minutes=settings.overlap_minutes))
        newest: datetime | None = None
        pages: list[dict] = []
        page = 1
        while True:
            body, response, request = self._get(
                f"/repos/{repo.full_name}/commits",
                {
                    "sha": repo.default_branch,
                    "since": iso(since),
                    "per_page": PER_PAGE,
                    "page": page,
                },
            )
            if body:
                pages.append(page_record(request, response, body, self.clock()))
                stats.commits += len(body)
                for commit in body:
                    ts = parse_ts(commit["commit"]["committer"]["date"])
                    newest = ts if newest is None or ts > newest else newest
            if len(body) < PER_PAGE:
                break
            page += 1
        if pages:
            key = self.raw.land(
                "github",
                "commits",
                repo.slug,
                pages,
                meta={"repo": repo.full_name, "branch": repo.default_branch, "since": iso(since)},
            )
            stats.raw_keys.append(key)
            stats.commit_pages = len(pages)
        # Only advance once every page for this window is landed.
        if newest is not None:
            wm.advance(newest, self.raw.run_id)

    # ------------------------------------------------------------------ pulls + enrichment
    def extract_pulls(self, repo: RepoConfig, stats: GitHubStats) -> None:
        settings = self.settings
        wm = Watermark(
            self.state,
            watermark_key("github", repo.slug, "pulls"),
            self.clock() - timedelta(days=settings.github_lookback_days),
        )
        wm.load()
        lower = wm.start_with_overlap(timedelta(minutes=settings.overlap_minutes))

        listed: dict[int, dict[str, Any]] = {}
        page = 1
        while True:
            body, response, request = self._get(
                f"/repos/{repo.full_name}/pulls",
                {
                    "state": "all",
                    "sort": "updated",
                    "direction": "desc",
                    "per_page": PER_PAGE,
                    "page": page,
                },
            )
            in_window = [pr for pr in body if parse_ts(pr["updated_at"]) >= lower]
            if in_window:
                key = self.raw.land(
                    "github",
                    "pulls",
                    repo.slug,
                    [page_record(request, response, body, self.clock())],
                    meta={"repo": repo.full_name, "lower_bound": iso(lower), "page": page},
                )
                stats.raw_keys.append(key)
                stats.pull_pages += 1
                for pr in in_window:
                    prev = listed.get(pr["number"])
                    if prev is None or pr["updated_at"] > prev["updated_at"]:
                        listed[pr["number"]] = pr
            if len(body) < PER_PAGE or len(in_window) < len(body):
                break
            page += 1
        stats.pulls_listed = len(listed)

        queue = sorted(listed.values(), key=lambda pr: (pr["updated_at"], pr["number"]))
        bundle: list[dict] = []
        bundle_last: datetime | None = None
        try:
            for pr in queue:
                bundle.extend(self._enrich(repo, pr, stats))
                bundle_last = parse_ts(pr["updated_at"])
                stats.pulls_enriched += 1
                if stats.pulls_enriched % ENRICH_BUNDLE == 0:
                    self._flush(repo, bundle, bundle_last, wm, stats)
                    bundle = []
        finally:
            # Flush what is complete even when the budget/rate limit stops us mid-queue.
            self._flush(repo, bundle, bundle_last, wm, stats)
            stats.pulls_pending = len(queue) - stats.pulls_enriched

    def _enrich(self, repo: RepoConfig, pr: dict[str, Any], stats: GitHubStats) -> list[dict]:
        records: list[dict] = []
        number = pr["number"]
        page = 1
        while True:
            body, response, request = self._get(
                f"/repos/{repo.full_name}/pulls/{number}/reviews",
                {"per_page": PER_PAGE, "page": page},
            )
            stats.review_calls += 1
            records.append(
                page_record(
                    request,
                    response,
                    body,
                    self.clock(),
                    kind="reviews",
                    pr_number=number,
                    pr_updated_at=pr["updated_at"],
                )
            )
            if len(body) < PER_PAGE:
                break
            page += 1
        sha = (pr.get("head") or {}).get("sha")
        if self.fetch_check_runs and pr.get("state") == "closed" and sha:
            body, response, request = self._get(
                f"/repos/{repo.full_name}/commits/{sha}/check-runs", {"per_page": PER_PAGE}
            )
            stats.check_run_calls += 1
            records.append(
                page_record(
                    request,
                    response,
                    body,
                    self.clock(),
                    kind="check_runs",
                    pr_number=number,
                    pr_updated_at=pr["updated_at"],
                    head_sha=sha,
                )
            )
        return records

    def _flush(
        self,
        repo: RepoConfig,
        bundle: list[dict],
        last: datetime | None,
        wm: Watermark,
        stats: GitHubStats,
    ) -> None:
        if not bundle or last is None:
            return
        key = self.raw.land(
            "github", "pr_details", repo.slug, bundle, meta={"repo": repo.full_name}
        )
        stats.raw_keys.append(key)
        wm.advance(last, self.raw.run_id, last_raw_key=key)
