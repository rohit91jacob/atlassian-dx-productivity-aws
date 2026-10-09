"""Pure functions: raw envelope -> curated rows. No I/O, so they are unit-tested directly.

Every row carries ``_version`` (an ISO-8601 UTC string that orders versions of the same parent:
the source's own ``updated`` timestamp where one exists, otherwise the fetch time) and
``_raw_key`` (lineage back to the raw object).
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable
from datetime import date, datetime
from typing import Any

from dx_productivity.extract.watermark import parse_ts
from dx_productivity.pseudonym import pseudonymise

Rows = dict[str, list[dict[str, Any]]]
PARENTS = "@parents:"  # rows[PARENTS + table] marks a parent seen, even with zero children

# PR-title Jira references: "KAFKA-1234: x", "[SPARK-1234][SQL] x", "[FLINK-1][FLINK-2] x".
JIRA_KEY = re.compile(r"(?<![A-Za-z0-9])([A-Z][A-Z0-9_]*)-([1-9][0-9]*)(?![0-9])")
# How default-branch commits name the PR they land, by merge convention:
#   GitHub merge commit     "Merge pull request #123 from org/branch"
#   ASF merge scripts       "... Closes #123 from user/branch."   (Apache Spark)
#   Flink committers        "This closes #123"
#   GitHub squash merge     "Subject line (#123)"                 (Kafka, Flink)
_PR_REFERENCES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("merge_commit", re.compile(r"^Merge pull request #(\d+) from ", re.MULTILINE)),
    ("closes", re.compile(r"(?:^|\s)Closes #(\d+) from \S+", re.MULTILINE)),
    ("this_closes", re.compile(r"This closes #(\d+)", re.IGNORECASE)),
    ("squash_title", re.compile(r"\(#(\d+)\)\s*$")),
)


def ts(value: str | None) -> datetime | None:
    """Aware string -> naive UTC datetime (Iceberg ``timestamp``)."""
    if not value:
        return None
    return parse_ts(value).replace(tzinfo=None)


def version_of(value: str | None, fallback: str) -> str:
    """Normalise a source timestamp to a sortable ISO string (lexical order == time order)."""
    if not value:
        return fallback
    return parse_ts(value).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


def _name(obj: dict[str, Any] | None, key: str = "name") -> str | None:
    return obj.get(key) if obj else None


def _is_bot(user: dict[str, Any] | None) -> bool | None:
    if not user:
        return None
    return user.get("type") == "Bot" or str(user.get("login", "")).endswith("[bot]")


def jira_keys(title: str | None, projects: Iterable[str]) -> list[str]:
    allowed = set(projects)
    keys: list[str] = []
    for m in JIRA_KEY.finditer(title or ""):
        key = f"{m.group(1)}-{m.group(2)}"
        if m.group(1) in allowed and key not in keys:
            keys.append(key)
    return keys


def pr_reference(message: str) -> tuple[int, str] | None:
    title = message.splitlines()[0] if message else ""
    for style, pattern in _PR_REFERENCES:
        text = title if style == "squash_title" else message
        m = pattern.search(text)
        if m:
            return int(m.group(1)), style
    return None


class Normalizer:
    def __init__(self, salt: str, jira_projects: Iterable[str]):
        self.salt = salt
        self.jira_projects = tuple(jira_projects)

    def person(self, source: str, login: str | None) -> str | None:
        return pseudonymise(source, login, self.salt)

    def envelope(self, envelope: dict[str, Any], raw_key: str) -> Rows:
        handler = {
            ("jira", "issues"): self._jira_issues,
            ("jira", "versions"): self._jira_versions,
            ("github", "pulls"): self._gh_pulls,
            ("github", "pr_details"): self._gh_pr_details,
            ("github", "commits"): self._gh_commits,
            ("github", "releases"): self._gh_releases,
        }.get((envelope["source"], envelope["entity"]))
        if handler is None:
            raise ValueError(f"no normalizer for {envelope['source']}/{envelope['entity']}")
        rows: Rows = defaultdict(list)
        for page in envelope["pages"]:
            handler(page, envelope, raw_key, rows)
        return dict(rows)

    # ------------------------------------------------------------------ jira
    def _jira_issues(self, page: dict, env: dict, raw_key: str, rows: Rows) -> None:
        for issue in page["body"]["issues"]:
            fields = issue["fields"]
            key = issue["key"]
            project = (fields.get("project") or {}).get("key") or key.split("-")[0]
            version = version_of(fields.get("updated"), page["fetched_at"])
            meta = {"_version": version, "_raw_key": raw_key}
            changelog = issue.get("changelog") or {}
            histories = changelog.get("histories") or []
            status = fields.get("status") or {}
            rows["jira_issues"].append(
                {
                    "issue_key": key,
                    "issue_id": issue.get("id"),
                    "project_key": project,
                    "summary": fields.get("summary"),
                    "issue_type": _name(fields.get("issuetype")),
                    "is_subtask": (fields.get("issuetype") or {}).get("subtask"),
                    "status": status.get("name"),
                    "status_category": (status.get("statusCategory") or {}).get("key"),
                    "priority": _name(fields.get("priority")),
                    "resolution": _name(fields.get("resolution")),
                    "created_at": ts(fields.get("created")),
                    "updated_at": ts(fields.get("updated")),
                    "resolved_at": ts(fields.get("resolutiondate")),
                    "reporter_id": self.person("jira", _name(fields.get("reporter"))),
                    "assignee_id": self.person("jira", _name(fields.get("assignee"))),
                    "components": ",".join(
                        sorted(c["name"] for c in fields.get("components") or [])
                    ),
                    "labels": ",".join(sorted(fields.get("labels") or [])),
                    "changelog_total": changelog.get("total"),
                    "changelog_returned": len(histories),
                    **meta,
                }
            )
            marker = {"issue_key": key, **meta}
            rows[PARENTS + "jira_issue_versions"].append(marker)
            rows[PARENTS + "jira_status_transitions"].append(marker)
            for relation, field_name in (("fix", "fixVersions"), ("affects", "versions")):
                for v in fields.get(field_name) or []:
                    rows["jira_issue_versions"].append(
                        {
                            "issue_key": key,
                            "project_key": project,
                            "relation": relation,
                            "version_id": str(v["id"]),
                            "version_name": v.get("name"),
                            **meta,
                        }
                    )
            for history in histories:
                for seq, item in enumerate(history.get("items") or []):
                    if item.get("field") != "status":
                        continue
                    rows["jira_status_transitions"].append(
                        {
                            "issue_key": key,
                            "project_key": project,
                            "history_id": str(history["id"]),
                            "item_seq": seq,
                            "changed_at": ts(history.get("created")),
                            "author_id": self.person("jira", _name(history.get("author"))),
                            "from_status": item.get("fromString"),
                            "to_status": item.get("toString"),
                            **meta,
                        }
                    )

    def _jira_versions(self, page: dict, env: dict, raw_key: str, rows: Rows) -> None:
        project = env["scope"]
        version = version_of(page["fetched_at"], page["fetched_at"])
        for v in page["body"]:
            rows["jira_versions"].append(
                {
                    "version_id": str(v["id"]),
                    "project_key": project,
                    "version_name": v.get("name"),
                    "released": bool(v.get("released")),
                    "archived": bool(v.get("archived")),
                    "release_date": _date(v.get("releaseDate")),
                    "start_date": _date(v.get("startDate")),
                    "_version": version,
                    "_raw_key": raw_key,
                }
            )

    # ------------------------------------------------------------------ github
    def _pr_row(self, repo: str, pr: dict, fetched_at: str, raw_key: str) -> dict:
        user = pr.get("user")
        return {
            "pr_key": f"{repo}#{pr['number']}",
            "repo": repo,
            "number": pr["number"],
            "title": pr.get("title"),
            "state": pr.get("state"),
            "is_draft": pr.get("draft"),
            "author_id": self.person("github", _name(user, "login")),
            "author_is_bot": _is_bot(user),
            "author_association": pr.get("author_association"),
            "created_at": ts(pr.get("created_at")),
            "updated_at": ts(pr.get("updated_at")),
            "closed_at": ts(pr.get("closed_at")),
            "merged_at": ts(pr.get("merged_at")),
            "merge_commit_sha": pr.get("merge_commit_sha"),
            "head_sha": (pr.get("head") or {}).get("sha"),
            "base_ref": (pr.get("base") or {}).get("ref"),
            "labels": ",".join(sorted(label["name"] for label in pr.get("labels") or [])),
            "requested_reviewers": len(pr.get("requested_reviewers") or []),
            "_version": version_of(pr.get("updated_at"), fetched_at),
            "_raw_key": raw_key,
        }

    def _gh_pulls(self, page: dict, env: dict, raw_key: str, rows: Rows) -> None:
        repo = env["meta"]["repo"]
        # The last listing page of a run also holds PRs updated before the run's window (the API
        # pages by 100). Keep only in-window PRs, exactly the set the extractor queued, so that
        # "every PR updated since the first watermark, and nothing older" holds for the curated
        # table and the coverage start (int_github_coverage) is a true completeness boundary.
        lower = ts(env["meta"].get("lower_bound"))
        for pr in page["body"]:
            updated = ts(pr.get("updated_at"))
            if lower is not None and updated is not None and updated < lower:
                continue
            row = self._pr_row(repo, pr, page["fetched_at"], raw_key)
            rows["gh_pull_requests"].append(row)
            rows[PARENTS + "gh_pr_jira_links"].append(
                {"pr_key": row["pr_key"], "_version": row["_version"], "_raw_key": raw_key}
            )
            for position, key in enumerate(jira_keys(pr.get("title"), self.jira_projects)):
                rows["gh_pr_jira_links"].append(
                    {
                        "pr_key": row["pr_key"],
                        "repo": repo,
                        "number": pr["number"],
                        "jira_key": key,
                        "jira_project": key.split("-")[0],
                        "position": position,
                        "_version": row["_version"],
                        "_raw_key": raw_key,
                    }
                )

    def _gh_pr_details(self, page: dict, env: dict, raw_key: str, rows: Rows) -> None:
        repo = env["meta"]["repo"]
        number = page["pr_number"]
        pr_key = f"{repo}#{number}"
        # Child sets are versioned by the run that fetched them (all pages of one PR share it):
        # a later fetch is a fuller picture.
        version = version_of(env["landed_at"], env["landed_at"])
        meta = {"_version": version, "_raw_key": raw_key}
        child_table = {"reviews": "gh_reviews", "check_runs": "gh_check_runs"}.get(page["kind"])
        if child_table:
            rows[PARENTS + child_table].append({"pr_key": pr_key, **meta})
            rows["gh_pr_fetches"].append(
                {
                    "pr_key": pr_key,
                    "kind": page["kind"],
                    "repo": repo,
                    "number": number,
                    "fetched_at": ts(page["fetched_at"]),
                    "pr_updated_at": ts(page.get("pr_updated_at")),
                    **meta,
                }
            )
        if page["kind"] == "reviews":
            for review in page["body"]:
                user = review.get("user")
                rows["gh_reviews"].append(
                    {
                        "review_id": str(review["id"]),
                        "pr_key": pr_key,
                        "repo": repo,
                        "number": number,
                        "reviewer_id": self.person("github", _name(user, "login")),
                        "reviewer_is_bot": _is_bot(user),
                        "author_association": review.get("author_association"),
                        "state": review.get("state"),
                        "submitted_at": ts(review.get("submitted_at")),
                        "commit_id": review.get("commit_id"),
                        **meta,
                    }
                )
        elif page["kind"] == "check_runs":
            body = page["body"]
            for run in body.get("check_runs") or []:
                rows["gh_check_runs"].append(
                    {
                        "check_run_id": str(run["id"]),
                        "pr_key": pr_key,
                        "repo": repo,
                        "head_sha": page["head_sha"],
                        "name": run.get("name"),
                        "app_slug": (run.get("app") or {}).get("slug"),
                        "status": run.get("status"),
                        "conclusion": run.get("conclusion"),
                        "started_at": ts(run.get("started_at")),
                        "completed_at": ts(run.get("completed_at")),
                        "total_count": body.get("total_count"),
                        **meta,
                    }
                )
        else:
            raise ValueError(f"unknown pr_details kind {page['kind']!r}")

    def _gh_commits(self, page: dict, env: dict, raw_key: str, rows: Rows) -> None:
        repo = env["meta"]["repo"]
        branch = env["meta"].get("branch")
        for c in page["body"]:
            commit = c["commit"]
            message = commit.get("message") or ""
            ref = pr_reference(message)
            rows["gh_commits"].append(
                {
                    "sha": c["sha"],
                    "repo": repo,
                    "branch": branch,
                    "committed_at": ts(commit["committer"]["date"]),
                    "authored_at": ts((commit.get("author") or {}).get("date")),
                    "author_id": self.person("github", _name(c.get("author"), "login")),
                    "committer_id": self.person("github", _name(c.get("committer"), "login")),
                    "message_title": message.splitlines()[0] if message else None,
                    "referenced_pr": ref[0] if ref else None,
                    "reference_style": ref[1] if ref else None,
                    # Commits are immutable; version by commit time so reruns are stable.
                    "_version": version_of(commit["committer"]["date"], page["fetched_at"]),
                    "_raw_key": raw_key,
                }
            )

    def _gh_releases(self, page: dict, env: dict, raw_key: str, rows: Rows) -> None:
        repo = env["meta"]["repo"]
        version = version_of(page["fetched_at"], page["fetched_at"])
        for r in page["body"]:
            rows["gh_releases"].append(
                {
                    "release_id": str(r["id"]),
                    "repo": repo,
                    "tag_name": r.get("tag_name"),
                    "name": r.get("name"),
                    "is_prerelease": r.get("prerelease"),
                    "is_draft": r.get("draft"),
                    "created_at": ts(r.get("created_at")),
                    "published_at": ts(r.get("published_at")),
                    "_version": version,
                    "_raw_key": raw_key,
                }
            )
