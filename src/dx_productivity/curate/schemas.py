"""Curated-table contracts: Arrow schemas, primary keys and the "parent" key used for merges.

Timestamps are UTC, microsecond precision, without a zone (Iceberg ``timestamp``), which maps
cleanly to DuckDB ``TIMESTAMP`` and Athena/Trino ``timestamp(6)``.

``parent`` is the grain at which a newer source record replaces older ones. For child tables
(transitions, fix/affects versions, reviews, check runs, Jira links) the whole set of children
of a parent is replaced together, so a version removed from an issue or a dismissed review
disappears instead of lingering.
"""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

TS = pa.timestamp("us")

_META = [
    pa.field("_version", pa.string(), nullable=False),
    pa.field("_raw_key", pa.string(), nullable=False),
]


@dataclass(frozen=True)
class TableSpec:
    name: str
    schema: pa.Schema
    primary_key: tuple[str, ...]
    parent_key: tuple[str, ...]
    doc: str


def _schema(*fields: pa.Field) -> pa.Schema:
    return pa.schema([*fields, *_META])


def f(name: str, typ: pa.DataType, nullable: bool = True) -> pa.Field:
    return pa.field(name, typ, nullable=nullable)


TABLES: dict[str, TableSpec] = {
    spec.name: spec
    for spec in [
        TableSpec(
            "jira_issues",
            _schema(
                f("issue_key", pa.string(), False),
                f("issue_id", pa.string()),
                f("project_key", pa.string(), False),
                f("summary", pa.string()),
                f("issue_type", pa.string()),
                f("is_subtask", pa.bool_()),
                f("status", pa.string()),
                f("status_category", pa.string()),
                f("priority", pa.string()),
                f("resolution", pa.string()),
                f("created_at", TS),
                f("updated_at", TS),
                f("resolved_at", TS),
                f("reporter_id", pa.string()),
                f("assignee_id", pa.string()),
                f("components", pa.string()),
                f("labels", pa.string()),
                f("changelog_total", pa.int64()),
                f("changelog_returned", pa.int64()),
            ),
            ("issue_key",),
            ("issue_key",),
            "One row per Jira issue, latest version seen.",
        ),
        TableSpec(
            "jira_issue_versions",
            _schema(
                f("issue_key", pa.string(), False),
                f("project_key", pa.string(), False),
                f("relation", pa.string(), False),
                f("version_id", pa.string(), False),
                f("version_name", pa.string()),
            ),
            ("issue_key", "relation", "version_id"),
            ("issue_key",),
            "Issue x version bridge: relation = fix (fixVersions) or affects (versions).",
        ),
        TableSpec(
            "jira_status_transitions",
            _schema(
                f("issue_key", pa.string(), False),
                f("project_key", pa.string(), False),
                f("history_id", pa.string(), False),
                f("item_seq", pa.int64(), False),
                f("changed_at", TS),
                f("author_id", pa.string()),
                f("from_status", pa.string()),
                f("to_status", pa.string()),
            ),
            ("issue_key", "history_id", "item_seq"),
            ("issue_key",),
            "Status changes from the issue changelog.",
        ),
        TableSpec(
            "jira_versions",
            _schema(
                f("version_id", pa.string(), False),
                f("project_key", pa.string(), False),
                f("version_name", pa.string()),
                f("released", pa.bool_()),
                f("archived", pa.bool_()),
                f("release_date", pa.date32()),
                f("start_date", pa.date32()),
            ),
            ("version_id",),
            ("version_id",),
            "Jira project versions (the projects' releases).",
        ),
        TableSpec(
            "gh_pull_requests",
            _schema(
                f("pr_key", pa.string(), False),
                f("repo", pa.string(), False),
                f("number", pa.int64(), False),
                f("title", pa.string()),
                f("state", pa.string()),
                f("is_draft", pa.bool_()),
                f("author_id", pa.string()),
                f("author_is_bot", pa.bool_()),
                f("author_association", pa.string()),
                f("created_at", TS),
                f("updated_at", TS),
                f("closed_at", TS),
                f("merged_at", TS),
                f("merge_commit_sha", pa.string()),
                f("head_sha", pa.string()),
                f("base_ref", pa.string()),
                f("labels", pa.string()),
                f("requested_reviewers", pa.int64()),
            ),
            ("pr_key",),
            ("pr_key",),
            "One row per pull request, latest version seen.",
        ),
        TableSpec(
            "gh_reviews",
            _schema(
                f("review_id", pa.string(), False),
                f("pr_key", pa.string(), False),
                f("repo", pa.string(), False),
                f("number", pa.int64(), False),
                f("reviewer_id", pa.string()),
                f("reviewer_is_bot", pa.bool_()),
                f("author_association", pa.string()),
                f("state", pa.string()),
                f("submitted_at", TS),
                f("commit_id", pa.string()),
            ),
            ("review_id",),
            ("pr_key",),
            "Submitted PR reviews (APPROVED, CHANGES_REQUESTED, COMMENTED, DISMISSED).",
        ),
        TableSpec(
            "gh_check_runs",
            _schema(
                f("check_run_id", pa.string(), False),
                f("pr_key", pa.string(), False),
                f("repo", pa.string(), False),
                f("head_sha", pa.string(), False),
                f("name", pa.string()),
                f("app_slug", pa.string()),
                f("status", pa.string()),
                f("conclusion", pa.string()),
                f("started_at", TS),
                f("completed_at", TS),
                f("total_count", pa.int64()),
            ),
            ("check_run_id",),
            ("pr_key",),
            "CI check runs on the head commit of closed PRs (first 100 per commit).",
        ),
        TableSpec(
            "gh_pr_fetches",
            _schema(
                f("pr_key", pa.string(), False),
                f("kind", pa.string(), False),
                f("repo", pa.string(), False),
                f("number", pa.int64(), False),
                f("fetched_at", TS),
                f("pr_updated_at", TS),
            ),
            ("pr_key", "kind"),
            ("pr_key", "kind"),
            "Which PRs had their reviews / check runs fetched, and when (distinguishes 'no reviews'"
            " from 'not fetched yet').",
        ),
        TableSpec(
            "gh_commits",
            _schema(
                f("sha", pa.string(), False),
                f("repo", pa.string(), False),
                f("branch", pa.string()),
                f("committed_at", TS),
                f("authored_at", TS),
                f("author_id", pa.string()),
                f("committer_id", pa.string()),
                f("message_title", pa.string()),
                f("referenced_pr", pa.int64()),
                f("reference_style", pa.string()),
            ),
            ("sha",),
            ("sha",),
            "Default-branch commits and the PR number each one closes, when it says so.",
        ),
        TableSpec(
            "gh_releases",
            _schema(
                f("release_id", pa.string(), False),
                f("repo", pa.string(), False),
                f("tag_name", pa.string()),
                f("name", pa.string()),
                f("is_prerelease", pa.bool_()),
                f("is_draft", pa.bool_()),
                f("created_at", TS),
                f("published_at", TS),
            ),
            ("release_id",),
            ("release_id",),
            "GitHub Releases (empty for repos that publish releases elsewhere).",
        ),
        TableSpec(
            "gh_pr_jira_links",
            _schema(
                f("pr_key", pa.string(), False),
                f("repo", pa.string(), False),
                f("number", pa.int64(), False),
                f("jira_key", pa.string(), False),
                f("jira_project", pa.string(), False),
                f("position", pa.int64(), False),
            ),
            ("pr_key", "jira_key"),
            ("pr_key",),
            "Jira issue keys mentioned in PR titles (e.g. 'KAFKA-1234: ...', '[SPARK-1][SQL] ...').",
        ),
    ]
}
