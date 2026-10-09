"""Declarative data-quality checks that gate each layer.

A check is a SQL query that returns the *violating* rows. ``error`` checks block the layer
(the curated tables are not published, dbt does not run, the CLI exits with code 3); ``warn``
checks are reported and recorded but do not block. Every result is persisted to the ops zone
(``ops/dq_results/...``) for trend monitoring.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime

import duckdb

from dx_productivity.curate.schemas import TABLES
from dx_productivity.storage.objects import ObjectStore


@dataclass(frozen=True)
class Check:
    name: str
    layer: str
    severity: str  # error | warn
    sql: str
    description: str


@dataclass
class CheckResult:
    name: str
    layer: str
    severity: str
    passed: bool
    violations: int
    sample: list[str] = field(default_factory=list)
    description: str = ""


class DataQualityError(RuntimeError):
    def __init__(self, failures: list[CheckResult]):
        names = ", ".join(f"{f.layer}.{f.name} ({f.violations})" for f in failures)
        super().__init__(f"data-quality gate failed: {names}")
        self.failures = failures


# ---------------------------------------------------------------------------- raw gate
RAW_SHAPES: dict[tuple[str, str], str] = {
    ("jira", "issues"): "dict with an 'issues' list of {key, fields.updated}",
    ("jira", "versions"): "list of {id, name}",
    ("github", "pulls"): "list of {number, updated_at, created_at, state}",
    ("github", "pr_details"): "reviews list or check_runs object, tagged with pr_number",
    ("github", "commits"): "list of {sha, commit.committer.date}",
    ("github", "releases"): "list of {id, tag_name}",
}


def validate_envelope(envelope: dict) -> list[str]:
    """Return the problems with one raw envelope (empty list = valid)."""
    problems: list[str] = []
    if envelope.get("schema") != "dx.raw.v1":
        return [f"unknown envelope schema {envelope.get('schema')!r}"]
    kind = (envelope.get("source"), envelope.get("entity"))
    if kind not in RAW_SHAPES:
        return [f"unknown source/entity {kind}"]
    pages = envelope.get("pages")
    if not isinstance(pages, list) or not pages:
        return ["envelope has no pages"]
    for i, page in enumerate(pages):
        body = page.get("body")
        status = page.get("status")
        if status != 200:
            problems.append(f"page {i}: HTTP status {status}")
            continue
        try:
            _validate_body(kind, page, body)
        except (KeyError, TypeError, AssertionError) as exc:
            problems.append(f"page {i}: expected {RAW_SHAPES[kind]} ({type(exc).__name__}: {exc})")
    return problems


def _validate_body(kind: tuple[str, str], page: dict, body: object) -> None:
    if kind == ("jira", "issues"):
        assert isinstance(body, dict), "body is not an object"
        for issue in body["issues"]:
            assert issue["key"] and issue["fields"]["updated"], "issue without key/updated"
    elif kind == ("jira", "versions"):
        assert isinstance(body, list), "body is not a list"
        for v in body:
            assert v["id"] and "name" in v, "version without id/name"
    elif kind == ("github", "pulls"):
        assert isinstance(body, list), "body is not a list"
        for pr in body:
            assert pr["number"] and pr["updated_at"] and pr["created_at"] and pr["state"]
    elif kind == ("github", "pr_details"):
        assert isinstance(page["pr_number"], int), "pr_number missing"
        if page["kind"] == "reviews":
            assert isinstance(body, list), "reviews body is not a list"
        else:
            assert isinstance(body["check_runs"], list), "check_runs missing"
    elif kind == ("github", "commits"):
        assert isinstance(body, list), "body is not a list"
        for c in body:
            assert c["sha"] and c["commit"]["committer"]["date"]
    elif kind == ("github", "releases"):
        assert isinstance(body, list), "body is not a list"


# ---------------------------------------------------------------------------- curated
def curated_checks(as_of: datetime) -> list[Check]:
    future = f"TIMESTAMP '{as_of.replace(tzinfo=None):%Y-%m-%d %H:%M:%S}' + INTERVAL 1 DAY"
    checks: list[Check] = []
    for spec in TABLES.values():
        pk = ", ".join(spec.primary_key)
        checks.append(
            Check(
                f"{spec.name}__pk_unique",
                "curated",
                "error",
                f"SELECT {pk}, count(*) AS n FROM {spec.name} GROUP BY ALL HAVING count(*) > 1",
                f"{spec.name}: primary key ({pk}) is unique",
            )
        )
        nulls = " OR ".join(f"{c} IS NULL" for c in spec.primary_key)
        checks.append(
            Check(
                f"{spec.name}__pk_not_null",
                "curated",
                "error",
                f"SELECT * FROM {spec.name} WHERE {nulls}",
                f"{spec.name}: primary key is never null",
            )
        )
    checks += [
        Check(
            "jira_issues__created_not_null",
            "curated",
            "error",
            "SELECT issue_key FROM jira_issues WHERE created_at IS NULL OR updated_at IS NULL",
            "every issue has created and updated timestamps",
        ),
        Check(
            "jira_issues__not_in_future",
            "curated",
            "error",
            f"SELECT issue_key FROM jira_issues WHERE updated_at > {future} OR created_at > {future}",
            "no issue timestamp is in the future (clock/timezone bug guard)",
        ),
        Check(
            "jira_issues__updated_after_created",
            "curated",
            "warn",
            "SELECT issue_key FROM jira_issues WHERE updated_at < created_at",
            "updated >= created",
        ),
        Check(
            "jira_issues__resolved_after_created",
            "curated",
            "warn",
            "SELECT issue_key FROM jira_issues WHERE resolved_at < created_at",
            "resolved >= created (migrated issues can violate this)",
        ),
        Check(
            "jira_issues__status_present",
            "curated",
            "error",
            "SELECT issue_key FROM jira_issues WHERE status IS NULL OR status_category IS NULL",
            "every issue has a status and status category",
        ),
        Check(
            "jira_status_transitions__issue_exists",
            "curated",
            "error",
            "SELECT t.issue_key FROM jira_status_transitions t ANTI JOIN jira_issues i USING (issue_key)",
            "every transition belongs to a curated issue",
        ),
        Check(
            "jira_status_transitions__to_status",
            "curated",
            "error",
            "SELECT issue_key FROM jira_status_transitions WHERE to_status IS NULL OR changed_at IS NULL",
            "transitions have a target status and a timestamp",
        ),
        Check(
            "jira_issue_versions__issue_exists",
            "curated",
            "error",
            "SELECT v.issue_key FROM jira_issue_versions v ANTI JOIN jira_issues i USING (issue_key)",
            "every fix/affects version row belongs to a curated issue",
        ),
        Check(
            "jira_versions__released_has_date",
            "curated",
            "warn",
            "SELECT version_id FROM jira_versions WHERE released AND release_date IS NULL",
            "released versions carry a release date (some historic ones do not)",
        ),
        Check(
            "gh_pull_requests__timestamps",
            "curated",
            "error",
            "SELECT pr_key FROM gh_pull_requests WHERE created_at IS NULL OR merged_at < created_at"
            " OR closed_at < created_at",
            "PRs have created_at, and merge/close never precede creation",
        ),
        Check(
            "gh_pull_requests__state",
            "curated",
            "error",
            "SELECT pr_key FROM gh_pull_requests WHERE state NOT IN ('open', 'closed')"
            " OR (merged_at IS NOT NULL AND state <> 'closed')",
            "state is open/closed and merged PRs are closed",
        ),
        Check(
            "gh_pull_requests__not_in_future",
            "curated",
            "error",
            f"SELECT pr_key FROM gh_pull_requests WHERE updated_at > {future}",
            "no PR timestamp is in the future",
        ),
        Check(
            "gh_reviews__pr_exists",
            "curated",
            "error",
            "SELECT r.pr_key FROM gh_reviews r ANTI JOIN gh_pull_requests p USING (pr_key)",
            "every review belongs to a curated PR",
        ),
        Check(
            "gh_reviews__state",
            "curated",
            "warn",
            "SELECT review_id FROM gh_reviews WHERE state NOT IN"
            " ('APPROVED', 'CHANGES_REQUESTED', 'COMMENTED', 'DISMISSED', 'PENDING')",
            "review states are GitHub's documented values",
        ),
        Check(
            "gh_check_runs__pr_exists",
            "curated",
            "error",
            "SELECT c.pr_key FROM gh_check_runs c ANTI JOIN gh_pull_requests p USING (pr_key)",
            "every check run belongs to a curated PR",
        ),
        Check(
            "gh_check_runs__duration",
            "curated",
            "warn",
            "SELECT check_run_id FROM gh_check_runs WHERE completed_at < started_at",
            "check runs finish after they start",
        ),
        Check(
            "gh_commits__timestamp",
            "curated",
            "error",
            "SELECT sha FROM gh_commits WHERE committed_at IS NULL",
            "every commit has a committer timestamp",
        ),
        Check(
            "gh_pr_jira_links__pr_exists",
            "curated",
            "error",
            "SELECT l.pr_key FROM gh_pr_jira_links l ANTI JOIN gh_pull_requests p USING (pr_key)",
            "every PR->Jira link belongs to a curated PR",
        ),
    ]
    return checks


# ---------------------------------------------------------------------------- marts
MART_CHECKS: list[Check] = [
    Check(
        "fct_pull_requests__not_empty",
        "marts",
        "error",
        "SELECT 1 WHERE (SELECT count(*) FROM marts.fct_pull_requests) = 0",
        "the PR fact table has rows",
    ),
    Check(
        "fct_jira_issues__not_empty",
        "marts",
        "error",
        "SELECT 1 WHERE (SELECT count(*) FROM marts.fct_jira_issues) = 0",
        "the issue fact table has rows",
    ),
    Check(
        "mart_dora_monthly__rates_in_range",
        "marts",
        "error",
        "SELECT * FROM marts.mart_dora_monthly WHERE change_failure_rate_proxy < 0"
        " OR change_failure_rate_proxy > 1 OR deployments < 0",
        "DORA rates are proportions and counts are non-negative",
    ),
    Check(
        "mart_pr_cycle_time_weekly__ordering",
        "marts",
        "error",
        "SELECT * FROM marts.mart_pr_cycle_time_weekly WHERE p90_hours_to_merge < p50_hours_to_merge",
        "p90 >= p50",
    ),
    Check(
        "fct_pull_requests__merge_breakdown_adds_up",
        "marts",
        "error",
        "SELECT pr_key FROM marts.fct_pull_requests WHERE hours_to_merge IS NOT NULL"
        " AND hours_to_first_review IS NOT NULL AND hours_to_first_review > hours_to_merge + 0.01"
        " AND first_review_at <= merged_at",
        "time to first review never exceeds time to merge when the review precedes the merge",
    ),
    Check(
        "mart_pr_jira_linkage__rate_in_range",
        "marts",
        "error",
        "SELECT * FROM marts.mart_pr_jira_linkage WHERE linked_share < 0 OR linked_share > 1",
        "linkage share is a proportion",
    ),
    Check(
        "fct_releases__has_releases",
        "marts",
        "warn",
        "SELECT 1 WHERE (SELECT count(*) FROM marts.fct_releases) = 0",
        "release history is present",
    ),
]


# ---------------------------------------------------------------------------- runner
def run_checks(con: duckdb.DuckDBPyConnection, checks: list[Check]) -> list[CheckResult]:
    results: list[CheckResult] = []
    for check in checks:
        rows = con.execute(f"SELECT * FROM ({check.sql}) LIMIT 1000").fetchall()
        results.append(
            CheckResult(
                check.name,
                check.layer,
                check.severity,
                passed=not rows,
                violations=len(rows),
                sample=[str(r) for r in rows[:5]],
                description=check.description,
            )
        )
    return results


def gate(results: list[CheckResult], fail_on_error: bool = True) -> None:
    failures = [r for r in results if not r.passed and r.severity == "error"]
    if failures and fail_on_error:
        raise DataQualityError(failures)


def record(
    store: ObjectStore, results: list[CheckResult], run_id: str, layer: str, now: datetime
) -> str:
    key = f"ops/dq_results/dt={now:%Y-%m-%d}/{run_id}-{layer}.json"
    payload = {
        "run_id": run_id,
        "layer": layer,
        "checked_at": now.isoformat(),
        "results": [asdict(r) for r in results],
    }
    store.put(key, json.dumps(payload, indent=1, default=str).encode())
    return key
