from __future__ import annotations

import json
from datetime import datetime

import pytest

from dx_productivity.curate.normalize import (
    PARENTS,
    Normalizer,
    jira_keys,
    pr_reference,
    version_of,
)
from dx_productivity.pseudonym import pseudonymise
from tests.conftest import FIXTURES

PROJECTS = ("KAFKA", "SPARK", "FLINK")


@pytest.mark.parametrize(
    ("title", "keys"),
    [
        ("KAFKA-21119: Replace temp file handler", ["KAFKA-21119"]),
        ("[SPARK-57881][SQL] UnionExec outputPartitioning", ["SPARK-57881"]),
        ("[FLINK-1][FLINK-22] two issues, [FLINK-1] repeated", ["FLINK-1", "FLINK-22"]),
        ("MINOR: fix typo", []),
        ("HADOOP-123 is another project", []),
        ("KAFKA-0 is not a key, XKAFKA-12 is not either", []),
    ],
)
def test_jira_keys_in_pr_titles(title: str, keys: list[str]) -> None:
    assert jira_keys(title, PROJECTS) == keys


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        (
            "[SPARK-1][SQL] Fix\n\n### What changes\n\nCloses #52313 from someone/branch-1.\n",
            (52313, "closes"),
        ),
        ("[FLINK-2] Fix\n\nThis closes #27011.", (27011, "this_closes")),
        ("KAFKA-3: Faster thing (#20512)", (20512, "squash_title")),
        ("Merge pull request #12 from org/feature\n\nstuff", (12, "merge_commit")),
        ("[hotfix] direct push without a PR", None),
        ("KAFKA-4: title\n\nreverts (#99) in the body only", None),
    ],
)
def test_pr_references_in_commit_messages(message: str, expected) -> None:
    assert pr_reference(message) == expected


def test_versions_sort_lexically_in_time_order() -> None:
    a = version_of("2026-10-08T23:59:59.000+0000", "x")
    b = version_of("2026-10-09T00:00:00Z", "x")
    assert a < b
    assert version_of(None, "fallback") == "fallback"


def test_pseudonyms_are_stable_salted_and_case_insensitive() -> None:
    a = pseudonymise("github", "SomeUser", "salt")
    assert a == pseudonymise("github", "someuser", "salt")
    assert a != pseudonymise("github", "someuser", "other-salt")
    assert a != pseudonymise("jira", "someuser", "salt")
    assert a.startswith("u_") and len(a) == 14
    assert pseudonymise("github", None, "salt") is None


def _envelope(source: str, entity: str, scope: str, body, **page_extra) -> dict:
    return {
        "schema": "dx.raw.v1",
        "source": source,
        "entity": entity,
        "scope": scope,
        "run_id": "r1",
        "landed_at": "2026-10-09T16:25:16Z",
        "meta": {"repo": "apache/kafka", "branch": "trunk"},
        "pages": [
            {
                "request": {},
                "status": 200,
                "headers": {},
                "fetched_at": "2026-10-09T16:25:16Z",
                "body": body,
                **page_extra,
            }
        ],
    }


def test_jira_issue_rows_from_real_fixture() -> None:
    issues = json.loads((FIXTURES / "jira" / "issues_KAFKA.json").read_text(encoding="utf-8"))
    rows = Normalizer("salt", PROJECTS).envelope(
        _envelope("jira", "issues", "KAFKA", {"issues": issues}), "raw/k1"
    )
    assert len(rows["jira_issues"]) == len(issues)
    first = rows["jira_issues"][0]
    assert first["project_key"] == "KAFKA"
    assert isinstance(first["created_at"], datetime) and first["created_at"].tzinfo is None
    assert first["reporter_id"] is None or first["reporter_id"].startswith("u_")
    # Every issue is a parent marker for its child tables, even with no children.
    assert len(rows[PARENTS + "jira_status_transitions"]) == len(issues)
    assert all(t["to_status"] for t in rows["jira_status_transitions"])
    assert {v["relation"] for v in rows["jira_issue_versions"]} <= {"fix", "affects"}


def test_pull_request_rows_and_links_from_real_fixture() -> None:
    pulls = json.loads(
        (FIXTURES / "github" / "apache__spark" / "pulls.json").read_text(encoding="utf-8")
    )
    env = _envelope("github", "pulls", "apache__spark", pulls)
    env["meta"]["repo"] = "apache/spark"
    rows = Normalizer("salt", PROJECTS).envelope(env, "raw/p1")
    prs = rows["gh_pull_requests"]
    assert len(prs) == len(pulls)
    assert all(p["pr_key"].startswith("apache/spark#") for p in prs)
    # Spark closes PRs through its merge script: GitHub never records merged_at.
    assert all(p["merged_at"] is None for p in prs)
    assert {link["jira_project"] for link in rows["gh_pr_jira_links"]} == {"SPARK"}


def test_reviews_and_check_runs_carry_fetch_log_and_bot_flag() -> None:
    reviews = [
        {
            "id": 1,
            "user": {"login": "alice", "type": "User"},
            "state": "APPROVED",
            "submitted_at": "2026-10-08T10:00:00Z",
            "commit_id": "a" * 40,
        },
        {
            "id": 2,
            "user": {"login": "copilot-pull-request-reviewer[bot]", "type": "Bot"},
            "state": "COMMENTED",
            "submitted_at": "2026-10-08T09:00:00Z",
            "commit_id": "a" * 40,
        },
    ]
    env = _envelope(
        "github",
        "pr_details",
        "apache__kafka",
        reviews,
        kind="reviews",
        pr_number=7,
        pr_updated_at="2026-10-08T11:00:00Z",
    )
    rows = Normalizer("salt", PROJECTS).envelope(env, "raw/d1")
    assert [r["reviewer_is_bot"] for r in rows["gh_reviews"]] == [False, True]
    assert rows["gh_pr_fetches"][0]["kind"] == "reviews"
    assert rows[PARENTS + "gh_reviews"] == [
        {
            "pr_key": "apache/kafka#7",
            "_version": "2026-10-09T16:25:16.000000Z",
            "_raw_key": "raw/d1",
        }
    ]


def test_unknown_entity_is_rejected() -> None:
    with pytest.raises(ValueError, match="no normalizer"):
        Normalizer("salt", PROJECTS).envelope(_envelope("github", "gists", "x", []), "raw/x")
