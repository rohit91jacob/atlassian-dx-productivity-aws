from __future__ import annotations

from datetime import UTC, datetime

import duckdb
import pyarrow as pa
import pytest

from dx_productivity.curate.schemas import TABLES
from dx_productivity.quality import checks as dq


def env(entity="issues", source="jira", body=None, status=200, **extra) -> dict:
    return {
        "schema": "dx.raw.v1",
        "source": source,
        "entity": entity,
        "pages": [
            {"status": status, "body": body if body is not None else {"issues": []}, **extra}
        ],
    }


def test_valid_envelopes_pass() -> None:
    assert (
        dq.validate_envelope(env(body={"issues": [{"key": "K-1", "fields": {"updated": "x"}}]}))
        == []
    )
    assert (
        dq.validate_envelope(
            env(
                "pulls",
                "github",
                [{"number": 1, "updated_at": "x", "created_at": "x", "state": "open"}],
            )
        )
        == []
    )


@pytest.mark.parametrize(
    ("envelope", "problem"),
    [
        ({"schema": "v0"}, "unknown envelope schema"),
        (env("gists", "github", []), "unknown source/entity"),
        ({**env(), "pages": []}, "no pages"),
        (env(status=500), "HTTP status 500"),
        (env(body={"errorMessages": ["boom"]}), "expected dict with an 'issues' list"),
        (env("pulls", "github", {"message": "Not Found"}), "body is not a list"),
        (env("pr_details", "github", [], kind="reviews"), "pr_number"),
    ],
)
def test_invalid_envelopes_are_reported(envelope, problem) -> None:
    problems = dq.validate_envelope(envelope)
    assert problems and problem in " ".join(problems)


def _register_empty(con) -> None:
    for spec in TABLES.values():
        con.register(spec.name, spec.schema.empty_table())


def test_curated_checks_pass_on_empty_tables() -> None:
    con = duckdb.connect()
    _register_empty(con)
    results = dq.run_checks(con, dq.curated_checks(datetime(2026, 10, 9, tzinfo=UTC)))
    assert results and all(r.passed for r in results)


def test_duplicate_keys_and_orphans_fail_the_gate() -> None:
    con = duckdb.connect()
    _register_empty(con)
    spec = TABLES["gh_reviews"]
    row = {
        "review_id": "1",
        "pr_key": "apache/kafka#9",
        "repo": "apache/kafka",
        "number": 9,
        "state": "APPROVED",
        "_version": "v",
        "_raw_key": "r",
    }
    con.register("gh_reviews", pa.Table.from_pylist([row, row], schema=spec.schema))
    results = dq.run_checks(con, dq.curated_checks(datetime(2026, 10, 9, tzinfo=UTC)))
    failed = {r.name for r in results if not r.passed}
    assert {"gh_reviews__pk_unique", "gh_reviews__pr_exists"} <= failed
    with pytest.raises(dq.DataQualityError, match="gh_reviews__pk_unique"):
        dq.gate(results)
    dq.gate(results, fail_on_error=False)  # reporting-only mode never raises


def test_future_timestamps_are_an_error() -> None:
    con = duckdb.connect()
    _register_empty(con)
    spec = TABLES["jira_issues"]
    row = {
        "issue_key": "KAFKA-1",
        "project_key": "KAFKA",
        "status": "Open",
        "status_category": "new",
        "created_at": datetime(2026, 10, 1),
        "updated_at": datetime(2027, 1, 1),
        "_version": "v",
        "_raw_key": "r",
    }
    con.register("jira_issues", pa.Table.from_pylist([row], schema=spec.schema))
    results = {
        r.name: r for r in dq.run_checks(con, dq.curated_checks(datetime(2026, 10, 9, tzinfo=UTC)))
    }
    assert not results["jira_issues__not_in_future"].passed
    assert results["jira_issues__not_in_future"].severity == "error"
