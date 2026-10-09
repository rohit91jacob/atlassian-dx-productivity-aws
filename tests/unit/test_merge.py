"""Latest-version-wins merge at the parent grain (curate/pipeline.merge)."""

from __future__ import annotations

from datetime import datetime

import duckdb
import pyarrow as pa
import pytest

from dx_productivity.curate.iceberg import content_hash
from dx_productivity.curate.pipeline import merge
from dx_productivity.curate.schemas import TABLES

ISSUES = TABLES["jira_issues"]
VERSIONS = TABLES["jira_issue_versions"]


def issue(key: str, status: str, version: str, raw: str) -> dict:
    return {
        "issue_key": key,
        "project_key": key.split("-")[0],
        "status": status,
        "status_category": "done" if status == "Resolved" else "new",
        "created_at": datetime(2026, 9, 1),
        "updated_at": datetime(2026, 9, 2),
        "_version": version,
        "_raw_key": raw,
    }


def table(spec, rows) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=spec.schema)


@pytest.fixture
def con():
    return duckdb.connect()


def test_newer_version_replaces_and_older_is_ignored(con) -> None:
    existing = table(
        ISSUES,
        [
            issue("KAFKA-1", "Open", "2026-10-01", "raw/a"),
            issue("KAFKA-2", "Open", "2026-10-05", "raw/a"),
        ],
    )
    incoming = table(
        ISSUES,
        [
            issue("KAFKA-1", "Resolved", "2026-10-02", "raw/b"),  # newer: wins
            issue("KAFKA-2", "Resolved", "2026-10-04", "raw/b"),  # older: ignored
            issue("KAFKA-3", "Open", "2026-10-03", "raw/b"),
        ],
    )  # new key
    out = {r["issue_key"]: r for r in merge(con, ISSUES, existing, incoming).to_pylist()}
    assert out["KAFKA-1"]["status"] == "Resolved"
    assert out["KAFKA-2"]["status"] == "Open"
    assert set(out) == {"KAFKA-1", "KAFKA-2", "KAFKA-3"}


def test_ties_keep_the_existing_row_so_reruns_are_no_ops(con) -> None:
    existing = table(ISSUES, [issue("KAFKA-1", "Open", "2026-10-01", "raw/z-later-key")])
    incoming = table(ISSUES, [issue("KAFKA-1", "Open", "2026-10-01", "raw/a-earlier-key")])
    out = merge(con, ISSUES, existing, incoming)
    assert out.to_pylist()[0]["_raw_key"] == "raw/z-later-key"
    assert content_hash(out) == content_hash(existing)


def test_duplicates_within_a_batch_collapse(con) -> None:
    incoming = table(ISSUES, [issue("KAFKA-1", "Open", "2026-10-01", "raw/a")] * 3)
    assert merge(con, ISSUES, ISSUES.schema.empty_table(), incoming).num_rows == 1


def bridge(key: str, vid: str, version: str, raw: str) -> dict:
    return {
        "issue_key": key,
        "project_key": "KAFKA",
        "relation": "fix",
        "version_id": vid,
        "version_name": vid,
        "_version": version,
        "_raw_key": raw,
    }


def test_child_sets_are_replaced_whole_including_by_an_empty_set(con) -> None:
    existing = table(
        VERSIONS,
        [
            bridge("KAFKA-1", "100", "2026-10-01", "raw/a"),
            bridge("KAFKA-1", "101", "2026-10-01", "raw/a"),
            bridge("KAFKA-2", "100", "2026-10-01", "raw/a"),
        ],
    )
    # KAFKA-1 was edited: fix version 101 removed. KAFKA-2 was edited: all fix versions removed
    # (it only appears as a parent marker, with no child rows).
    incoming = table(VERSIONS, [bridge("KAFKA-1", "100", "2026-10-02", "raw/b")])
    markers = [
        {"issue_key": "KAFKA-1", "_version": "2026-10-02", "_raw_key": "raw/b"},
        {"issue_key": "KAFKA-2", "_version": "2026-10-02", "_raw_key": "raw/b"},
    ]
    out = merge(con, VERSIONS, existing, incoming, markers).to_pylist()
    assert [(r["issue_key"], r["version_id"]) for r in out] == [("KAFKA-1", "100")]


def test_content_hash_ignores_row_order_and_lineage() -> None:
    a = table(
        ISSUES, [issue("KAFKA-1", "Open", "v1", "raw/a"), issue("KAFKA-2", "Open", "v1", "raw/a")]
    )
    b = table(
        ISSUES, [issue("KAFKA-2", "Open", "v1", "raw/x"), issue("KAFKA-1", "Open", "v1", "raw/y")]
    )
    c = table(
        ISSUES,
        [issue("KAFKA-2", "Resolved", "v1", "raw/x"), issue("KAFKA-1", "Open", "v1", "raw/y")],
    )
    assert content_hash(a) == content_hash(b) != content_hash(c)
