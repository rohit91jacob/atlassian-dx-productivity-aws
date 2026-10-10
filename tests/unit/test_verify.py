from __future__ import annotations

import re
from datetime import UTC, date, datetime

import duckdb
import pytest

from dx_productivity.http import RateLimited
from dx_productivity.verify.published import (
    Report,
    check_github_merged,
    check_release_dates_jira,
    check_release_dates_published,
    check_release_notes,
    minute_ceil,
    minute_floor,
    parse_release_notes,
    parse_release_page,
    summarise,
)

# Shapes copied from the official pages (October 2026), trimmed to a few entries.
KAFKA = """<h3 id=411>4.1.1</h3><ul><li>Released November 12, 2025</li></ul>
<h3>4.1.0</h3><ul><li>Released September 2, 2025</li></ul><h3>3.2.3</h3><p>Released Sept 19, 2022</p>
<h3>0.11.0.2</h3><p>Released November 17, 2017</p>"""
SPARK = """<h3 class="entry-title"><a href="/news/spark-4-0-1-released.html">Spark 4.0.1 released</a>
</h3><div class="entry-date">September 6, 2025</div>"""
FLINK = """<h3 id="flink-210---2025-07-31">Flink 2.1.0 - 2025-07-31 <a href="#">#</a></h3>"""


def test_kafka_downloads_page() -> None:
    parsed = parse_release_page("kafka_downloads", KAFKA)
    assert parsed["4.1.1"] == date(2025, 11, 12)
    assert parsed["4.1.0"] == date(2025, 9, 2)
    assert parsed["3.2.3"] == date(2022, 9, 19)  # "Sept" abbreviation
    assert "11.0.2" not in parsed  # 0.11.0.2 is not misread as 11.0.2


def test_spark_news_and_flink_downloads_pages() -> None:
    assert parse_release_page("spark_news", SPARK) == {"4.0.1": date(2025, 9, 6)}
    assert parse_release_page("flink_downloads", FLINK) == {"2.1.0": date(2025, 7, 31)}


def test_release_notes_count_distinct_keys() -> None:
    notes = "<li>[<a>KAFKA-1</a>] - x</li><li>[<a>KAFKA-22</a>]</li><li>KAFKA-1 again</li><li>SPARK-3</li>"
    assert parse_release_notes(notes, "KAFKA") == {"KAFKA-1", "KAFKA-22"}


def test_minute_rounding_keeps_the_window_inside_the_data() -> None:
    ts = datetime(2026, 8, 10, 14, 55, 25)
    assert minute_ceil(ts) == datetime(2026, 8, 10, 14, 56)
    assert minute_floor(ts) == datetime(2026, 8, 10, 14, 55)
    exact = datetime(2026, 8, 10, 14, 55)
    assert minute_ceil(exact) == minute_floor(exact) == exact


# --------------------------------------------------------------------------- check logic
class FakeLive:
    """Stands in for the live sources; records every query it is asked."""

    def __init__(self, *, pages=None, versions=None, keys=None, search=None):
        self.pages = pages or {}
        self.versions = versions or {}
        self.keys = keys or (lambda jql: set())
        self.search = search or {}
        self.jql: list[str] = []
        self.queries: list[str] = []

    def page(self, url: str) -> str:
        return self.pages[url]

    def jira_versions(self, project: str) -> dict:
        return self.versions[project]

    def jira_keys(self, jql: str) -> set[str]:
        self.jql.append(jql)
        return self.keys(jql)

    def jira_total(self, jql: str) -> int:
        raise AssertionError("not used")

    def github_search_total(self, query: str) -> int:
        self.queries.append(query)
        value = self.search[re.search(r"merged:(\d{4}-\d{2}-\d{2})", query).group(1)]
        if isinstance(value, Exception):
            raise value
        return value


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    for schema in ("marts", "staging", "intermediate"):
        con.execute(f"CREATE SCHEMA {schema}")
    con.execute(
        "CREATE TABLE marts.fct_releases (project_key VARCHAR, version_name VARCHAR,"
        " release_type VARCHAR, release_date DATE)"
    )
    con.execute(
        "CREATE TABLE staging.stg_jira__issue_versions (issue_key VARCHAR, project_key VARCHAR,"
        " relation VARCHAR, version_id VARCHAR, version_name VARCHAR)"
    )
    con.execute(
        "CREATE TABLE intermediate.int_jira_coverage (project_key VARCHAR,"
        " covered_from TIMESTAMP, covered_from_date DATE, covered_to TIMESTAMP)"
    )
    con.execute(
        "CREATE TABLE intermediate.int_github_coverage (repo VARCHAR, covered_from TIMESTAMP,"
        " covered_to TIMESTAMP)"
    )
    con.execute(
        "CREATE TABLE marts.fct_pull_requests (repo VARCHAR, merge_source VARCHAR,"
        " merged_at TIMESTAMP)"
    )
    return con


RELEASE_CFG = {
    "since": "2025-01-01",
    "sources": [{"project": "KAFKA", "kind": "kafka_downloads", "url": "https://k/downloads"}],
}


def test_release_dates_jira_is_like_for_like_and_finds_a_lost_version(con) -> None:
    con.execute(
        "INSERT INTO marts.fct_releases VALUES"
        " ('KAFKA', '4.1.0', 'minor', DATE '2025-09-02'),"
        " ('KAFKA', '4.1.1', 'patch', DATE '2025-11-14'),"
        " ('KAFKA', 'kafka-5.0.0', 'subproject', DATE '2026-06-02')"
    )
    live = FakeLive(
        versions={
            "KAFKA": {
                "4.1.0": {"released": True, "releaseDate": "2025-09-02"},
                "4.1.1": {"released": True, "releaseDate": "2025-11-14"},
                "4.2.0": {"released": True, "releaseDate": "2026-02-17"},  # pipeline lost it
                "4.3.0": {"released": False},
                "3.9.0": {"released": True, "releaseDate": "2024-11-06"},  # before `since`
            }
        }
    )
    report = Report()
    check_release_dates_jira(con, live, RELEASE_CFG, report)
    assert [(o.subject, o.status) for o in report.outcomes] == [
        ("KAFKA 4.1.0", "match"),
        ("KAFKA 4.1.1", "match"),
        ("KAFKA 4.2.0", "missing_ours"),
    ]


def test_release_dates_published_explains_versions_missing_from_jira(con) -> None:
    con.execute(
        "INSERT INTO marts.fct_releases VALUES"
        " ('KAFKA', '4.1.0', 'minor', DATE '2025-09-02'),"
        " ('KAFKA', '4.1.1', 'patch', DATE '2025-11-14')"
    )
    page = KAFKA + "<h3>4.1.2</h3><ul><li>Released January 5, 2026</li></ul>"
    live = FakeLive(
        pages={"https://k/downloads": page},
        versions={"KAFKA": {"4.1.2": {"released": False}}},
    )
    report = Report()
    check_release_dates_published(con, live, RELEASE_CFG, report)
    got = {o.subject: (o.status, o.note) for o in report.outcomes}
    assert got["KAFKA 4.1.0"][0] == "match"
    assert got["KAFKA 4.1.1"] == ("mismatch", "Jira release date is +2 days vs the announcement")
    assert got["KAFKA 4.1.2"][0] == "missing_ours"
    assert "not marked released" in got["KAFKA 4.1.2"][1]
    assert set(got) == {"KAFKA 4.1.0", "KAFKA 4.1.1", "KAFKA 4.1.2"}  # 3.2.3 is before `since`


NOTES_CFG = {"items": [{"project": "KAFKA", "version": "4.2.0", "url": "https://a/notes.html"}]}
NOTES = "<li>KAFKA-1</li><li>KAFKA-2</li><li>KAFKA-3</li><li>KAFKA-4</li>"


def _notes_con(con) -> None:
    con.execute(
        "INSERT INTO staging.stg_jira__issue_versions VALUES"
        " ('KAFKA-1', 'KAFKA', 'fix', '1', '4.2.0'),"
        " ('KAFKA-2', 'KAFKA', 'fix', '1', '4.2.0'),"
        " ('KAFKA-2', 'KAFKA', 'affects', '1', '4.2.0'),"
        " ('KAFKA-9', 'KAFKA', 'fix', '1', '4.2.0')"  # tagged after release: not in the notes
    )
    con.execute(
        "INSERT INTO intermediate.int_jira_coverage VALUES ('KAFKA',"
        " TIMESTAMP '2026-08-10 14:55:25', DATE '2026-08-10', TIMESTAMP '2026-10-09 17:15:35')"
    )


def test_release_notes_compare_pipeline_output_inside_the_extracted_window(con) -> None:
    _notes_con(con)
    # KAFKA-1 and -2 were last updated inside the window; KAFKA-3 was not (the pipeline never saw
    # it); KAFKA-4 is inside the window but no longer carries the fix version.
    live = FakeLive(
        pages={"https://a/notes.html": NOTES},
        keys=lambda jql: {"KAFKA-1", "KAFKA-2", "KAFKA-9"},
    )
    report = Report()
    check_release_notes(con, live, NOTES_CFG, report)
    (outcome,) = report.outcomes
    assert (outcome.status, outcome.ours, outcome.published) == ("match", 2, 2)
    assert "2 of the 4 issues in the notes" in outcome.note
    assert "also holds 1 issues" in outcome.note
    (jql,) = live.jql
    assert 'fixVersion = "4.2.0"' in jql
    # The window is rounded inwards to Jira's minute precision.
    assert 'updated >= "2026/08/10 14:56"' in jql and 'updated < "2026/10/09 17:15"' in jql


def test_release_notes_report_an_issue_the_pipeline_lost(con) -> None:
    _notes_con(con)
    live = FakeLive(
        pages={"https://a/notes.html": NOTES},
        keys=lambda jql: {"KAFKA-1", "KAFKA-2", "KAFKA-3"},  # KAFKA-3 was updated in the window
    )
    report = Report()
    check_release_notes(con, live, NOTES_CFG, report)
    (outcome,) = report.outcomes
    assert (outcome.status, outcome.ours, outcome.published) == ("mismatch", 2, 3)
    assert outcome.note.endswith("not in the pipeline: KAFKA-3")


def test_release_notes_offline_and_nothing_comparable(con) -> None:
    _notes_con(con)
    report = Report()
    check_release_notes(con, None, NOTES_CFG, report)
    live = FakeLive(pages={"https://a/notes.html": NOTES})
    check_release_notes(con, live, NOTES_CFG, report)
    assert [o.status for o in report.outcomes] == ["skipped", "skipped"]


def test_github_merged_compares_only_days_inside_the_github_window(con) -> None:
    con.execute(
        "INSERT INTO intermediate.int_github_coverage VALUES"
        " ('apache/kafka', TIMESTAMP '2026-10-06 12:20:00', TIMESTAMP '2026-10-09 16:00:00')"
    )
    con.execute(
        "INSERT INTO marts.fct_pull_requests VALUES"
        " ('apache/kafka', 'github_merge', TIMESTAMP '2026-10-08 23:59:59'),"
        " ('apache/kafka', 'github_merge', TIMESTAMP '2026-10-08 00:00:00'),"
        " ('apache/kafka', 'commit_reference', TIMESTAMP '2026-10-08 10:00:00'),"
        " ('apache/kafka', 'github_merge', TIMESTAMP '2026-10-07 09:00:00'),"
        " ('apache/kafka', 'github_merge', TIMESTAMP '2026-10-06 13:00:00')"
    )
    live = FakeLive(search={"2026-10-08": 2, "2026-10-07": 3})
    report = Report()
    cfg = {"days": 4, "repos": ["apache/kafka", "apache/flink"]}
    check_github_merged(con, live, cfg, datetime(2026, 10, 9, 16, 25, tzinfo=UTC), report)
    got = {o.subject: (o.status, o.ours, o.published) for o in report.outcomes}
    assert got["apache/kafka 2026-10-08"] == ("match", 2, 2)
    assert got["apache/kafka 2026-10-07"] == ("mismatch", 1, 3)
    # 6 October starts before the window opened (12:20), so it is not comparable; nor is a
    # repository with no GitHub data at all.
    assert got["apache/kafka 2026-10-06"][0] == "skipped"
    assert got["apache/kafka 2026-10-05"][0] == "skipped"
    assert {got[f"apache/flink 2026-10-0{d}"][0] for d in (5, 6, 7, 8)} == {"skipped"}
    assert len(live.queries) == 2  # GitHub search is only asked about comparable days
    summary = summarise(report)
    assert summary["by_check"]["github_merged"] == {"match": 1, "mismatch": 1, "skipped": 6}
    assert summary["mismatches"] == 1


def test_github_merged_rate_limit_is_a_skip_not_a_crash(con) -> None:
    con.execute(
        "INSERT INTO intermediate.int_github_coverage VALUES"
        " ('apache/kafka', TIMESTAMP '2026-10-01 00:00:00', TIMESTAMP '2026-10-09 16:00:00')"
    )
    live = FakeLive(search={"2026-10-08": RateLimited(None)})
    report = Report()
    cfg = {"days": 1, "repos": ["apache/kafka"]}
    check_github_merged(con, live, cfg, datetime(2026, 10, 9, 16, 25, tzinfo=UTC), report)
    assert [(o.status, o.note) for o in report.outcomes] == [("skipped", "rate limited")]
