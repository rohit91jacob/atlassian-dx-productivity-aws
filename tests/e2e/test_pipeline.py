"""End-to-end: extract -> curate -> dbt build -> mart DQ -> report, on the recorded fixtures.

Covers idempotency (a rerun changes nothing and writes no Iceberg snapshots), equivalence of
incremental and full-refresh curation, incremental pick-up of new activity, and the DQ gates.
"""

from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path

import duckdb
import pytest

from dx_productivity.cli import EXIT_DQ, EXIT_OK, main
from dx_productivity.config import Settings
from dx_productivity.curate.iceberg import load_catalog
from dx_productivity.curate.schemas import TABLES
from tests.conftest import AS_OF, FIXTURES

pytestmark = pytest.mark.e2e

MARTS = [
    "fct_pull_requests",
    "fct_jira_issues",
    "fct_releases",
    "mart_dora_monthly",
    "mart_pr_cycle_time_weekly",
    "mart_issue_flow_weekly",
    "mart_issue_aging",
    "mart_reviewer_load_weekly",
    "mart_reviewer_load_summary",
    "mart_pr_jira_linkage",
    "mart_ci_health_weekly",
]


@pytest.fixture
def env(tmp_path, monkeypatch) -> Settings:
    monkeypatch.setenv("DX_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("DX_SOURCE", "fixtures")
    monkeypatch.setenv("DX_FIXTURES_DIR", str(FIXTURES))
    monkeypatch.setenv("DX_AS_OF", AS_OF.isoformat())
    monkeypatch.setenv("DX_LOG_FORMAT", "text")
    monkeypatch.setenv("DX_LOG_LEVEL", "WARNING")
    return Settings.from_env()


def snapshot_counts(settings: Settings) -> dict[str, int]:
    catalog = load_catalog(settings)
    return {n: len(catalog.load_table(f"curated.{n}").snapshots()) for n in TABLES}


def curated_digest(settings: Settings) -> dict[str, str]:
    catalog = load_catalog(settings)
    out = {}
    for name, spec in TABLES.items():
        data = catalog.load_table(f"curated.{name}").scan().to_arrow()
        cols = [c for c in spec.schema.names if c != "_raw_key"]
        rows = sorted(repr(tuple(r[c] for c in cols)) for r in data.select(cols).to_pylist())
        out[name] = hashlib.sha256("\n".join(rows).encode()).hexdigest()
    return out


def mart_digest(settings: Settings) -> dict[str, str]:
    con = duckdb.connect(str(settings.warehouse_db))
    try:
        out = {}
        for mart in MARTS:
            rows = con.execute(f"SELECT * FROM marts.{mart}").fetchall()
            out[mart] = hashlib.sha256("\n".join(sorted(map(repr, rows))).encode()).hexdigest()
        return out
    finally:
        con.close()


def query(settings: Settings, sql: str) -> list[tuple]:
    con = duckdb.connect(str(settings.warehouse_db))
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_full_pipeline_rerun_and_full_refresh(env: Settings, tmp_path: Path) -> None:
    assert main(["run"]) == EXIT_OK

    # Every mart is built, and the headline facts match the recorded real data.
    for mart in MARTS[:4]:
        assert query(env, f"SELECT count(*) FROM marts.{mart}")[0][0] > 0, mart
    spark = query(
        env,
        "SELECT count(*), sum(CASE WHEN merge_source = 'commit_reference' THEN 1 END)"
        " FROM marts.fct_pull_requests WHERE repo = 'apache/spark' AND is_merged",
    )[0]
    assert spark[0] > 0 and spark[0] == spark[1]  # Spark lands PRs through its merge script
    kafka_releases = query(
        env,
        "SELECT version_name, release_date FROM marts.fct_releases"
        " WHERE project_key = 'KAFKA' AND version_name IN ('4.1.0', '4.2.0')"
        " ORDER BY 1",
    )
    assert [(v, str(d)) for v, d in kafka_releases] == [
        ("4.1.0", "2025-09-02"),
        ("4.2.0", "2026-02-17"),
    ]

    first_curated, first_marts, first_snaps = (
        curated_digest(env),
        mart_digest(env),
        snapshot_counts(env),
    )

    # Rerun with the same source data: watermarks move, overlapping pages are re-landed, and
    # nothing downstream changes. No new Iceberg snapshots are written.
    assert main(["run"]) == EXIT_OK
    assert curated_digest(env) == first_curated
    assert mart_digest(env) == first_marts
    assert snapshot_counts(env) == first_snaps

    # A full refresh from every raw object reproduces the incremental result exactly.
    assert main(["curate", "--full-refresh"]) == EXIT_OK
    assert curated_digest(env) == first_curated
    assert snapshot_counts(env) == first_snaps

    # Operator commands work on the same lake.
    assert main(["dq"]) == EXIT_OK
    assert main(["state"]) == EXIT_OK
    assert main(["sql", "--list"]) == EXIT_OK
    figures = Path(__file__).parents[2] / "verification" / "published_figures.json"
    assert main(["verify", "--offline", "--figures", str(figures)]) == EXIT_OK

    # The results site is generated with real numbers.
    out = tmp_path / "site"
    assert main(["report", "--out", str(out)]) == EXIT_OK
    html = (out / "index.html").read_text(encoding="utf-8")
    assert "DORA keys (proxies)" in html and "apache/spark" in html
    summary = json.loads((out / "summary.json").read_text(encoding="utf-8"))
    assert summary["headline"]["jira_issues"] == sum(
        len(json.loads(p.read_text("utf-8"))) for p in (FIXTURES / "jira").glob("issues_*.json")
    )


def test_new_activity_is_picked_up_incrementally(
    env: Settings, fixtures_copy: Path, monkeypatch
) -> None:
    monkeypatch.setenv("DX_FIXTURES_DIR", str(fixtures_copy))
    assert main(["run"]) == EXIT_OK
    settings = Settings.from_env()
    before = dict(query(settings, "SELECT issue_key, status FROM marts.fct_jira_issues"))

    # Simulate the next day's activity on a copy of the real records: one open issue is resolved.
    path = fixtures_copy / "jira" / "issues_KAFKA.json"
    issues = json.loads(path.read_text("utf-8"))
    target = next(i for i in issues if i["fields"]["status"]["name"] == "Open")
    target["fields"]["status"] = {"name": "Resolved", "statusCategory": {"key": "done"}}
    target["fields"]["resolution"] = {"name": "Fixed"}
    target["fields"]["updated"] = target["fields"]["resolutiondate"] = (
        "2026-10-09T16:24:00.000+0000"
    )
    target["changelog"]["histories"].append(
        {
            "id": "999999999",
            "created": "2026-10-09T16:24:00.000+0000",
            "author": None,
            "items": [{"field": "status", "fromString": "Open", "toString": "Resolved"}],
        }
    )
    path.write_text(json.dumps(issues), encoding="utf-8")

    assert main(["run"]) == EXIT_OK
    after = dict(query(settings, "SELECT issue_key, status FROM marts.fct_jira_issues"))
    assert after[target["key"]] == "Resolved"
    assert {k: v for k, v in after.items() if k != target["key"]} == {
        k: v for k, v in before.items() if k != target["key"]
    }
    transitions = query(
        settings,
        "SELECT count(*) FROM staging.stg_jira__status_transitions"
        f" WHERE issue_key = '{target['key']}' AND history_id = '999999999'",
    )
    assert transitions[0][0] == 1


def test_corrupt_raw_object_blocks_curation(env: Settings) -> None:
    assert main(["extract"]) == EXIT_OK
    lake = env.lake_dir / env.raw_bucket
    victim = sorted(lake.rglob("*.json.gz"))[0]
    envelope = json.loads(gzip.decompress(victim.read_bytes()))
    envelope["pages"][0]["status"] = 500
    victim.write_bytes(gzip.compress(json.dumps(envelope).encode()))

    assert main(["curate"]) == EXIT_DQ
    catalog = load_catalog(env)
    # Nothing was published: tables either do not exist yet or have no snapshot.
    for name in TABLES:
        try:
            table = catalog.load_table(f"curated.{name}")
        except Exception:
            continue
        assert table.current_snapshot() is None
    results = sorted((env.lake_dir / env.raw_bucket / "ops" / "dq_results").rglob("*-raw.json"))
    assert json.loads(results[-1].read_text())["results"][0]["passed"] is False


def test_failing_dbt_test_exits_with_dq_code(env: Settings, monkeypatch) -> None:
    assert main(["run"]) == EXIT_OK
    # Break referential integrity in the warehouse copy of the curated data: the project of one
    # issue no longer exists in the repo_project_map seed, so a dbt relationships test fails.
    con = duckdb.connect(str(env.warehouse_db))
    con.execute(
        "UPDATE curated.jira_issues SET project_key = 'NOPE' WHERE issue_key = "
        "(SELECT min(issue_key) FROM curated.jira_issues)"
    )
    con.close()
    from dx_productivity.transform.dbt import DbtTestError, run_dbt

    with pytest.raises(DbtTestError):
        run_dbt(env, "build", extra_args=["--select", "stg_jira__issues"])
