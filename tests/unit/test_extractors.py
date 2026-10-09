"""Extractors against the API emulators (real recorded records, no network)."""

from __future__ import annotations

import json
from datetime import timedelta

from dx_productivity.extract.emulators import GitHubEmulator, JiraEmulator
from dx_productivity.extract.raw import decode_envelope
from dx_productivity.extract.runner import run_extract
from dx_productivity.extract.watermark import parse_ts
from dx_productivity.http import Request, Response
from dx_productivity.storage.objects import LocalObjectStore
from dx_productivity.storage.state import SqliteStateStore
from tests.conftest import FIXTURES


def _stores(settings):
    return LocalObjectStore(settings.lake_dir, "dx-raw"), SqliteStateStore(settings.state_db)


def _landed_issue_keys(store, project: str) -> list[str]:
    keys = []
    for k in store.list(f"raw/jira/issues/scope={project}/"):
        for page in decode_envelope(store.get(k))["pages"]:
            keys += [i["key"] for i in page["body"]["issues"]]
    return keys


def test_jira_keyset_pagination_lands_every_issue(fixture_settings) -> None:
    settings = fixture_settings.with_overrides(jira_page_size=7)  # force many pages
    store, state = _stores(settings)
    summary = run_extract(settings, ("jira",), store=store, state=state, run_id="r1")
    for project in ("KAFKA", "SPARK", "FLINK"):
        expected = {
            i["key"]
            for i in json.loads((FIXTURES / "jira" / f"issues_{project}.json").read_text("utf-8"))
        }
        assert set(_landed_issue_keys(store, project)) == expected
    by_project = {s["project"]: s for s in summary["jira"]}
    assert by_project["KAFKA"]["versions"] == 125
    # The watermark is the newest `updated` seen.
    newest = max(
        parse_ts(i["fields"]["updated"])
        for i in json.loads((FIXTURES / "jira" / "issues_SPARK.json").read_text("utf-8"))
    )
    assert parse_ts(state.get("watermark#jira#SPARK#issues").doc["watermark"]) == newest


class MutatingJira(JiraEmulator):
    """Moves the first issue to the end of the ordering after the first page, as a real edit
    during pagination would. Offset paging would skip the issue that slides into its place."""

    def __init__(self, root, project: str):
        super().__init__(root)
        self.project, self.pages = project, 0

    def _search(self, request: Request) -> Response:
        response = super()._search(request)
        self.pages += 1
        if self.pages == 1:
            path = self.root / f"issues_{self.project}.json"
            issues = json.loads(path.read_text("utf-8"))
            first = min(issues, key=lambda i: parse_ts(i["fields"]["updated"]))
            first["fields"]["updated"] = "2026-10-09T16:20:00.000+0000"
            path.write_text(json.dumps(issues), encoding="utf-8")
        return response


def test_no_issue_is_skipped_when_issues_change_mid_pagination(
    fixture_settings, fixtures_copy
) -> None:
    settings = fixture_settings.with_overrides(
        fixtures_dir=fixtures_copy, jira_page_size=5, jira_projects=("KAFKA",)
    )
    store, state = _stores(settings)
    transports = {
        "jira": MutatingJira(fixtures_copy, "KAFKA"),
        "github": GitHubEmulator(fixtures_copy),
    }
    run_extract(settings, ("jira",), store=store, state=state, transports=transports, run_id="r1")
    expected = {
        i["key"]
        for i in json.loads((fixtures_copy / "jira" / "issues_KAFKA.json").read_text("utf-8"))
    }
    assert set(_landed_issue_keys(store, "KAFKA")) == expected


def test_jira_rerun_only_requests_from_the_watermark(fixture_settings) -> None:
    store, state = _stores(fixture_settings)
    run_extract(fixture_settings, ("jira",), store=store, state=state, run_id="r1")
    jira = JiraEmulator(FIXTURES)
    run_extract(
        fixture_settings,
        ("jira",),
        store=store,
        state=state,
        run_id="r2",
        transports={"jira": jira, "github": GitHubEmulator(FIXTURES)},
    )
    searches = [r for r in jira.requests if r.url.endswith("/search")]
    wm = parse_ts(state.get("watermark#jira#KAFKA#issues").doc["watermark"])
    lower = (wm - timedelta(minutes=fixture_settings.overlap_minutes)).strftime("%Y/%m/%d %H:%M")
    assert any(f'updated >= "{lower}"' in r.param("jql") for r in searches)


def test_github_budget_stops_cleanly_and_resumes_without_gaps(fixture_settings) -> None:
    repo_only = fixture_settings.with_overrides(
        github_repos=fixture_settings.github_repos[:1]
    )  # kafka
    store, state = _stores(repo_only)
    runs = 0
    while True:
        runs += 1
        summary = run_extract(
            repo_only.with_overrides(github_max_requests=12),
            ("github",),
            store=store,
            state=state,
            run_id=f"r{runs:02d}",
        )
        stats = summary["github"][0]
        if stats["stopped"] == "complete":
            break
        assert stats["stopped"] == "budget"
        assert runs < 40, "backlog never drained"
    assert runs > 1  # the budget really did split the work across runs
    reviewed = set()
    for k in store.list("raw/github/pr_details/"):
        reviewed |= {
            p["pr_number"] for p in decode_envelope(store.get(k))["pages"] if p["kind"] == "reviews"
        }
    pulls = json.loads((FIXTURES / "github" / "apache__kafka" / "pulls.json").read_text("utf-8"))
    assert reviewed == {p["number"] for p in pulls}


def test_github_sends_token_only_when_configured(fixture_settings) -> None:
    store, state = _stores(fixture_settings)
    gh = GitHubEmulator(FIXTURES)
    run_extract(
        fixture_settings.with_overrides(github_token="t0k3n"),
        ("github",),
        store=store,
        state=state,
        transports={"jira": JiraEmulator(FIXTURES), "github": gh},
        run_id="r1",
    )
    assert all(r.headers["Authorization"] == "Bearer t0k3n" for r in gh.requests)
    assert all(r.headers["X-GitHub-Api-Version"] == "2022-11-28" for r in gh.requests)


def test_raw_envelopes_are_deterministic(fixture_settings, tmp_path) -> None:
    a = fixture_settings.with_overrides(data_dir=tmp_path / "a")
    b = fixture_settings.with_overrides(data_dir=tmp_path / "b")
    for s in (a, b):
        store, state = _stores(s)
        run_extract(s, ("jira",), store=store, state=state, run_id="same-run")
    sa, _ = _stores(a)
    sb, _ = _stores(b)
    assert sa.list("raw/") == sb.list("raw/")
    assert all(sa.get(k) == sb.get(k) for k in sa.list("raw/"))
