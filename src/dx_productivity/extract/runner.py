"""Wire settings -> HTTP clients -> extractors. Shared by the CLI and the Lambda handlers."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import asdict
from datetime import datetime
from typing import Any

from dx_productivity.config import Settings
from dx_productivity.extract.emulators import GitHubEmulator, JiraEmulator, fixture_as_of
from dx_productivity.extract.github import GitHubExtractor
from dx_productivity.extract.jira import JiraExtractor
from dx_productivity.extract.raw import RawWriter, new_run_id
from dx_productivity.http import HttpClient, Transport, UrllibTransport
from dx_productivity.logs import get_logger
from dx_productivity.storage import ObjectStore, StateStore, build_object_store, build_state_store

log = get_logger(__name__)

SOURCES = ("jira", "github")


def effective_settings(settings: Settings) -> Settings:
    """Fixture runs are evaluated as of the recording instant unless DX_AS_OF says otherwise."""
    if settings.source == "fixtures" and settings.as_of is None:
        recorded = fixture_as_of(settings.fixtures_dir)
        if recorded is not None:
            return settings.with_overrides(as_of=recorded)
    return settings


def build_transports(settings: Settings) -> dict[str, Transport]:
    if settings.source == "fixtures":
        return {
            "jira": JiraEmulator(settings.fixtures_dir),
            "github": GitHubEmulator(settings.fixtures_dir),
        }
    live = UrllibTransport()
    return {"jira": live, "github": live}


def jira_client(settings: Settings, transport: Transport) -> HttpClient:
    return HttpClient(
        transport=transport,
        base_headers={"Accept": "application/json", "User-Agent": settings.user_agent},
        timeout_s=settings.http_timeout_s,
        retries=settings.http_retries,
        min_interval_s=0 if settings.source == "fixtures" else settings.jira_min_interval_s,
    )


def github_client(settings: Settings, transport: Transport) -> HttpClient:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": settings.user_agent,
    }
    if settings.github_token:
        headers["Authorization"] = f"Bearer {settings.github_token}"
    return HttpClient(
        transport=transport,
        base_headers=headers,
        timeout_s=settings.http_timeout_s,
        retries=settings.http_retries,
        min_interval_s=0 if settings.source == "fixtures" else settings.github_min_interval_s,
        max_requests=settings.github_max_requests,
    )


def run_extract(
    settings: Settings,
    sources: tuple[str, ...] = SOURCES,
    *,
    store: ObjectStore | None = None,
    state: StateStore | None = None,
    transports: dict[str, Transport] | None = None,
    should_stop: Callable[[], bool] = lambda: False,
    run_id: str | None = None,
) -> dict[str, Any]:
    settings = effective_settings(settings)
    store = store or build_object_store(settings)
    state = state or build_state_store(settings)
    transports = transports or build_transports(settings)
    started = time.monotonic()
    now = settings.now()
    clock: Callable[[], datetime] = settings.now
    raw = RawWriter(store, settings.raw_prefix, run_id or new_run_id(now), now)
    summary: dict[str, Any] = {
        "run_id": raw.run_id,
        "as_of": now.isoformat(),
        "source": settings.source,
    }

    if "jira" in sources:
        extractor = JiraExtractor(
            settings, jira_client(settings, transports["jira"]), raw, state, clock, should_stop
        )
        summary["jira"] = [_public(asdict(s)) for s in extractor.run()]
    if "github" in sources:
        if settings.source == "live" and not settings.github_token:
            log.warning(
                "GITHUB_TOKEN not set: GitHub runs unauthenticated (60 requests/hour per IP); "
                "lower DX_GITHUB_MAX_REQUESTS or set a token"
            )
        client = github_client(settings, transports["github"])
        extractor_gh = GitHubExtractor(settings, client, raw, state, clock, should_stop)
        summary["github"] = [_public(asdict(s)) for s in extractor_gh.run()]
        summary["github_requests"] = client.requests_made
    summary["raw_objects"] = len(raw.landed)
    summary["seconds"] = round(time.monotonic() - started, 1)
    return summary


def _public(stats: dict[str, Any]) -> dict[str, Any]:
    keys = stats.pop("raw_keys", [])
    stats["raw_objects"] = len(keys)
    return stats
