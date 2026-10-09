"""Offline emulators of the two APIs, serving *recorded real responses* from ``tests/fixtures/api``.

They implement just enough of each API's query semantics (JQL ``updated >=`` lower bound and
ordering, GitHub ``sort=updated`` paging, ``since`` filters, per-PR reviews/check runs) for the
real extractors to run unchanged against them: same requests, same pagination, same watermarks.
That is what lets CI run the whole pipeline, and the rerun/idempotency tests, with no network.

The fixture *records* are real API objects, trimmed and pseudonymised by
``scripts/build_fixtures.py`` (see DATA_LICENSE.md). Nothing here invents data.
"""

from __future__ import annotations

import json
import re
import urllib.parse
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dx_productivity.extract.watermark import parse_ts
from dx_productivity.http import Request, Response

_JQL = re.compile(r'project = "(?P<project>[A-Z0-9_]+)" AND updated >= "(?P<lower>[0-9/: ]+)"')


def _json_response(body: Any, status: int = 200, headers: dict[str, str] | None = None) -> Response:
    return Response(
        status, {"content-type": "application/json", **(headers or {})}, json.dumps(body).encode()
    )


def _load(path: Path, default: Any) -> Any:
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else default


class JiraEmulator:
    def __init__(self, root: Path):
        self.root = Path(root) / "jira"
        self.requests: list[Request] = []

    def send(self, request: Request, timeout: float) -> Response:
        self.requests.append(request)
        path = urllib.parse.urlparse(request.url).path
        if m := re.search(r"/rest/api/2/project/([A-Z0-9_]+)/versions$", path):
            versions = _load(self.root / f"versions_{m.group(1)}.json", None)
            if versions is None:
                return _json_response({"errorMessages": ["No project"]}, 404)
            return _json_response(versions)
        if path.endswith("/rest/api/2/search"):
            return self._search(request)
        return _json_response({"errorMessages": [f"not emulated: {path}"]}, 404)

    def _search(self, request: Request) -> Response:
        jql = request.param("jql") or ""
        m = _JQL.search(jql)
        if not m:
            return _json_response({"errorMessages": [f"unsupported JQL: {jql}"]}, 400)
        lower = datetime.strptime(m.group("lower"), "%Y/%m/%d %H:%M").replace(tzinfo=UTC)
        issues = _load(self.root / f"issues_{m.group('project')}.json", [])
        matching = sorted(
            (i for i in issues if parse_ts(i["fields"]["updated"]) >= lower),
            key=lambda i: (parse_ts(i["fields"]["updated"]), int(i["key"].split("-")[1])),
        )
        start = int(request.param("startAt", "0") or 0)
        size = int(request.param("maxResults", "50") or 50)
        page = matching[start : start + size]
        return _json_response(
            {"startAt": start, "maxResults": size, "total": len(matching), "issues": page}
        )


class GitHubEmulator:
    def __init__(self, root: Path):
        self.root = Path(root) / "github"
        self.requests: list[Request] = []

    def _repo(self, owner: str, name: str, kind: str, default: Any) -> Any:
        return _load(self.root / f"{owner}__{name}" / f"{kind}.json", default)

    @staticmethod
    def _page(items: list, request: Request) -> list:
        per_page = int(request.param("per_page", "30") or 30)
        page = int(request.param("page", "1") or 1)
        return items[(page - 1) * per_page : page * per_page]

    def send(self, request: Request, timeout: float) -> Response:
        self.requests.append(request)
        path = urllib.parse.urlparse(request.url).path
        headers = {"x-ratelimit-remaining": "4999", "x-ratelimit-resource": "core"}
        m = re.match(r"^/repos/([^/]+)/([^/]+)/(.+)$", path)
        if not m:
            return _json_response({"message": "Not Found"}, 404)
        owner, name, rest = m.groups()
        if rest == "releases":
            return _json_response(
                self._page(self._repo(owner, name, "releases", []), request), headers=headers
            )
        if rest == "pulls":
            pulls = sorted(
                self._repo(owner, name, "pulls", []),
                key=lambda p: (p["updated_at"], p["number"]),
                reverse=True,
            )
            return _json_response(self._page(pulls, request), headers=headers)
        if rest == "commits":
            since = request.param("since")
            commits = self._repo(owner, name, "commits", [])
            if since:
                floor = parse_ts(since)
                commits = [
                    c for c in commits if parse_ts(c["commit"]["committer"]["date"]) >= floor
                ]
            commits = sorted(commits, key=lambda c: c["commit"]["committer"]["date"], reverse=True)
            return _json_response(self._page(commits, request), headers=headers)
        if rm := re.match(r"^pulls/(\d+)/reviews$", rest):
            reviews = self._repo(owner, name, "reviews", {}).get(rm.group(1), [])
            return _json_response(self._page(reviews, request), headers=headers)
        if cm := re.match(r"^commits/([0-9a-f]{40})/check-runs$", rest):
            runs = self._repo(owner, name, "check_runs", {}).get(cm.group(1))
            body = runs if runs is not None else {"total_count": 0, "check_runs": []}
            return _json_response(body, headers=headers)
        return _json_response({"message": "Not Found"}, 404)


def fixture_as_of(root: Path) -> datetime | None:
    """The instant the fixtures were recorded; fixture runs are evaluated "as of" it."""
    manifest = _load(Path(root) / "manifest.json", {})
    return parse_ts(manifest["recorded_at"]) if manifest.get("recorded_at") else None
